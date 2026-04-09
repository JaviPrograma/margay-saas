from flask import Flask, render_template, request, redirect, url_for, abort, send_from_directory, jsonify, flash, session, g
import sqlite3
import os, re, sqlite3, shutil
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta, time, date, timezone
from zoneinfo import ZoneInfo
from urllib.parse import quote
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
from recordatorios import bp as recordatorios_bp

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'tu_clave_secreta_aqui')

# --- Config generales ---
CLINIC_NAME = "MARGAY"
CLINIC_WHATSAPP_RETURN = "agenda_lista"  # adónde volver luego de abrir WhatsApp
app.config['UPLOAD_FOLDER'] = os.environ.get('UPLOAD_FOLDER', '/var/data/uploads' if (os.environ.get('RENDER') or os.environ.get('PORT')) else 'static/uploads')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB
# Base de datos: en Render conviene /tmp; en local usa veterinaria.db
_database_env = os.environ.get('DATABASE_PATH')
if _database_env:
    DATABASE = _database_env
elif os.environ.get('RENDER') or os.environ.get('PORT'):
    DATABASE = '/tmp/veterinaria.db'
else:
    DATABASE = 'veterinaria.db'
# Opcional: clave simple para el programador de tareas
app.config.setdefault('TASK_SECRET', 'margay-task')

# Si estamos en un entorno efímero (Render) y no existe la DB todavía,
# copiamos la base incluida en el proyecto para arrancar con datos y esquema.
if DATABASE.startswith('/tmp/') and not os.path.exists(DATABASE):
    _seed_db = os.path.join(os.path.dirname(__file__), 'veterinaria.db')
    if os.path.exists(_seed_db):
        shutil.copy(_seed_db, DATABASE)

if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

PUBLIC_ENDPOINTS = {'login', 'setup_saas', 'static'}

def current_empresa_id():
    return session.get('empresa_id')

def current_user_id():
    return session.get('user_id')

def current_empresa_id_resolved(conn=None):
    empresa_id = session.get('empresa_id')
    user_id = session.get('user_id')
    if not user_id:
        return empresa_id
    own_conn = False
    if conn is None:
        conn = get_db()
        own_conn = True
    try:
        row = conn.execute('SELECT empresa_id, email, nombre, rol FROM usuarios WHERE id=? AND activo=1', (user_id,)).fetchone()
        if row and row['empresa_id']:
            if empresa_id != row['empresa_id']:
                session['empresa_id'] = row['empresa_id']
            return row['empresa_id']
        return empresa_id
    finally:
        if own_conn:
            conn.close()


def is_margay_master():
    email = (session.get('user_email') or '').strip().lower()
    rol = (session.get('rol') or '').strip().lower()
    return email == 'admin@margay.local' and rol == 'admin'

def require_master_admin(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user_id() or not current_empresa_id():
            return redirect(url_for('login'))
        if current_user_role() != 'admin' or not is_margay_master():
            flash('No tenés permisos para entrar ahí.', 'danger')
            return redirect(url_for('index'))
        return view(*args, **kwargs)
    return wrapped


def require_auth(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user_id() or not current_empresa_id():
            return redirect(url_for('login'))
        return view(*args, **kwargs)
    return wrapped

def _query_params(params, empresa_id=None, prepend=False):
    if params is None:
        params = ()
    if not isinstance(params, (tuple, list)):
        params = (params,)
    else:
        params = tuple(params)
    if empresa_id is None:
        empresa_id = current_empresa_id()
    if empresa_id is None:
        return params
    return ((empresa_id,) + params) if prepend else (params + (empresa_id,))

def _ensure_owned(conn, table, row_id, column='id'):
    empresa_id = current_empresa_id()
    row = conn.execute(f'SELECT 1 FROM {table} WHERE {column}=? AND empresa_id=?', (row_id, empresa_id)).fetchone()
    if not row:
        abort(404)

def _fetchone_empresa(conn, sql, params=(), prepend=False):
    return conn.execute(sql, _query_params(params, prepend=prepend)).fetchone()

def _fetchall_empresa(conn, sql, params=(), prepend=False):
    return conn.execute(sql, _query_params(params, prepend=prepend)).fetchall()

def _execute_empresa(conn, sql, params=(), prepend=False):
    return conn.execute(sql, _query_params(params, prepend=prepend))


def _get_browser_timezone() -> str:
    tz_name = (request.cookies.get('browser_tz') or '').strip()
    if tz_name:
        try:
            ZoneInfo(tz_name)
            return tz_name
        except Exception:
            pass
    return 'America/Montevideo'


def _parse_datetime_value(value):
    if value in (None, ''):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        candidates = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f',
        ]
        for fmt in candidates:
            try:
                return datetime.strptime(raw, fmt)
            except Exception:
                continue
        try:
            return datetime.fromisoformat(raw.replace('Z', '+00:00'))
        except Exception:
            return None
    return None


def format_datetime_local(value, fmt='%Y-%m-%d %H:%M:%S'):
    dt = _parse_datetime_value(value)
    if not dt:
        return '-'
    try:
        tz_name = _get_browser_timezone()
        target_tz = ZoneInfo(tz_name)
    except Exception:
        target_tz = ZoneInfo('America/Montevideo')
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.astimezone(target_tz).strftime(fmt)


app.jinja_env.filters['datetime_local'] = format_datetime_local


@app.context_processor
def inject_timezone_helpers():
    return {
        'browser_timezone': _get_browser_timezone()
    }

@app.before_request
def _saas_guard():
    endpoint = request.endpoint or ''
    g.empresa_id = current_empresa_id()
    g.user_id = current_user_id()
    if endpoint.startswith('recordatorios.'):
        return None
    if endpoint in PUBLIC_ENDPOINTS or endpoint.startswith('static'):
        return None
    # permitir tareas automáticas con secreto
    if request.path.startswith('/tareas/'):
        secret = request.args.get('secret') or request.headers.get('X-Task-Secret')
        if secret == app.config.get('TASK_SECRET'):
            return None
    conn = get_db()
    try:
        row = conn.execute('SELECT COUNT(1) c FROM empresas').fetchone()
        if (row['c'] or 0) == 0 and endpoint != 'setup_saas':
            return redirect(url_for('setup_saas'))
    finally:
        conn.close()
    if endpoint not in PUBLIC_ENDPOINTS and not current_user_id():
        return redirect(url_for('login'))


# --------------------- DB HELPERS ---------------------
def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def _to_float(v):
    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return None

def _primer_y_ultimo_dia(anio:int, mes:int):
    inicio = datetime(anio, mes, 1)
    fin = (datetime(anio+1,1,1) - timedelta(days=1)) if mes==12 else (datetime(anio, mes+1, 1) - timedelta(days=1))
    return inicio.date().isoformat(), fin.date().isoformat()

def init_db():
    conn = get_db()
    cur = conn.cursor()

    # SaaS base
    cur.execute("""
    CREATE TABLE IF NOT EXISTS empresas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        slug TEXT UNIQUE,
        plan TEXT DEFAULT 'starter',
        activa INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id INTEGER NOT NULL,
        nombre TEXT NOT NULL,
        email TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        rol TEXT DEFAULT 'admin',
        activo INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(empresa_id, email),
        FOREIGN KEY(empresa_id) REFERENCES empresas(id)
    )""")

    # Doctores
    cur.execute("""
    CREATE TABLE IF NOT EXISTS doctores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        especialidad TEXT
    )""")

    # Clientes
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        telefono TEXT,
        cedula TEXT,
        tipo TEXT DEFAULT 'Particular', -- 'Mensual' o 'Particular'
        deudor INTEGER DEFAULT 0
    )""")

    # Animales
    cur.execute("""
    CREATE TABLE IF NOT EXISTS animales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER NOT NULL,
        nombre TEXT NOT NULL,
        especie TEXT,
        raza TEXT,
        fecha_nacimiento TEXT,
        FOREIGN KEY(cliente_id) REFERENCES clientes(id)
    )""")

    # Historia clínica + imágenes
    cur.execute("""
    CREATE TABLE IF NOT EXISTS historia_clinica (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        animal_id INTEGER NOT NULL,
        fecha TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        FOREIGN KEY(animal_id) REFERENCES animales(id)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS imagenes_historia (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        historia_id INTEGER NOT NULL,
        filename TEXT NOT NULL,
        FOREIGN KEY(historia_id) REFERENCES historia_clinica(id)
    )""")

    # Vacunas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vacunas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        animal_id INTEGER NOT NULL,
        fecha_vacuna TEXT NOT NULL,
        fecha_vencimiento TEXT NOT NULL,
        FOREIGN KEY(animal_id) REFERENCES animales(id)
    )""")

    # Desparasitaciones
    cur.execute("""
    CREATE TABLE IF NOT EXISTS desparasitaciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        animal_id INTEGER NOT NULL,
        tipo TEXT,
        fecha_aplicacion TEXT NOT NULL,
        fecha_vencimiento TEXT NOT NULL,
        empresa_id INTEGER DEFAULT 1,
        FOREIGN KEY(animal_id) REFERENCES animales(id)
    )""")

    # Motivos
    cur.execute("""
    CREATE TABLE IF NOT EXISTS motivos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        duracion_minutos INTEGER NOT NULL
    )""")

    # Agenda
    cur.execute("""
    CREATE TABLE IF NOT EXISTS agenda (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER NOT NULL,
        animal_id INTEGER NOT NULL,
        doctor_id INTEGER NOT NULL,
        fecha TEXT NOT NULL,
        hora TEXT NOT NULL,
        motivo_id INTEGER,
        estado_pago TEXT DEFAULT 'Debe',
        FOREIGN KEY(cliente_id) REFERENCES clientes(id),
        FOREIGN KEY(animal_id) REFERENCES animales(id),
        FOREIGN KEY(doctor_id) REFERENCES doctores(id),
        FOREIGN KEY(motivo_id) REFERENCES motivos(id)
    )""")

    # Mensualidades
    cur.execute("""
    CREATE TABLE IF NOT EXISTS mensualidades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER NOT NULL,
        anio INTEGER NOT NULL,
        mes INTEGER NOT NULL,
        pagado INTEGER DEFAULT 0,
        fecha_pago TEXT,
        UNIQUE(cliente_id, anio, mes),
        FOREIGN KEY(cliente_id) REFERENCES clientes(id)
    )""")

    # -------- Migraciones suaves --------
    # Clientes extras
    for alter in [
        "ALTER TABLE clientes ADD COLUMN direccion TEXT",
        "ALTER TABLE clientes ADD COLUMN email TEXT",
        "ALTER TABLE clientes ADD COLUMN activo INTEGER DEFAULT 1",
        "ALTER TABLE clientes ADD COLUMN cuota_mensual REAL",
        "ALTER TABLE clientes ADD COLUMN fecha_afiliacion TEXT"
    ]:
        try: cur.execute(alter)
        except Exception: pass

    # Motivos: precios + tipo
    for alter in [
        "ALTER TABLE motivos ADD COLUMN precio_mensual REAL",
        "ALTER TABLE motivos ADD COLUMN precio_particular REAL",
        "ALTER TABLE motivos ADD COLUMN tipo TEXT DEFAULT 'consulta'",
        "ALTER TABLE motivos ADD COLUMN genera_historia INTEGER DEFAULT 1"
    ]:
        try: cur.execute(alter)
        except Exception: pass

    # Agenda: vínculo a mensualidad + precio + lugar + atendida
    for alter in [
        "ALTER TABLE agenda ADD COLUMN cobrada_mensualidad_id INTEGER",
        "ALTER TABLE agenda ADD COLUMN precio REAL",
        "ALTER TABLE agenda ADD COLUMN lugar TEXT DEFAULT 'Clinica'",
        "ALTER TABLE agenda ADD COLUMN atendida INTEGER DEFAULT 0"
    ]:
        try: cur.execute(alter)
        except Exception: pass

    # Mensualidades: montos
    for alter in [
        "ALTER TABLE mensualidades ADD COLUMN monto_cuota REAL",
        "ALTER TABLE mensualidades ADD COLUMN monto_pagado REAL DEFAULT 0"
    ]:
        try: cur.execute(alter)
        except Exception: pass

    # Feriados
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS feriados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT UNIQUE NOT NULL -- 'YYYY-MM-DD'
        )""")
    except Exception:
        pass

    # Matrículas
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS matriculas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id INTEGER NOT NULL,
            fecha TEXT NOT NULL,
            monto REAL DEFAULT 200,
            pagado INTEGER DEFAULT 0,
            fecha_pago TEXT,
            FOREIGN KEY(cliente_id) REFERENCES clientes(id)
        )""")
    except Exception:
        pass

    # Campos extra en ANIMALES (intake más completo)
    for alter in [
        "ALTER TABLE animales ADD COLUMN sexo TEXT",
        "ALTER TABLE animales ADD COLUMN color TEXT",
        "ALTER TABLE animales ADD COLUMN peso_kg REAL",
        "ALTER TABLE animales ADD COLUMN esterilizado INTEGER DEFAULT 0",
        "ALTER TABLE animales ADD COLUMN microchip TEXT",
        "ALTER TABLE animales ADD COLUMN alergias TEXT",
        "ALTER TABLE animales ADD COLUMN enfermedades_cronicas TEXT",
        "ALTER TABLE animales ADD COLUMN temperamento TEXT",
        "ALTER TABLE animales ADD COLUMN alimentacion TEXT",
        "ALTER TABLE animales ADD COLUMN senas_particulares TEXT",
        "ALTER TABLE animales ADD COLUMN ultima_desparasitacion TEXT",
        "ALTER TABLE animales ADD COLUMN ultima_vacunacion TEXT",
    ]:
        try: cur.execute(alter)
        except Exception: pass

    # Campos extra en HISTORIA CLÍNICA (no obligatorios)
    for alter in [
        "ALTER TABLE historia_clinica ADD COLUMN peso_kg REAL",
        "ALTER TABLE historia_clinica ADD COLUMN temp_c REAL",
        "ALTER TABLE historia_clinica ADD COLUMN fc INTEGER",
        "ALTER TABLE historia_clinica ADD COLUMN fr INTEGER",
        "ALTER TABLE historia_clinica ADD COLUMN mucosas TEXT",
        "ALTER TABLE historia_clinica ADD COLUMN hidratacion TEXT",
        "ALTER TABLE historia_clinica ADD COLUMN diagnostico_presuntivo TEXT",
        "ALTER TABLE historia_clinica ADD COLUMN diagnostico_diferencial TEXT",
        "ALTER TABLE historia_clinica ADD COLUMN tratamiento TEXT",
        "ALTER TABLE historia_clinica ADD COLUMN indicaciones TEXT",
        "ALTER TABLE historia_clinica ADD COLUMN particularidades TEXT",
        "ALTER TABLE historia_clinica ADD COLUMN proxima_cita TEXT",
        "ALTER TABLE historia_clinica ADD COLUMN tipo_visita TEXT",
        "ALTER TABLE historia_clinica ADD COLUMN motivo_consulta TEXT",
        "ALTER TABLE historia_clinica ADD COLUMN anamnesis TEXT",
        "ALTER TABLE historia_clinica ADD COLUMN doctor_id INTEGER",
        "ALTER TABLE historia_clinica ADD COLUMN cita_id INTEGER"
    ]:
        try: cur.execute(alter)
        except Exception: pass

    # Índice único para cédula no vacía
    try:
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_clientes_cedula_unique
            ON clientes(cedula)
            WHERE cedula IS NOT NULL AND cedula <> ''
        """)
    except Exception:
        pass

    # Recordatorios de vacunas (único por vacuna_id)
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vacuna_recordatorios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vacuna_id INTEGER UNIQUE NOT NULL,
                animal_id INTEGER NOT NULL,
                cliente_id INTEGER NOT NULL,
                fecha_vencimiento TEXT NOT NULL,
                enviado_en TEXT NOT NULL,
                FOREIGN KEY(vacuna_id) REFERENCES vacunas(id),
                FOREIGN KEY(animal_id) REFERENCES animales(id),
                FOREIGN KEY(cliente_id) REFERENCES clientes(id)
            )
        """)
    except Exception:
        pass

    # Columnas multiempresa
    for alter in [
        "ALTER TABLE doctores ADD COLUMN empresa_id INTEGER DEFAULT 1",
        "ALTER TABLE clientes ADD COLUMN empresa_id INTEGER DEFAULT 1",
        "ALTER TABLE animales ADD COLUMN empresa_id INTEGER DEFAULT 1",
        "ALTER TABLE historia_clinica ADD COLUMN empresa_id INTEGER DEFAULT 1",
        "ALTER TABLE vacunas ADD COLUMN empresa_id INTEGER DEFAULT 1",
        "ALTER TABLE desparasitaciones ADD COLUMN empresa_id INTEGER DEFAULT 1",
        "ALTER TABLE motivos ADD COLUMN empresa_id INTEGER DEFAULT 1",
        "ALTER TABLE agenda ADD COLUMN empresa_id INTEGER DEFAULT 1",
        "ALTER TABLE mensualidades ADD COLUMN empresa_id INTEGER DEFAULT 1",
        "ALTER TABLE feriados ADD COLUMN empresa_id INTEGER DEFAULT 1",
        "ALTER TABLE matriculas ADD COLUMN empresa_id INTEGER DEFAULT 1",
        "ALTER TABLE vacuna_recordatorios ADD COLUMN empresa_id INTEGER DEFAULT 1"
    ]:
        try: cur.execute(alter)
        except Exception: pass

    # empresa por defecto para instalaciones viejas
    try:
        cur.execute("INSERT OR IGNORE INTO empresas (id, nombre, slug, plan, activa) VALUES (1, 'Margay', 'margay', 'starter', 1)")
    except Exception:
        pass

    # backfill de empresa_id
    for table in ['doctores','clientes','animales','historia_clinica','vacunas','desparasitaciones','motivos','agenda','mensualidades','matriculas','vacuna_recordatorios']:
        try:
            cur.execute(f"UPDATE {table} SET empresa_id=1 WHERE empresa_id IS NULL")
        except Exception:
            pass
    try:
        cur.execute("UPDATE feriados SET empresa_id=1 WHERE empresa_id IS NULL")
    except Exception:
        pass

    # usuario admin por defecto si no existe ninguno
    try:
        hay = cur.execute("SELECT 1 FROM usuarios LIMIT 1").fetchone()
        if not hay:
            cur.execute(
                "INSERT INTO usuarios (empresa_id, nombre, email, password_hash, rol, activo) VALUES (?, ?, ?, ?, 'admin', 1)",
                (1, 'Administrador', 'admin@margay.local', generate_password_hash('admin1234'))
            )
    except Exception:
        pass

    try:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS login_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER NOT NULL,
                user_id INTEGER,
                email TEXT,
                ip TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    except Exception:
        pass

    conn.commit()
    conn.close()

init_db()

# --------------------- REGLAS DE NEGOCIO ---------------------
def _es_feriado(conn, fecha_iso: str) -> bool:
    try:
        r = conn.execute("SELECT 1 FROM feriados WHERE fecha=?", (fecha_iso,)).fetchone()
        return r is not None
    except Exception:
        return False

def _en_horario_gratis(dt: datetime) -> bool:
    wd = dt.weekday()  # 0=lunes ... 6=domingo
    t = dt.time()
    if wd <= 5:  # Lunes-Sábado 08-20
        return time(8, 0) <= t <= time(20, 0)
    else:       # Domingo 09-12
        return time(9, 0) <= t <= time(12, 0)

def _calc_cuota_automatica(conn, cliente_id: int) -> float:
    base = 300.0
    n = conn.execute("SELECT COUNT(1) c FROM animales WHERE cliente_id=?", (cliente_id,)).fetchone()['c']
    extras = max(0, n - 4)
    import math
    bloques = math.ceil(extras / 2.0)
    return base + bloques * 50.0

def _cuota_cliente(conn, cliente_row) -> float:
    if cliente_row['cuota_mensual'] is not None:
        return float(cliente_row['cuota_mensual'])
    return _calc_cuota_automatica(conn, cliente_row['id'])

def _actualizar_flag_deudor(conn, cliente_id: int, empresa_id: int | None = None):
    cur = conn.cursor()
    empresa_id = empresa_id or current_empresa_id_resolved(conn)
    if not empresa_id:
        row = cur.execute("SELECT empresa_id FROM clientes WHERE id=?", (cliente_id,)).fetchone()
        empresa_id = row['empresa_id'] if row and row['empresa_id'] else None
    if not empresa_id:
        return

    imp_mens = cur.execute(
        "SELECT COUNT(1) c FROM mensualidades WHERE cliente_id=? AND empresa_id=? AND pagado=0",
        (cliente_id, empresa_id)
    ).fetchone()['c'] or 0

    imp_mat = cur.execute(
        "SELECT COUNT(1) c FROM matriculas WHERE cliente_id=? AND empresa_id=? AND pagado=0",
        (cliente_id, empresa_id)
    ).fetchone()['c'] or 0

    imp_citas = cur.execute("""
        SELECT COUNT(1) c
        FROM agenda a
        LEFT JOIN mensualidades me ON me.id = a.cobrada_mensualidad_id AND me.empresa_id = a.empresa_id
        WHERE a.cliente_id=?
          AND a.empresa_id=?
          AND a.estado_pago='Debe'
          AND (a.cobrada_mensualidad_id IS NULL OR me.pagado=0)
    """, (cliente_id, empresa_id)).fetchone()['c'] or 0

    deudor = 1 if (imp_mens > 0 or imp_mat > 0 or imp_citas > 0) else 0
    cur.execute("UPDATE clientes SET deudor=? WHERE id=? AND empresa_id=?", (deudor, cliente_id, empresa_id))

def _precio_cita_calculado(conn, cliente_id:int, motivo_id:int, fecha:str, hora:str, lugar:str, empresa_id: int | None = None) -> float:
    empresa_id = empresa_id or current_empresa_id_resolved(conn)
    cl = conn.execute("SELECT tipo FROM clientes WHERE id=? AND empresa_id=?", (cliente_id, empresa_id)).fetchone()
    mo = conn.execute("SELECT precio_mensual, precio_particular, tipo FROM motivos WHERE id=? AND empresa_id=?", (motivo_id, empresa_id)).fetchone()
    if not cl or not mo:
        return None

    # Socio - consulta en clínica dentro de horario y no feriado => $0
    if cl['tipo'] == 'Mensual' and (mo['tipo'] or 'consulta') == 'consulta' and (lugar or 'Clinica') == 'Clinica':
        try:
            dt = datetime.strptime(f"{fecha} {hora}", "%Y-%m-%d %H:%M")
        except Exception:
            dt = None
        if dt is not None and _en_horario_gratis(dt) and not _es_feriado(conn, fecha):
            return 0.0

    if cl['tipo'] == 'Mensual':
        return mo['precio_mensual']
    else:
        return mo['precio_particular']

# ---------- WhatsApp helpers ----------
def _uy_to_e164_digits(phone_raw: str) -> str | None:
    """Devuelve dígitos para wa.me (sin '+'). Heurística para Uruguay."""
    if not phone_raw:
        return None
    digits = re.sub(r'\D', '', phone_raw)
    if not digits:
        return None
    if digits.startswith('598'):
        return digits
    if digits.startswith('0'):
        return '598' + digits.lstrip('0')
    if len(digits) >= 8:
        return '598' + digits
    return None

def build_whatsapp_text(cita_row, cliente_row, animal_row, doctor_row, motivo_row):
    fecha = cita_row['fecha']
    hora = cita_row['hora']
    try:
        dt = datetime.strptime(f"{fecha} {hora}", "%Y-%m-%d %H:%M")
        fecha_fmt = dt.strftime("%d/%m/%Y")
        hora_fmt  = dt.strftime("%H:%M")
    except Exception:
        fecha_fmt, hora_fmt = fecha, hora

    lugar = cita_row['lugar'] or "Clinica"
    if lugar == "Clinica":
        lugar_desc = f"Clínica {CLINIC_NAME}"
    else:
        direccion = (cliente_row['direccion'] or '').strip()
        lugar_desc = f"Domicilio{': ' + direccion if direccion else ''}"

    lineas = [
        f"Hola {cliente_row['nombre']}, te escribe {CLINIC_NAME} 🐾",
        "",
        "Tu cita quedó agendada:",
        f"• Fecha: {fecha_fmt}",
        f"• Hora: {hora_fmt}",
        f"• Paciente: {animal_row['nombre']}",
        f"• Motivo: {motivo_row['nombre'] if motivo_row else '—'}",
        f"• Doctor/a: {doctor_row['nombre']}",
        f"• Lugar: {lugar_desc}",
        "",
        "Si no podés asistir, por favor avisanos respondiendo a este WhatsApp. ¡Gracias! 🐶🐱"
    ]
    return "\n".join(lineas)

def build_vacunas_text(cliente_nombre: str, items: list[tuple[str, str]]) -> str:
    """
    items: lista de (animal_nombre, fecha_vencimiento 'YYYY-MM-DD')
    """
    filas = []
    for nom, fv in items:
        try:
            dt = datetime.strptime(fv, "%Y-%m-%d")
            fv_fmt = dt.strftime("%d/%m/%Y")
        except Exception:
            fv_fmt = fv
        filas.append(f"• {nom}: vence {fv_fmt}")

    msg = [
        f"Hola {cliente_nombre}, te escribe {CLINIC_NAME} 🐾",
        "",
        "Vimos que se aproxima el vencimiento de vacunas:",
        *filas,
        "",
        "Si querés, te agendamos la vacunación. Respondé este mensaje y coordinamos. ¡Gracias!"
    ]
    return "\n".join(msg)

def _motivo_ids_vacunacion(conn):
    """
    IDs de motivos cuyo nombre es EXACTAMENTE: 'vacuna', 'vacunación' o 'vacunacion'
    (insensible a mayúsculas/minúsculas y espacios alrededor).
    """
    rows = conn.execute("""
        SELECT id
        FROM motivos
        WHERE LOWER(TRIM(nombre)) IN ('vacuna','vacunación','vacunacion')
    """).fetchall()
    return [r['id'] for r in rows]

# --------------------- RUTAS ---------------------
@app.route('/setup', methods=['GET', 'POST'])
def setup_saas():
    conn = get_db()
    try:
        count = conn.execute('SELECT COUNT(1) c FROM empresas').fetchone()['c']
        if count > 0:
            return redirect(url_for('login'))
        if request.method == 'POST':
            empresa = (request.form.get('empresa') or '').strip()
            nombre = (request.form.get('nombre') or '').strip()
            email = (request.form.get('email') or '').strip().lower()
            password = request.form.get('password') or ''
            if not empresa or not nombre or not email or len(password) < 6:
                flash('Completá todos los campos. La contraseña debe tener al menos 6 caracteres.', 'danger')
                return render_template('setup_saas.html')
            slug = secure_filename(empresa.lower()).replace('_', '-') or 'clinica'
            cur = conn.cursor()
            cur.execute('INSERT INTO empresas (nombre, slug, plan, activa) VALUES (?, ?, ?, 1)', (empresa, slug, 'starter'))
            empresa_id = cur.lastrowid
            cur.execute('INSERT INTO usuarios (empresa_id, nombre, email, password_hash, rol, activo) VALUES (?, ?, ?, ?, ?, 1)',
                        (empresa_id, nombre, email, generate_password_hash(password), 'admin'))
            conn.commit()
            flash('Instalación SaaS creada. Ya podés iniciar sesión.', 'success')
            return redirect(url_for('login'))
        return render_template('setup_saas.html')
    finally:
        conn.close()

@app.route('/login', methods=['GET', 'POST'])
def login():
    conn = get_db()
    try:
        empresas = conn.execute('SELECT id, nombre FROM empresas WHERE activa=1 ORDER BY nombre').fetchall()
        if request.method == 'POST':
            empresa_id = request.form.get('empresa_id', type=int)
            email = (request.form.get('email') or '').strip().lower()
            password = request.form.get('password') or ''
            user = conn.execute(
                'SELECT u.*, e.nombre as empresa_nombre FROM usuarios u JOIN empresas e ON e.id=u.empresa_id WHERE u.empresa_id=? AND LOWER(u.email)=? AND u.activo=1 AND e.activa=1',
                (empresa_id, email)
            ).fetchone()
            if user and check_password_hash(user['password_hash'], password):
                session.clear()
                session['user_id'] = user['id']
                session['empresa_id'] = user['empresa_id']
                session['empresa_nombre'] = user['empresa_nombre']
                session['user_nombre'] = user['nombre']
                session['user_email'] = user['email']
                session['rol'] = user['rol']
                try:
                    conn.execute(
                        'INSERT INTO login_audit (empresa_id, user_id, email, ip, created_at) VALUES (?, ?, ?, ?, ?)',
                        (user['empresa_id'], user['id'], user['email'], request.headers.get('X-Forwarded-For', request.remote_addr), datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
                    )
                    conn.commit()
                except Exception:
                    pass
                return redirect(url_for('home'))
            flash('Datos de acceso inválidos.', 'danger')
        return render_template('login.html', empresas=empresas)
    finally:
        conn.close()

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


def current_user_role():
    return session.get('rol')

def require_admin(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user_id() or not current_empresa_id():
            return redirect(url_for('login'))
        if current_user_role() != 'admin':
            flash('No tenés permisos para entrar ahí.', 'danger')
            return redirect(url_for('home'))
        return view(*args, **kwargs)
    return wrapped

@app.route('/veterinarias')
@require_master_admin
def veterinarias_panel():
    conn = get_db()
    try:
        empresas = conn.execute("""
            SELECT e.*,
                   (SELECT COUNT(1) FROM usuarios u WHERE u.empresa_id=e.id AND u.activo=1) AS usuarios_activos
            FROM empresas e
            ORDER BY e.id
        """).fetchall()
        return render_template('veterinarias.html', empresas=empresas)
    finally:
        conn.close()

@app.route('/veterinarias/nueva', methods=['POST'])
@require_master_admin
def veterinaria_nueva():
    nombre = (request.form.get('nombre') or '').strip()
    slug_raw = (request.form.get('slug') or '').strip().lower()
    plan = (request.form.get('plan') or 'starter').strip().lower()
    admin_nombre = (request.form.get('admin_nombre') or '').strip()
    admin_email = (request.form.get('admin_email') or '').strip().lower()
    password = request.form.get('password') or ''

    if not nombre or not admin_nombre or not admin_email or len(password) < 6:
        flash('Completá nombre de veterinaria, administrador, email y una clave de al menos 6 caracteres.', 'danger')
        return redirect(url_for('veterinarias_panel'))

    slug = secure_filename(slug_raw or nombre.lower()).replace('_', '-') or 'clinica'
    if plan not in ('starter', 'pro', 'premium'):
        plan = 'starter'

    conn = get_db()
    try:
        existe_slug = conn.execute('SELECT id FROM empresas WHERE slug=?', (slug,)).fetchone()
        if existe_slug:
            flash('Ya existe una veterinaria con ese identificador. Probá con otro slug.', 'danger')
            return redirect(url_for('veterinarias_panel'))

        cur = conn.cursor()
        cur.execute('INSERT INTO empresas (nombre, slug, plan, activa) VALUES (?, ?, ?, 1)', (nombre, slug, plan))
        empresa_id = cur.lastrowid
        cur.execute(
            'INSERT INTO usuarios (empresa_id, nombre, email, password_hash, rol, activo) VALUES (?, ?, ?, ?, ?, 1)',
            (empresa_id, admin_nombre, admin_email, generate_password_hash(password), 'admin')
        )

        motivos_base = ['Consulta', 'Vacunación', 'Control', 'Cirugía', 'Desparasitación', 'Urgencia']
        for m in motivos_base:
            try:
                cur.execute('INSERT INTO motivos (nombre, empresa_id) VALUES (?, ?)', (m, empresa_id))
            except Exception:
                pass

        conn.commit()
        flash(f'Veterinaria creada correctamente. Acceso inicial: {admin_email}', 'success')
        return redirect(url_for('veterinarias_panel'))
    finally:
        conn.close()

@app.route('/veterinarias/toggle/<int:empresa_id>', methods=['POST'])
@require_master_admin
def veterinaria_toggle(empresa_id):
    if empresa_id == current_empresa_id():
        flash('No podés desactivar la veterinaria en la que estás logueado.', 'danger')
        return redirect(url_for('veterinarias_panel'))
    conn = get_db()
    try:
        empresa = conn.execute('SELECT * FROM empresas WHERE id=?', (empresa_id,)).fetchone()
        if not empresa:
            abort(404)
        nuevo = 0 if int(empresa['activa'] or 0) == 1 else 1
        conn.execute('UPDATE empresas SET activa=? WHERE id=?', (nuevo, empresa_id))
        conn.commit()
        flash('Estado de la veterinaria actualizado.', 'success')
        return redirect(url_for('veterinarias_panel'))
    finally:
        conn.close()


@app.route('/administrador')
@require_master_admin
def administrador_panel():
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT
                e.id,
                e.nombre,
                e.slug,
                COALESCE(e.plan, 'starter') AS plan,
                COALESCE(e.activa, 1) AS activa,
                (SELECT COUNT(1) FROM usuarios u WHERE u.empresa_id=e.id) AS usuarios_total,
                (SELECT COUNT(1) FROM usuarios u WHERE u.empresa_id=e.id AND COALESCE(u.activo,1)=1) AS usuarios_activos,
                (SELECT COUNT(1) FROM clientes c WHERE c.empresa_id=e.id) AS clientes_total,
                (SELECT COUNT(1) FROM agenda a WHERE a.empresa_id=e.id AND date(a.fecha) >= date('now','-30 day')) AS turnos_30,
                (SELECT COUNT(1) FROM login_audit la WHERE la.empresa_id=e.id AND datetime(la.created_at) >= datetime('now','-30 day')) AS logins_30,
                (SELECT MAX(la.created_at) FROM login_audit la WHERE la.empresa_id=e.id) AS ultimo_login
            FROM empresas e
            ORDER BY e.id
        """).fetchall()
        resumen = [dict(r) for r in rows]
        kpis = {
            'veterinarias_total': len(resumen),
            'veterinarias_activas': sum(1 for r in resumen if int(r.get('activa') or 0) == 1),
            'veterinarias_con_uso': sum(1 for r in resumen if (r.get('logins_30') or 0) > 0 or (r.get('turnos_30') or 0) > 0),
            'logins_30_total': sum((r.get('logins_30') or 0) for r in resumen),
        }
        ultimos = conn.execute("""
            SELECT la.*, e.nombre AS empresa_nombre
            FROM login_audit la
            JOIN empresas e ON e.id = la.empresa_id
            ORDER BY datetime(la.created_at) DESC
            LIMIT 20
        """).fetchall()
        return render_template('administrador.html', resumen=resumen, kpis=kpis, ultimos=ultimos)
    finally:
        conn.close()


@app.route('/administrador/veterinaria/<int:empresa_id>')
@app.route('/administrador/veterinaria/<int:empresa_id>/')
@require_master_admin
def administrador_veterinaria(empresa_id):
    conn = get_db()
    try:
        empresa = conn.execute('SELECT * FROM empresas WHERE id=?', (empresa_id,)).fetchone()
        if not empresa:
            abort(404)
        usuarios = conn.execute('SELECT * FROM usuarios WHERE empresa_id=? ORDER BY id', (empresa_id,)).fetchall()
        metricas = conn.execute("""
            SELECT
                (SELECT COUNT(1) FROM clientes WHERE empresa_id=?) AS clientes_total,
                (SELECT COUNT(1) FROM agenda WHERE empresa_id=? AND date(fecha) >= date('now','-30 day')) AS turnos_30,
                (SELECT COUNT(1) FROM login_audit WHERE empresa_id=? AND datetime(created_at) >= datetime('now','-30 day')) AS logins_30
        """, (empresa_id, empresa_id, empresa_id)).fetchone()
        ultimos = conn.execute('SELECT * FROM login_audit WHERE empresa_id=? ORDER BY datetime(created_at) DESC LIMIT 20', (empresa_id,)).fetchall()
        return render_template('administrador_veterinaria.html', empresa=empresa, usuarios=usuarios, metricas=metricas, ultimos=ultimos)
    finally:
        conn.close()


@app.route('/administrador/veterinaria/<int:empresa_id>/editar', methods=['POST'])
@require_master_admin
def administrador_veterinaria_editar(empresa_id):
    nombre = (request.form.get('nombre') or '').strip()
    slug = secure_filename((request.form.get('slug') or '').strip().lower()).replace('_', '-')
    plan = (request.form.get('plan') or 'starter').strip().lower()
    activa = 1 if str(request.form.get('activa', '1')) == '1' else 0
    if empresa_id == current_empresa_id() and activa == 0:
        flash('No podés desactivar la veterinaria en la que estás logueado.', 'danger')
        return redirect(url_for('administrador_veterinaria', empresa_id=empresa_id))
    conn = get_db()
    try:
        empresa = conn.execute('SELECT * FROM empresas WHERE id=?', (empresa_id,)).fetchone()
        if not empresa:
            abort(404)
        if not nombre:
            flash('El nombre es obligatorio.', 'danger')
            return redirect(url_for('administrador_veterinaria', empresa_id=empresa_id))
        if slug:
            existe = conn.execute('SELECT id FROM empresas WHERE slug=? AND id<>?', (slug, empresa_id)).fetchone()
            if existe:
                flash('Ese slug ya está siendo usado por otra veterinaria.', 'danger')
                return redirect(url_for('administrador_veterinaria', empresa_id=empresa_id))
        conn.execute('UPDATE empresas SET nombre=?, slug=?, plan=?, activa=? WHERE id=?', (nombre, slug or None, plan, activa, empresa_id))
        conn.commit()
        flash('Veterinaria actualizada correctamente.', 'success')
        return redirect(url_for('administrador_veterinaria', empresa_id=empresa_id))
    finally:
        conn.close()


@app.route('/administrador/veterinaria/<int:empresa_id>/usuarios/nuevo', methods=['POST'])
@app.route('/administrador/veterinaria/<int:empresa_id>/usuarios/crear', methods=['POST'])
@require_master_admin
def administrador_usuario_nuevo(empresa_id):
    nombre = (request.form.get('nombre') or '').strip()
    email = (request.form.get('email') or '').strip().lower()
    rol = (request.form.get('rol') or 'usuario').strip().lower()
    password = request.form.get('password') or ''
    if rol == 'operador':
        rol = 'usuario'
    if rol not in ('admin', 'usuario'):
        rol = 'usuario'
    if not nombre or not email or len(password) < 6:
        flash('Completá nombre, email y contraseña válida.', 'danger')
        return redirect(url_for('administrador_veterinaria', empresa_id=empresa_id))
    conn = get_db()
    try:
        existe = conn.execute('SELECT id FROM usuarios WHERE empresa_id=? AND lower(email)=?', (empresa_id, email)).fetchone()
        if existe:
            flash('Ya existe un usuario con ese email en esa veterinaria.', 'danger')
            return redirect(url_for('administrador_veterinaria', empresa_id=empresa_id))
        conn.execute(
            'INSERT INTO usuarios (empresa_id, nombre, email, password_hash, rol, activo) VALUES (?, ?, ?, ?, ?, 1)',
            (empresa_id, nombre, email, generate_password_hash(password), rol)
        )
        conn.commit()
        flash('Usuario creado correctamente.', 'success')
        return redirect(url_for('administrador_veterinaria', empresa_id=empresa_id))
    finally:
        conn.close()


@app.route('/administrador/usuarios/<int:user_id>/toggle', methods=['POST'])
@require_master_admin
def administrador_usuario_toggle(user_id):
    conn = get_db()
    try:
        user = conn.execute('SELECT * FROM usuarios WHERE id=?', (user_id,)).fetchone()
        if not user:
            abort(404)
        if (user['email'] or '').strip().lower() == 'admin@margay.local':
            flash('No podés desactivar la cuenta superadministradora.', 'danger')
            return redirect(url_for('administrador_veterinaria', empresa_id=user['empresa_id']))
        nuevo = 0 if int(user['activo'] or 0) == 1 else 1
        conn.execute('UPDATE usuarios SET activo=? WHERE id=?', (nuevo, user_id))
        conn.commit()
        flash('Estado del usuario actualizado.', 'success')
        return redirect(url_for('administrador_veterinaria', empresa_id=user['empresa_id']))
    finally:
        conn.close()


@app.route('/administrador/usuarios/<int:user_id>/password', methods=['POST'])
@require_master_admin
def administrador_usuario_password(user_id):
    password = request.form.get('password') or ''
    conn = get_db()
    try:
        user = conn.execute('SELECT * FROM usuarios WHERE id=?', (user_id,)).fetchone()
        if not user:
            abort(404)
        if len(password) < 6:
            flash('La nueva contraseña debe tener al menos 6 caracteres.', 'danger')
            return redirect(url_for('administrador_veterinaria', empresa_id=user['empresa_id']))
        conn.execute('UPDATE usuarios SET password_hash=? WHERE id=?', (generate_password_hash(password), user_id))
        conn.commit()
        flash('Contraseña actualizada correctamente.', 'success')
        return redirect(url_for('administrador_veterinaria', empresa_id=user['empresa_id']))
    finally:
        conn.close()


@app.route('/mi-cuenta', methods=['GET', 'POST'])
@require_auth
def mi_cuenta():
    conn = get_db()
    try:
        user = conn.execute(
            'SELECT id, nombre, email, password_hash FROM usuarios WHERE id=? AND empresa_id=?',
            (current_user_id(), current_empresa_id())
        ).fetchone()
        if not user:
            session.clear()
            flash('Sesión inválida. Iniciá sesión nuevamente.', 'danger')
            return redirect(url_for('login'))

        if request.method == 'POST':
            actual = request.form.get('actual_password') or ''
            nueva = request.form.get('new_password') or ''
            repetir = request.form.get('repeat_password') or ''

            if not check_password_hash(user['password_hash'], actual):
                flash('La contraseña actual no es correcta.', 'danger')
            elif len(nueva) < 6:
                flash('La nueva contraseña debe tener al menos 6 caracteres.', 'danger')
            elif nueva != repetir:
                flash('La repetición de la contraseña no coincide.', 'danger')
            else:
                conn.execute(
                    'UPDATE usuarios SET password_hash=? WHERE id=? AND empresa_id=?',
                    (generate_password_hash(nueva), current_user_id(), current_empresa_id())
                )
                conn.commit()
                flash('Contraseña actualizada correctamente.', 'success')
                return redirect(url_for('mi_cuenta'))

        return render_template('mi_cuenta.html', user=user)
    finally:
        conn.close()

@app.context_processor
def inject_saas_context():
    return {
        'empresa_nombre': session.get('empresa_nombre', CLINIC_NAME),
        'user_nombre': session.get('user_nombre'),
        'user_email': session.get('user_email'),
        'user_rol': session.get('rol'),
        'is_margay_master': is_margay_master(),
        'is_master_admin': is_margay_master(),
    }

@app.route("/")
def inicio():
    if not current_user_id() or not current_empresa_id():
        return redirect(url_for("login"))
    return redirect(url_for("home"))

@app.route("/home")
@require_auth
def home():
    return render_template("index.html")

# -------- Doctores --------
@app.route("/doctores")
@require_admin
def doctores():
    conn = get_db()
    doctores = _fetchall_empresa(conn, "SELECT * FROM doctores WHERE empresa_id=?")
    conn.close()
    return render_template("doctores.html", doctores=doctores, doctor_edit=None)

@app.route("/doctores/nuevo", methods=["POST"])
@require_admin
def doctor_nuevo():
    nombre = request.form.get("nombre", "").strip()
    especialidad = request.form.get("especialidad", "").strip()
    conn = get_db()
    _execute_empresa(conn, "INSERT INTO doctores (nombre, especialidad, empresa_id) VALUES (?, ?, ?)", (nombre, especialidad), prepend=False)
    conn.commit()
    conn.close()
    return redirect(url_for("doctores"))

@app.route("/doctores/editar/<int:id>", methods=["GET", "POST"])
@require_admin
def doctor_editar(id):
    conn = get_db()
    if request.method == "POST":
        nombre = request.form.get("nombre", "").strip()
        especialidad = request.form.get("especialidad", "").strip()
        _ensure_owned(conn, "doctores", id)
        conn.execute("UPDATE doctores SET nombre=?, especialidad=? WHERE id=? AND empresa_id=?", (nombre, especialidad, id, current_empresa_id()))
        conn.commit()
        conn.close()
        return redirect(url_for("doctores"))
    doctor = conn.execute("SELECT * FROM doctores WHERE id=? AND empresa_id=?", (id, current_empresa_id())).fetchone()
    conn.close()
    if doctor is None:
        abort(404)
    return render_template("doctores.html", doctores=[], doctor_edit=doctor)

@app.route("/doctores/eliminar/<int:id>")
@require_admin
def doctor_eliminar(id):
    conn = get_db()
    conn.execute("DELETE FROM doctores WHERE id=? AND empresa_id=?", (id, current_empresa_id()))
    conn.commit()
    conn.close()
    return redirect(url_for("doctores"))

# -------- Clientes (filtros + dirección + cuota + matrícula) --------
@app.route("/clientes")
def clientes():
    q_nombre = (request.args.get("nombre", "") or "").strip()
    q_cedula = (request.args.get("cedula", "") or "").strip()
    q_tipo   = (request.args.get("tipo", "") or "").strip()

    conn = get_db()
    empresa_id = current_empresa_id_resolved(conn)
    query = "SELECT * FROM clientes WHERE empresa_id=?"
    params = [empresa_id]

    if q_nombre:
        like = f"%{q_nombre}%"
        query += " AND (nombre LIKE ? COLLATE NOCASE OR COALESCE(telefono,'') LIKE ? OR COALESCE(email,'') LIKE ? COLLATE NOCASE)"
        params.extend([like, like, like])

    if q_cedula:
        ced_digits = re.sub(r"\D", "", q_cedula)
        if ced_digits:
            query += " AND REPLACE(REPLACE(REPLACE(COALESCE(cedula,''), '.', ''), '-', ''), ' ', '') LIKE ?"
            params.append(f"%{ced_digits}%")
        else:
            query += " AND COALESCE(cedula,'') LIKE ?"
            params.append(f"%{q_cedula}%")

    if q_tipo in ("Mensual", "Particular"):
        query += " AND tipo = ?"
        params.append(q_tipo)

    query += " ORDER BY nombre COLLATE NOCASE"

    filas = conn.execute(query, params).fetchall()

    pend = conn.execute("""
        SELECT cliente_id, COUNT(1) c
        FROM matriculas
        WHERE pagado=0
        GROUP BY cliente_id
    """).fetchall()
    map_pend = {r['cliente_id']: r['c'] for r in pend}

    cuotas_sugeridas = {}
    for c in filas:
        if c['tipo'] == 'Mensual' and c['cuota_mensual'] is None:
            cuotas_sugeridas[c['id']] = _calc_cuota_automatica(conn, c['id'])

    conn.close()
    filtros = {"nombre": q_nombre, "cedula": q_cedula, "tipo": q_tipo}
    return render_template("clientes.html",
                           clientes=filas, cliente_edit=None, filtros=filtros,
                           matricula_pend=map_pend, cuotas_sugeridas=cuotas_sugeridas)

@app.route("/clientes/nuevo", methods=["POST"])
def cliente_nuevo():
    nombre = request.form.get("nombre", "").strip()
    telefono = request.form.get("telefono", "").strip()
    cedula = (request.form.get("cedula", "") or "").strip()
    tipo = request.form.get("tipo", "Particular").strip()
    direccion = request.form.get("direccion", "").strip()
    email = (request.form.get("email", "") or "").strip().lower()
    cuota_mensual = _to_float(request.form.get("cuota_mensual", ""))

    conn = get_db()

    if cedula:
        dup = conn.execute("SELECT id FROM clientes WHERE cedula = ? AND empresa_id=?", (cedula, current_empresa_id())).fetchone()
        if dup:
            conn.close()
            flash("Ya existe un cliente con esa cédula.", "danger")
            return redirect(url_for("clientes"))

    try:
        fecha_af = datetime.now().strftime("%Y-%m-%d") if tipo == "Mensual" else None
        conn.execute(
            """INSERT INTO clientes (nombre, telefono, cedula, tipo, deudor, direccion, email, activo, cuota_mensual, fecha_afiliacion, empresa_id)
               VALUES (?, ?, ?, ?, 0, ?, ?, 1, ?, ?, ?)""",
            (nombre, telefono, cedula, tipo, direccion, email, cuota_mensual, fecha_af, current_empresa_id()),
        )
        cliente_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()['id']

        if tipo == "Mensual":
            conn.execute(
                "INSERT INTO matriculas (cliente_id, fecha, monto, pagado, empresa_id) VALUES (?, ?, ?, 0, ?)",
                (cliente_id, datetime.now().strftime("%Y-%m-%d"), 200.0, current_empresa_id())
            )
            _actualizar_flag_deudor(conn, cliente_id, current_empresa_id())

        conn.commit()
    except sqlite3.IntegrityError:
        conn.rollback()
        conn.close()
        flash("Ya existe un cliente con esa cédula.", "danger")
        return redirect(url_for("clientes"))

    conn.close()
    return redirect(url_for("clientes"))

@app.route("/clientes/editar/<int:id>", methods=["GET", "POST"])
def cliente_editar(id):
    conn = get_db()
    if request.method == "POST":
        nombre = request.form.get("nombre", "").strip()
        telefono = request.form.get("telefono", "").strip()
        cedula = (request.form.get("cedula", "") or "").strip()
        tipo = request.form.get("tipo", "Particular").strip()
        direccion = request.form.get("direccion", "").strip()
        email = (request.form.get("email", "") or "").strip().lower()
        cuota_mensual = _to_float(request.form.get("cuota_mensual", ""))

        prev = conn.execute("SELECT tipo FROM clientes WHERE id=? AND empresa_id=?", (id, current_empresa_id())).fetchone()
        prev_tipo = prev['tipo'] if prev else None

        if cedula:
            dup = conn.execute("SELECT id FROM clientes WHERE cedula = ? AND id <> ? AND empresa_id=?", (cedula, id, current_empresa_id())).fetchone()
            if dup:
                conn.close()
                flash("Ya existe otro cliente con esa cédula.", "danger")
                return redirect(url_for("clientes"))

        try:
            fecha_af = None
            if prev_tipo != "Mensual" and tipo == "Mensual":
                fecha_af = datetime.now().strftime("%Y-%m-%d")

            conn.execute(
                """UPDATE clientes
                   SET nombre=?, telefono=?, cedula=?, tipo=?, direccion=?, email=?, cuota_mensual=?, fecha_afiliacion=COALESCE(fecha_afiliacion, ?)
                   WHERE id=? AND empresa_id=?""",
                (nombre, telefono, cedula, tipo, direccion, email, cuota_mensual, fecha_af, id, current_empresa_id()),
            )

            if prev_tipo != "Mensual" and tipo == "Mensual":
                existe_pend = conn.execute("SELECT 1 FROM matriculas WHERE cliente_id=? AND empresa_id=? AND pagado=0", (id, current_empresa_id())).fetchone()
                if not existe_pend:
                    conn.execute(
                        "INSERT INTO matriculas (cliente_id, fecha, monto, pagado, empresa_id) VALUES (?, ?, ?, 0, ?)",
                        (id, datetime.now().strftime("%Y-%m-%d"), 200.0, current_empresa_id())
                    )
                _actualizar_flag_deudor(conn, id, current_empresa_id())

            conn.commit()
        except sqlite3.IntegrityError:
            conn.rollback()
            conn.close()
            flash("Ya existe otro cliente con esa cédula.", "danger")
            return redirect(url_for("clientes"))

        conn.close()
        return redirect(url_for("clientes"))

    cliente = conn.execute("SELECT * FROM clientes WHERE id=? AND empresa_id=?", (id, current_empresa_id())).fetchone()
    conn.close()
    if cliente is None:
        abort(404)
    return render_template("clientes.html", clientes=[], cliente_edit=cliente, filtros={"nombre":"", "cedula":"", "tipo":""}, matricula_pend={}, cuotas_sugeridas={})

@app.route("/clientes/eliminar/<int:id>")
def cliente_eliminar(id):
    conn = get_db()
    conn.execute("DELETE FROM clientes WHERE id=? AND empresa_id=?", (id, current_empresa_id()))
    conn.commit()
    conn.close()
    return redirect(url_for("clientes"))

@app.route("/clientes/baja_deuda/<int:id>", methods=["POST"])
def cliente_baja_deuda(id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE clientes SET activo=0, deudor=1 WHERE id=? AND empresa_id=?", (id, current_empresa_id()))
    conn.commit()
    conn.close()
    flash("Cliente dado de baja por deuda.", "warning")
    return redirect(url_for("clientes"))

@app.route("/clientes/reactivar/<int:id>", methods=["POST"])
def cliente_reactivar(id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE clientes SET activo=1 WHERE id=? AND empresa_id=?", (id, current_empresa_id()))
    conn.commit()
    conn.close()
    flash("Cliente reactivado.", "success")
    return redirect(url_for("clientes"))

@app.route("/matricula/pagar/<int:cliente_id>", methods=["POST"])
def matricula_pagar(cliente_id):
    conn = get_db()
    cur = conn.cursor()
    empresa_id = current_empresa_id_resolved(conn)
    cur.execute("""
        UPDATE matriculas
        SET pagado=1, fecha_pago=?
        WHERE cliente_id=? AND empresa_id=? AND pagado=0
    """, (datetime.now().strftime("%Y-%m-%d %H:%M"), cliente_id, empresa_id))
    _actualizar_flag_deudor(conn, cliente_id, empresa_id)
    conn.commit()
    conn.close()
    flash("Matrícula cobrada.", "success")
    return redirect(url_for("clientes"))

# -------- API: validación de cédula --------
@app.route("/api/cedula/check")
def api_cedula_check():
    cedula = (request.args.get("cedula") or "").strip()
    excluir_id = request.args.get("excluir_id", type=int)
    if not cedula:
        return jsonify({"ok": True, "disponible": True})
    conn = get_db()
    if excluir_id:
        row = conn.execute("SELECT 1 FROM clientes WHERE cedula=? AND id<>? AND empresa_id=?", (cedula, excluir_id, current_empresa_id())).fetchone()
    else:
        row = conn.execute("SELECT 1 FROM clientes WHERE cedula=? AND empresa_id=?", (cedula, current_empresa_id())).fetchone()
    conn.close()
    return jsonify({"ok": True, "disponible": (row is None)})

# -------- Animales --------
@app.route("/animales/<int:cliente_id>")
def animales(cliente_id):
    conn = get_db()
    cliente = conn.execute("SELECT * FROM clientes WHERE id=? AND empresa_id=?", (cliente_id, current_empresa_id())).fetchone()
    if cliente is None:
        conn.close()
        abort(404)
    animales = conn.execute("SELECT * FROM animales WHERE cliente_id=? AND empresa_id=?", (cliente_id, current_empresa_id())).fetchall()
    conn.close()
    return render_template("animales.html", cliente=cliente, animales=animales, animal_edit=None)

@app.route("/animales/nuevo/<int:cliente_id>", methods=["POST"])
def animal_nuevo(cliente_id):
    nombre = request.form.get("nombre", "").strip()
    especie = request.form.get("especie", "").strip()
    raza = request.form.get("raza", "").strip()
    fecha_nacimiento = request.form.get("fecha_nacimiento", "").strip()
    ultima_desparasitacion = request.form.get("ultima_desparasitacion", "").strip()
    conn = get_db()
    conn.execute(
        "INSERT INTO animales (cliente_id, nombre, especie, raza, fecha_nacimiento, ultima_desparasitacion, empresa_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (cliente_id, nombre, especie, raza, fecha_nacimiento, ultima_desparasitacion, current_empresa_id()),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("animales", cliente_id=cliente_id))

@app.route("/animales/editar/<int:id>", methods=["GET", "POST"])
def animal_editar(id):
    conn = get_db()
    if request.method == "POST":
        nombre = request.form.get("nombre", "").strip()
        especie = request.form.get("especie", "").strip()
        raza = request.form.get("raza", "").strip()
        fecha_nacimiento = request.form.get("fecha_nacimiento", "").strip()
        ultima_desparasitacion = request.form.get("ultima_desparasitacion", "").strip()
        conn.execute(
            "UPDATE animales SET nombre=?, especie=?, raza=?, fecha_nacimiento=?, ultima_desparasitacion=? WHERE id=? AND empresa_id=?",
            (nombre, especie, raza, fecha_nacimiento, ultima_desparasitacion, id, current_empresa_id()),
        )
        conn.commit()
        cliente_id = request.args.get("cliente_id")
        conn.close()
        if cliente_id:
            return redirect(url_for("animales", cliente_id=cliente_id))
        else:
            return redirect(url_for("clientes"))
    animal = conn.execute("SELECT * FROM animales WHERE id=? AND empresa_id=?", (id, current_empresa_id())).fetchone()
    conn.close()
    if animal is None:
        abort(404)
    return render_template("animales.html", cliente=None, animales=[], animal_edit=animal)

@app.route("/animales/eliminar/<int:id>")
def animal_eliminar(id):
    conn = get_db()
    animal = conn.execute("SELECT * FROM animales WHERE id=? AND empresa_id=?", (id, current_empresa_id())).fetchone()
    if animal is None:
        conn.close()
        abort(404)
    cliente_id = animal["cliente_id"]
    conn.execute("DELETE FROM animales WHERE id=? AND empresa_id=?", (id, current_empresa_id()))
    conn.commit()
    conn.close()
    return redirect(url_for("animales", cliente_id=cliente_id))

# -------- Historia clínica --------
@app.route("/historia/<int:animal_id>")
def historia(animal_id):
    conn = get_db()
    animal = conn.execute("SELECT * FROM animales WHERE id=? AND empresa_id=?", (animal_id, current_empresa_id())).fetchone()
    if animal is None:
        conn.close()
        abort(404)

    # Traigo doctor y datos de la cita asociada (si la hubo) para armar botón "Próxima cita"
    historia = conn.execute(
        """
        SELECT h.*,
               d.nombre AS doctor_nombre,
               a.doctor_id AS a_doctor_id,
               a.motivo_id AS a_motivo_id,
               a.lugar     AS a_lugar
          FROM historia_clinica h
          LEFT JOIN doctores d ON d.id = h.doctor_id
          LEFT JOIN agenda   a ON a.id = h.cita_id
         WHERE h.animal_id=?
         ORDER BY h.fecha DESC
        """,
        (animal_id,),
    ).fetchall()

    imagenes = {}
    for h in historia:
        imgs = conn.execute(
            "SELECT filename FROM imagenes_historia WHERE historia_id=?",
            (h["id"],),
        ).fetchall()
        imagenes[h["id"]] = imgs
    conn.close()
    return render_template("historia_clinica.html", animal=animal, historia=historia, imagenes=imagenes)

@app.route("/historia/nuevo/<int:animal_id>", methods=["POST"])
def historia_nueva(animal_id):
    descripcion = request.form.get("descripcion", "").strip()
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO historia_clinica (animal_id, fecha, descripcion, empresa_id) VALUES (?, ?, ?, ?)",
        (animal_id, fecha, descripcion, current_empresa_id()),
    )
    historia_id = cur.lastrowid

    if "imagen" in request.files:
        files = request.files.getlist("imagen")
        for file in files:
            if file.filename != "":
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                cur.execute(
                    "INSERT INTO imagenes_historia (historia_id, filename) VALUES (?, ?)",
                    (historia_id, filename),
                )

    conn.commit()
    conn.close()
    return redirect(url_for("historia", animal_id=animal_id))

# -------- Vacunas --------
@app.route("/vacunas/<int:animal_id>")
def vacunas(animal_id):
    conn = get_db()
    animal = conn.execute("SELECT * FROM animales WHERE id=? AND empresa_id=?", (animal_id, current_empresa_id())).fetchone()
    if animal is None:
        conn.close()
        abort(404)
    vacunas = conn.execute("SELECT * FROM vacunas WHERE animal_id=? AND empresa_id=?", (animal_id, current_empresa_id())).fetchall()
    conn.close()
    return render_template("vacunas.html", animal=animal, vacunas=vacunas)

@app.route("/vacunas/nuevo/<int:animal_id>", methods=["POST"])
def vacuna_nueva(animal_id):
    fecha_vacuna = request.form.get("fecha_vacuna")
    fecha_vencimiento = request.form.get("fecha_vencimiento")
    conn = get_db()
    conn.execute(
        "INSERT INTO vacunas (animal_id, fecha_vacuna, fecha_vencimiento, empresa_id) VALUES (?, ?, ?, ?)",
        (animal_id, fecha_vacuna, fecha_vencimiento, current_empresa_id()),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("vacunas", animal_id=animal_id))


@app.route("/desparasitaciones/<int:animal_id>")
def desparasitaciones(animal_id):
    conn = get_db()
    animal = conn.execute("SELECT * FROM animales WHERE id=? AND empresa_id=?", (animal_id, current_empresa_id())).fetchone()
    if animal is None:
        conn.close()
        abort(404)
    desparasitaciones = conn.execute(
        "SELECT * FROM desparasitaciones WHERE animal_id=? AND empresa_id=? ORDER BY fecha_aplicacion DESC, id DESC",
        (animal_id, current_empresa_id())
    ).fetchall()
    conn.close()
    return render_template("desparasitaciones.html", animal=animal, desparasitaciones=desparasitaciones)

@app.route("/desparasitaciones/nuevo/<int:animal_id>", methods=["POST"])
def desparasitacion_nueva(animal_id):
    tipo = request.form.get("tipo", "").strip()
    fecha_aplicacion = request.form.get("fecha_aplicacion")
    fecha_vencimiento = request.form.get("fecha_vencimiento")
    conn = get_db()
    conn.execute(
        "INSERT INTO desparasitaciones (animal_id, tipo, fecha_aplicacion, fecha_vencimiento, empresa_id) VALUES (?, ?, ?, ?, ?)",
        (animal_id, tipo, fecha_aplicacion, fecha_vencimiento, current_empresa_id()),
    )
    # Mantener visible la última desparasitación en la tabla de animales
    try:
        conn.execute(
            "UPDATE animales SET ultima_desparasitacion=? WHERE id=? AND empresa_id=?",
            (fecha_aplicacion, animal_id, current_empresa_id())
        )
    except Exception:
        pass
    conn.commit()
    conn.close()
    return redirect(url_for("desparasitaciones", animal_id=animal_id))

# -------- Archivos subidos --------
@app.route("/uploads/<filename>")
def uploads(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

# -------- Motivos (precios + tipo) --------
@app.route("/motivos")
@require_admin
def motivos():
    conn = get_db()
    motivos = _fetchall_empresa(conn, "SELECT * FROM motivos WHERE empresa_id=?")
    conn.close()
    return render_template("motivos.html", motivos=motivos, motivo_edit=None)

@app.route("/motivos/nuevo", methods=["POST"])
@require_admin
def motivo_nuevo():
    nombre = request.form.get("nombre", "").strip()
    duracion = request.form.get("duracion", "0").strip()
    precio_mensual = request.form.get("precio_mensual", "").strip()
    precio_particular = request.form.get("precio_particular", "").strip()
    tipo = request.form.get("tipo", "consulta").strip()
    genera_historia = 1 if request.form.get("genera_historia") == "1" else 0

    if not duracion.isdigit():
        flash("Duración inválida", "danger")
        return redirect(url_for("motivos"))

    duracion = int(duracion)
    pm = _to_float(precio_mensual)
    pp = _to_float(precio_particular)

    conn = get_db()
    conn.execute(
        "INSERT INTO motivos (nombre, duracion_minutos, precio_mensual, precio_particular, tipo, genera_historia, empresa_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (nombre, duracion, pm, pp, tipo, genera_historia, current_empresa_id()),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("motivos"))

@app.route("/motivos/editar/<int:id>", methods=["GET", "POST"])
@require_admin
def motivo_editar(id):
    conn = get_db()
    if request.method == "POST":
        nombre = request.form.get("nombre", "").strip()
        duracion = request.form.get("duracion", "0").strip()
        precio_mensual = request.form.get("precio_mensual", "").strip()
        precio_particular = request.form.get("precio_particular", "").strip()
        tipo = request.form.get("tipo", "consulta").strip()
        genera_historia = 1 if request.form.get("genera_historia") == "1" else 0

        if not duracion.isdigit():
            conn.close()
            flash("Duración inválida", "danger")
            return redirect(url_for("motivos"))

        duracion = int(duracion)
        pm = _to_float(precio_mensual)
        pp = _to_float(precio_particular)

        conn.execute(
            "UPDATE motivos SET nombre=?, duracion_minutos=?, precio_mensual=?, precio_particular=?, tipo=? WHERE id=? AND empresa_id=?",
            (nombre, duracion, pm, pp, tipo, id),
        )
        conn.commit()
        conn.close()
        return redirect(url_for("motivos"))

    motivo = conn.execute("SELECT * FROM motivos WHERE id=? AND empresa_id=?", (id, current_empresa_id())).fetchone()
    conn.close()
    if motivo is None:
        abort(404)
    return render_template("motivos.html", motivos=[], motivo_edit=motivo)

@app.route("/motivos/eliminar/<int:id>")
@require_admin
def motivo_eliminar(id):
    conn = get_db()
    conn.execute("DELETE FROM motivos WHERE id=? AND empresa_id=?", (id, current_empresa_id()))
    conn.commit()
    conn.close()
    return redirect(url_for("motivos"))

# -------- API precio cita --------
@app.route("/api/precio_cita")
def api_precio_cita():
    cliente_id = request.args.get("cliente_id", type=int)
    motivo_id = request.args.get("motivo_id", type=int)
    fecha = request.args.get("fecha", type=str)
    hora = request.args.get("hora", type=str)
    lugar = request.args.get("lugar", type=str) or "Clinica"

    if not all([cliente_id, motivo_id, fecha, hora]):
        return jsonify({"ok": False, "error": "Faltan parámetros"}), 400

    conn = get_db()
    precio = _precio_cita_calculado(conn, cliente_id, motivo_id, fecha, hora, lugar)
    conn.close()
    return jsonify({"ok": True, "precio": precio})

# -------- Agenda: NUEVA (prefill soportado) --------
@app.route("/agenda/nueva", methods=["GET", "POST"])
def agenda_nueva():
    if request.method == "POST":
        cliente_id = request.form.get("cliente_id")
        animal_id = request.form.get("animal_id")
        doctor_id = request.form.get("doctor_id")
        fecha = request.form.get("fecha")
        hora = request.form.get("hora")
        motivo_id = request.form.get("motivo_id")
        estado_pago = request.form.get("estado_pago", "Debe")
        lugar = request.form.get("lugar", "Clinica")

        if not all([cliente_id, animal_id, doctor_id, fecha, hora, motivo_id]):
            flash("Faltan datos en la cita", "danger")
            return redirect(url_for("agenda_nueva"))

        conn = get_db()
        cur = conn.cursor()
        motivo = cur.execute("SELECT duracion_minutos FROM motivos WHERE id=? AND empresa_id=?", (motivo_id, current_empresa_id())).fetchone()
        if motivo is None:
            conn.close()
            flash("Motivo no válido", "danger")
            return redirect(url_for("agenda_nueva"))
        duracion = motivo["duracion_minutos"]

        try:
            inicio_nuevo = datetime.strptime(f"{fecha} {hora}", "%Y-%m-%d %H:%M")
            fin_nuevo = inicio_nuevo + timedelta(minutes=duracion)
        except Exception:
            conn.close()
            flash("Fecha u hora no válidas", "danger")
            return redirect(url_for("agenda_nueva"))

        citas = cur.execute("""
            SELECT a.hora, m.duracion_minutos
            FROM agenda a
            JOIN motivos m ON a.motivo_id = m.id AND m.empresa_id = a.empresa_id
            WHERE a.fecha = ? AND a.doctor_id = ? AND a.atendida=0 AND a.empresa_id = ?
        """, (fecha, doctor_id, current_empresa_id())).fetchall()

        for c in citas:
            inicio_cita = datetime.strptime(f"{fecha} {c['hora']}", "%Y-%m-%d %H:%M")
            fin_cita = inicio_cita + timedelta(minutes=c["duracion_minutos"])
            if inicio_nuevo < fin_cita and fin_nuevo > inicio_cita:
                conn.close()
                flash(f"Error: La hora elegida solapa con otra cita desde {inicio_cita.strftime('%H:%M')} hasta {fin_cita.strftime('%H:%M')}", "danger")
                return redirect(url_for("agenda_nueva"))

        precio_cita = _precio_cita_calculado(conn, int(cliente_id), int(motivo_id), fecha, hora, lugar)
        if precio_cita == 0:
            estado_pago = "Pagado"

        cur.execute("""
            INSERT INTO agenda (cliente_id, animal_id, doctor_id, fecha, hora, motivo_id, estado_pago, precio, lugar, atendida, empresa_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, (cliente_id, animal_id, doctor_id, fecha, hora, motivo_id, estado_pago, precio_cita, lugar, current_empresa_id()))

        cita_id = cur.lastrowid
        _actualizar_flag_deudor(conn, int(cliente_id))
        conn.commit()
        conn.close()

        flash("Cita agendada correctamente", "success")
        return redirect(url_for("whatsapp_web_cita", cita_id=cita_id))

    # --- GET: armar datos para el form (JSON-serializable para buscador) ---
    conn = get_db()
    clientes_rows = conn.execute("""
        SELECT c.id, c.nombre, c.cedula,
               COALESCE(GROUP_CONCAT(a.nombre, ' || '), '') AS animales
        FROM clientes c
        LEFT JOIN animales a
               ON a.cliente_id = c.id
              AND a.empresa_id = c.empresa_id
        WHERE c.empresa_id = ?
        GROUP BY c.id, c.nombre, c.cedula
        ORDER BY c.nombre COLLATE NOCASE
    """, (current_empresa_id(),)).fetchall()
    clientes = []
    for r in clientes_rows:
        animales = [x.strip() for x in (r["animales"] or '').split(' || ') if x and x.strip()]
        clientes.append({
            "id": r["id"],
            "nombre": r["nombre"],
            "cedula": r["cedula"],
            "animales": animales,
        })

    motivos_rows = conn.execute("SELECT * FROM motivos WHERE empresa_id=?", (current_empresa_id(),)).fetchall()
    motivos = [dict(m) for m in motivos_rows]
    doctores_rows = conn.execute("SELECT id, nombre FROM doctores WHERE empresa_id=?", (current_empresa_id(),)).fetchall()
    doctores = [dict(d) for d in doctores_rows]
    conn.close()

    # PREFILL por querystring
    preset = {
        "cliente_id": request.args.get("cliente_id", type=int),
        "animal_id":  request.args.get("animal_id",  type=int),
        "doctor_id":  request.args.get("doctor_id",  type=int),
        "motivo_id":  request.args.get("motivo_id",  type=int),
        "lugar":      request.args.get("lugar") or "Clinica",
        "fecha":      request.args.get("fecha"),
        "hora":       request.args.get("hora"),
    }
    return render_template("agenda_nueva.html", clientes=clientes, motivos=motivos, doctores=doctores, preset=preset)

@app.route("/api/animales/<int:cliente_id>")
def api_animales(cliente_id):
    conn = get_db()
    animales = conn.execute("SELECT id, nombre FROM animales WHERE cliente_id=? AND empresa_id=?", (cliente_id, current_empresa_id())).fetchall()
    conn.close()
    lista = [{"id": a["id"], "nombre": a["nombre"]} for a in animales]
    return jsonify(lista)

# -------- Agenda: LISTA (con filtro de estado y botón Atender) --------
@app.route("/agenda")
def agenda_lista():
    conn = get_db()
    fecha = request.args.get("fecha")
    doctor_id = request.args.get("doctor_id")
    cliente_id = request.args.get("cliente_id")
    estado = request.args.get("estado", "Pendiente")  # Pendiente / Atendidas / Todas

    query = """
        SELECT a.id, a.fecha, a.hora, a.lugar, a.atendida,
               c.nombre as cliente_nombre, an.nombre as animal_nombre,
               d.nombre as doctor_nombre, m.nombre as motivo_nombre, m.tipo as motivo_tipo,
               a.estado_pago, a.precio, a.cobrada_mensualidad_id
        FROM agenda a
        JOIN clientes c ON a.cliente_id = c.id
        JOIN animales an ON a.animal_id = an.id
        JOIN doctores d ON a.doctor_id = d.id
        LEFT JOIN motivos m ON a.motivo_id = m.id
        WHERE a.empresa_id = ?
    """
    params = [current_empresa_id()]

    if fecha:
        query += " AND a.fecha = ?"
        params.append(fecha)
    if doctor_id:
        query += " AND a.doctor_id = ?"
        params.append(doctor_id)
    if cliente_id:
        query += " AND a.cliente_id = ?"
        params.append(cliente_id)
    if estado == "Pendiente":
        query += " AND a.atendida = 0"
    elif estado == "Atendidas":
        query += " AND a.atendida = 1"

    query += " ORDER BY a.fecha DESC, a.hora DESC"

    citas = conn.execute(query, params).fetchall()
    doctores = conn.execute("SELECT id, nombre FROM doctores WHERE empresa_id=?", (current_empresa_id(),)).fetchall()
    clientes = conn.execute("SELECT id, nombre FROM clientes WHERE empresa_id=?", (current_empresa_id(),)).fetchall()
    conn.close()

    filtros = {"fecha": fecha, "doctor_id": doctor_id, "cliente_id": cliente_id, "estado": estado}
    return render_template("agenda_lista.html", citas=citas, doctores=doctores, clientes=clientes, filtros=filtros)

@app.route("/agenda/estado_pago/<int:cita_id>", methods=["POST"])
def agenda_actualizar_estado_pago(cita_id):
    if not request.is_json:
        return jsonify({"success": False, "error": "Invalid request"}), 400
    data = request.get_json()
    nuevo_estado = data.get("estado_pago")
    if nuevo_estado not in ["Debe", "Pagado"]:
        return jsonify({"success": False, "error": "Estado inválido"}), 400

    conn = get_db()
    cur = conn.cursor()

    empresa_id = current_empresa_id_resolved(conn)
    row = cur.execute("SELECT cliente_id FROM agenda WHERE id=? AND empresa_id=?", (cita_id, empresa_id)).fetchone()
    if not row:
        conn.close()
        return jsonify({"success": False, "error": "Cita no encontrada"}), 404

    cur.execute("UPDATE agenda SET estado_pago=? WHERE id=? AND empresa_id=?", (nuevo_estado, cita_id, empresa_id))
    _actualizar_flag_deudor(conn, row['cliente_id'], empresa_id)

    conn.commit()
    conn.close()
    return jsonify({"success": True})

# -------- Atender cita: crea historia y marca la cita como atendida --------
@app.route("/agenda/atender/<int:cita_id>", methods=["GET", "POST"])
def atender_cita(cita_id):
    conn = get_db()
    cur = conn.cursor()
    cita = cur.execute(
        """
        SELECT a.*,
               c.nombre  AS cliente_nombre, c.telefono AS cliente_telefono, c.direccion AS cliente_direccion,
               an.nombre AS animal_nombre, an.cliente_id AS cid,
               d.nombre  AS doctor_nombre,
               m.nombre  AS motivo_nombre, m.tipo AS motivo_tipo, m.genera_historia AS motivo_genera_historia
          FROM agenda a
          JOIN clientes c ON c.id = a.cliente_id
          JOIN animales an ON an.id = a.animal_id
          JOIN doctores d ON d.id = a.doctor_id
          LEFT JOIN motivos m ON m.id = a.motivo_id
         WHERE a.id = ? AND a.empresa_id = ?
        """, (cita_id, current_empresa_id())
    ).fetchone()
    if not cita:
        conn.close()
        flash("Cita no encontrada.", "danger")
        return redirect(url_for("agenda_lista"))

    motivo_nombre_norm = ((cita["motivo_nombre"] or "").strip()).lower()
    genera_historia = int(cita["motivo_genera_historia"] if cita["motivo_genera_historia"] is not None else 1)
    if "peluquer" in motivo_nombre_norm:
        genera_historia = 0

    if request.method == "GET" and not genera_historia:
        cur.execute("UPDATE agenda SET atendida=1 WHERE id=? AND empresa_id=?", (cita_id, current_empresa_id()))
        conn.commit()
        conn.close()
        flash("La cita fue marcada como atendida. Este motivo no genera historia clínica.", "success")
        return redirect(url_for("agenda_lista"))

    if request.method == "POST":
        descripcion = (request.form.get("descripcion") or "").strip()
        if not descripcion:
            flash("La descripción/examen es obligatoria.", "warning")
            conn.close()
            return render_template("atender_cita.html", cita=cita)

        # Datos de historia
        peso_kg  = _to_float(request.form.get("peso_kg"))
        temp_c   = _to_float(request.form.get("temp_c"))
        fc       = request.form.get("fc") or None
        fr       = request.form.get("fr") or None
        mucosas  = (request.form.get("mucosas") or "").strip() or None
        hidratacion = (request.form.get("hidratacion") or "").strip() or None

        motivo_consulta       = (request.form.get("motivo_consulta") or "").strip() or None
        anamnesis             = (request.form.get("anamnesis") or "").strip() or None
        dx_presuntivo         = (request.form.get("diagnostico_presuntivo") or "").strip() or None
        dx_diferencial        = (request.form.get("diagnostico_diferencial") or "").strip() or None
        tratamiento           = (request.form.get("tratamiento") or "").strip() or None
        indicaciones          = (request.form.get("indicaciones") or "").strip() or None
        particularidades      = (request.form.get("particularidades") or "").strip() or None
        proxima_cita_texto    = (request.form.get("proxima_cita") or "").strip() or None

        ahora = datetime.now().strftime("%Y-%m-%d %H:%M")

        # Guardar historia (vinculada a doctor y a esta cita)
        cur.execute("""
            INSERT INTO historia_clinica
            (animal_id, fecha, descripcion,
             peso_kg, temp_c, fc, fr, mucosas, hidratacion,
             motivo_consulta, anamnesis,
             diagnostico_presuntivo, diagnostico_diferencial,
             tratamiento, indicaciones, particularidades,
             proxima_cita, tipo_visita,
             doctor_id, cita_id)
            VALUES
            ( ?, ?, ?,
              ?, ?, ?, ?, ?, ?,
              ?, ?,
              ?, ?,
              ?, ?, ?,
              ?, ?, ?, ? )
        """, (
            cita["animal_id"], ahora, descripcion,
            peso_kg, temp_c, fc, fr, mucosas, hidratacion,
            motivo_consulta, anamnesis,
            dx_presuntivo, dx_diferencial,
            tratamiento, indicaciones, particularidades,
            proxima_cita_texto, cita["motivo_tipo"],
            cita["doctor_id"], cita_id
        ))
        historia_id = cur.lastrowid

        # Imágenes
        if "imagen" in request.files:
            files = request.files.getlist("imagen")
            for file in files:
                if file and file.filename:
                    filename = secure_filename(file.filename)
                    file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                    cur.execute(
                        "INSERT INTO imagenes_historia (historia_id, filename) VALUES (?, ?)",
                        (historia_id, filename)
                    )

        # Marcar cita como atendida
        cur.execute("UPDATE agenda SET atendida=1 WHERE id=? AND empresa_id=?", (cita_id, current_empresa_id()))
        conn.commit()
        conn.close()
        flash("Historia clínica registrada. La cita fue marcada como atendida.", "success")
        return redirect(url_for("historia", animal_id=cita['animal_id']))

    conn.close()
    return render_template("atender_cita.html", cita=cita)

# -------- WhatsApp Web: abre chat auto y vuelve a la agenda --------
@app.route("/whatsapp/cita/<int:cita_id>")
def whatsapp_web_cita(cita_id):
    conn = get_db()
    cur = conn.cursor()
    empresa_id = current_empresa_id_resolved(conn)
    cita = cur.execute("SELECT * FROM agenda WHERE id=? AND empresa_id=?", (cita_id, empresa_id)).fetchone()
    if not cita:
        conn.close()
        flash("Cita no encontrada para WhatsApp.", "danger")
        return redirect(url_for(CLINIC_WHATSAPP_RETURN))

    cli = cur.execute("SELECT * FROM clientes WHERE id=? AND empresa_id=?", (cita['cliente_id'], empresa_id)).fetchone()
    ani = cur.execute("SELECT * FROM animales WHERE id=? AND empresa_id=?", (cita['animal_id'], empresa_id)).fetchone()
    doc = cur.execute("SELECT * FROM doctores WHERE id=? AND empresa_id=?", (cita['doctor_id'], empresa_id)).fetchone()
    mot = cur.execute("SELECT * FROM motivos WHERE id=? AND empresa_id=?", (cita['motivo_id'], empresa_id)).fetchone()
    conn.close()

    phone_digits = _uy_to_e164_digits(cli['telefono'] if cli else "")
    if not phone_digits:
        flash("El cliente no tiene teléfono válido para WhatsApp.", "warning")
        return redirect(url_for(CLINIC_WHATSAPP_RETURN))

    text = build_whatsapp_text(cita, cli, ani, doc, mot)
    # Abrir WhatsApp en una pestaña nueva y mantener el sistema abierto.
    return render_template("whatsapp.auto.html", phone=phone_digits, text=text)
def _normalizar_mensualidades_empresa(conn, empresa_id=None):
    empresa_id = empresa_id or current_empresa_id_resolved(conn)
    if not empresa_id:
        return
    conn.execute(
        """
        UPDATE mensualidades
           SET empresa_id = ?
         WHERE cliente_id IN (SELECT id FROM clientes WHERE empresa_id = ?)
           AND COALESCE(empresa_id, 0) <> ?
        """,
        (empresa_id, empresa_id, empresa_id),
    )
    conn.commit()

def _sanear_facturacion_empresa(conn, empresa_id=None):
    empresa_id = empresa_id or current_empresa_id_resolved(conn)
    if not empresa_id:
        return

    conn.execute(
        """
        DELETE FROM mensualidades
         WHERE empresa_id = ?
           AND cliente_id NOT IN (
               SELECT id FROM clientes WHERE empresa_id = ?
           )
        """,
        (empresa_id, empresa_id),
    )

    conn.execute(
        """
        DELETE FROM matriculas
         WHERE empresa_id = ?
           AND cliente_id NOT IN (
               SELECT id FROM clientes WHERE empresa_id = ?
           )
        """,
        (empresa_id, empresa_id),
    )

    conn.execute(
        """
        UPDATE agenda
           SET cobrada_mensualidad_id = NULL
         WHERE empresa_id = ?
           AND cobrada_mensualidad_id IS NOT NULL
           AND cobrada_mensualidad_id NOT IN (
               SELECT id FROM mensualidades WHERE empresa_id = ?
           )
        """,
        (empresa_id, empresa_id),
    )

    conn.commit()


@app.route('/mensualidades', methods=['GET'])
@require_auth
def mensualidades():
    hoy = datetime.now()
    anio = int(request.values.get('anio', hoy.year))
    mes = int(request.values.get('mes', hoy.month))
    conn = get_db()
    empresa_id = current_empresa_id_resolved(conn)

    cur = conn.cursor()

    _normalizar_mensualidades_empresa(conn, empresa_id)
    _sanear_facturacion_empresa(conn, empresa_id)

    mensuales = cur.execute(
        "SELECT id, cuota_mensual FROM clientes WHERE tipo='Mensual' AND activo=1 AND empresa_id=?",
        (empresa_id,),
    ).fetchall()

    for c in mensuales:
        cuota = c['cuota_mensual'] if c['cuota_mensual'] is not None else _calc_cuota_automatica(conn, c['id'])
        cur.execute(
            """
            INSERT OR IGNORE INTO mensualidades (cliente_id, anio, mes, pagado, monto_cuota, monto_pagado, empresa_id)
            VALUES (?, ?, ?, 0, ?, 0, ?)
            """,
            (c['id'], anio, mes, cuota, empresa_id),
        )
        cur.execute(
            """
            UPDATE mensualidades
               SET monto_cuota = CASE WHEN monto_cuota IS NULL OR monto_cuota = 0 THEN ? ELSE monto_cuota END,
                   empresa_id = ?
             WHERE cliente_id = ? AND anio = ? AND mes = ?
            """,
            (cuota, empresa_id, c['id'], anio, mes),
        )

    conn.commit()

    filas = cur.execute(
        """
        SELECT me.id AS mensualidad_id, me.cliente_id, me.anio, me.mes, me.pagado, me.fecha_pago,
               me.monto_cuota, me.monto_pagado,
               cl.nombre, cl.cedula, cl.telefono, cl.deudor, cl.activo
          FROM mensualidades me
          JOIN clientes cl ON cl.id = me.cliente_id
         WHERE me.anio = ? AND me.mes = ? AND me.empresa_id = ? AND cl.empresa_id = ? AND cl.tipo='Mensual' AND cl.activo=1
         ORDER BY cl.nombre COLLATE NOCASE
        """,
        (anio, mes, empresa_id, empresa_id),
    ).fetchall()

    registros = []
    for r in filas:
        extras = cur.execute(
            "SELECT COALESCE(SUM(COALESCE(precio,0)),0) s FROM agenda WHERE cobrada_mensualidad_id=? AND empresa_id=?",
            (r['mensualidad_id'], empresa_id),
        ).fetchone()['s']
        total = (r['monto_cuota'] or 0) + (extras or 0)
        saldo = max(total - (r['monto_pagado'] or 0), 0)
        registros.append({**dict(r), 'extras': extras, 'total': total, 'saldo': saldo})

    conn.close()
    return render_template('mensualidades.html', registros=registros, anio=anio, mes=mes)


@app.route('/mensualidades/toggle/<int:mensualidad_id>', methods=['POST'])
@require_auth
def mensualidad_toggle(mensualidad_id):
    conn = get_db()
    empresa_id = current_empresa_id_resolved(conn)
    conn = get_db()
    _sanear_facturacion_empresa(conn, empresa_id)
    cur = conn.cursor()

    me = cur.execute(
        "SELECT me.*, cl.cuota_mensual, cl.id AS cid FROM mensualidades me JOIN clientes cl ON cl.id=me.cliente_id AND cl.empresa_id=me.empresa_id WHERE me.id=? AND me.empresa_id=?",
        (mensualidad_id, empresa_id),
    ).fetchone()
    if me is None:
        conn.close()
        return jsonify({'success': False, 'error': 'Mensualidad no encontrada'}), 404

    extras = cur.execute(
        "SELECT COALESCE(SUM(COALESCE(precio,0)),0) s FROM agenda WHERE cobrada_mensualidad_id=? AND empresa_id=?",
        (mensualidad_id, empresa_id),
    ).fetchone()['s']
    monto_cuota = me['monto_cuota'] if me['monto_cuota'] is not None else me['cuota_mensual']
    total = (monto_cuota or 0) + (extras or 0)

    if me['pagado'] == 0:
        fecha_pago = datetime.now().strftime('%Y-%m-%d %H:%M')
        cur.execute("UPDATE mensualidades SET pagado=1, fecha_pago=?, monto_pagado=? WHERE id=? AND empresa_id=?", (fecha_pago, total, mensualidad_id, empresa_id))
        cur.execute("UPDATE agenda SET estado_pago='Pagado' WHERE cobrada_mensualidad_id=? AND empresa_id=?", (mensualidad_id, empresa_id))
    else:
        cur.execute("UPDATE mensualidades SET pagado=0, fecha_pago=NULL, monto_pagado=0 WHERE id=? AND empresa_id=?", (mensualidad_id, empresa_id))
        cur.execute(
            """
            UPDATE agenda
               SET estado_pago='Debe'
             WHERE cobrada_mensualidad_id=? AND empresa_id=? AND COALESCE(precio,0) > 0
            """,
            (mensualidad_id, empresa_id),
        )

    _actualizar_flag_deudor(conn, me['cid'])
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'pagado': (me['pagado'] == 0)})


@app.route('/mensualidades/registrar_pago/<int:cliente_id>', methods=['POST'])
@require_auth
def mensualidades_registrar_pago(cliente_id):
    conn = get_db()
    empresa_id = current_empresa_id_resolved(conn)
    hoy = datetime.now()
    anio, mes = hoy.year, hoy.month
    conn = get_db()
    cur = conn.cursor()

    cl = cur.execute('SELECT cuota_mensual FROM clientes WHERE id=? AND empresa_id=?', (cliente_id, empresa_id)).fetchone()
    cuota = cl['cuota_mensual'] if cl and cl['cuota_mensual'] is not None else _calc_cuota_automatica(conn, cliente_id)
    cur.execute(
        """
        INSERT OR IGNORE INTO mensualidades (cliente_id, anio, mes, pagado, monto_cuota, monto_pagado, empresa_id)
        VALUES (?, ?, ?, 0, ?, 0, ?)
        """,
        (cliente_id, anio, mes, cuota, empresa_id),
    )

    me = cur.execute(
        'SELECT id, monto_cuota FROM mensualidades WHERE cliente_id=? AND anio=? AND mes=? AND empresa_id=?',
        (cliente_id, anio, mes, empresa_id),
    ).fetchone()
    extras = cur.execute(
        'SELECT COALESCE(SUM(COALESCE(precio,0)),0) s FROM agenda WHERE cobrada_mensualidad_id=? AND empresa_id=?',
        (me['id'], empresa_id),
    ).fetchone()['s']
    total = (me['monto_cuota'] or 0) + (extras or 0)

    fecha_pago = datetime.now().strftime('%Y-%m-%d %H:%M')
    cur.execute('UPDATE mensualidades SET pagado=1, fecha_pago=?, monto_pagado=? WHERE id=? AND empresa_id=?', (fecha_pago, total, me['id'], empresa_id))
    cur.execute("UPDATE agenda SET estado_pago='Pagado' WHERE cobrada_mensualidad_id=? AND empresa_id=?", (me['id'], empresa_id))

    _actualizar_flag_deudor(conn, cliente_id)
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/mensualidades/abonar/<int:mensualidad_id>', methods=['POST'])
@require_auth
def mensualidades_abonar(mensualidad_id):
    conn = get_db()
    empresa_id = current_empresa_id_resolved(conn)
    data = request.get_json(force=True) if request.is_json else request.form
    monto = _to_float(data.get('monto'))
    if monto is None or monto <= 0:
        return jsonify({'success': False, 'error': 'Monto inválido'}), 400

    conn = get_db()
    cur = conn.cursor()
    me = cur.execute(
        "SELECT me.*, cl.cuota_mensual, cl.id AS cid FROM mensualidades me JOIN clientes cl ON cl.id=me.cliente_id AND cl.empresa_id=me.empresa_id WHERE me.id=? AND me.empresa_id=?",
        (mensualidad_id, empresa_id),
    ).fetchone()
    if not me:
        conn.close()
        return jsonify({'success': False, 'error': 'Mensualidad no encontrada'}), 404

    extras = cur.execute(
        'SELECT COALESCE(SUM(COALESCE(precio,0)),0) s FROM agenda WHERE cobrada_mensualidad_id=? AND empresa_id=?',
        (mensualidad_id, empresa_id),
    ).fetchone()['s']
    monto_cuota = me['monto_cuota'] if me['monto_cuota'] is not None else me['cuota_mensual']
    total = (monto_cuota or 0) + (extras or 0)
    nuevo_pagado = (me['monto_pagado'] or 0) + monto

    if nuevo_pagado >= total:
        fecha_pago = datetime.now().strftime('%Y-%m-%d %H:%M')
        cur.execute('UPDATE mensualidades SET monto_pagado=?, pagado=1, fecha_pago=? WHERE id=? AND empresa_id=?', (nuevo_pagado, fecha_pago, mensualidad_id, empresa_id))
        cur.execute("UPDATE agenda SET estado_pago='Pagado' WHERE cobrada_mensualidad_id=? AND empresa_id=?", (mensualidad_id, empresa_id))
    else:
        cur.execute('UPDATE mensualidades SET monto_pagado=?, pagado=0 WHERE id=? AND empresa_id=?', (nuevo_pagado, mensualidad_id, empresa_id))

    _actualizar_flag_deudor(conn, me['cid'])
    conn.commit()
    conn.close()
    return jsonify({'success': True})


def _asegurar_mensualidades_anio(conn, anio:int):
    cur = conn.cursor()
    empresa_id = current_empresa_id()
    _sanear_facturacion_empresa(conn, empresa_id)
    mensuales = conn.execute("SELECT id, cuota_mensual FROM clientes WHERE tipo='Mensual' AND empresa_id=?", (empresa_id,)).fetchall()
    for c in mensuales:
        cuota = c['cuota_mensual'] if c['cuota_mensual'] is not None else _calc_cuota_automatica(conn, c['id'])
        for mes in range(1, 13):
            cur.execute(
                """
                INSERT OR IGNORE INTO mensualidades (cliente_id, anio, mes, pagado, monto_cuota, monto_pagado, empresa_id)
                VALUES (?, ?, ?, 0, ?, 0, ?)
                """,
                (c['id'], anio, mes, cuota, empresa_id),
            )
            cur.execute(
                'UPDATE mensualidades SET empresa_id=? WHERE cliente_id=? AND anio=? AND mes=?',
                (empresa_id, c['id'], anio, mes),
            )
    conn.commit()


@app.route('/mensualidades/anual')
@require_auth
def mensualidades_anual():
    anio = int(request.args.get('anio', datetime.now().year))
    conn = get_db()
    empresa_id = current_empresa_id_resolved(conn)
    conn = get_db()
    _sanear_facturacion_empresa(conn, empresa_id)
    _asegurar_mensualidades_anio(conn, anio)
    cur = conn.cursor()

    clientes = cur.execute(
        """
        SELECT id, nombre, cedula, telefono, deudor, tipo
          FROM clientes
         WHERE tipo='Mensual' AND empresa_id=?
         ORDER BY nombre COLLATE NOCASE
        """,
        (empresa_id,),
    ).fetchall()
    filas = cur.execute(
        """
        SELECT me.id AS mensualidad_id, me.cliente_id, me.mes, me.pagado, me.fecha_pago
          FROM mensualidades me
          JOIN clientes cl ON cl.id = me.cliente_id
         WHERE me.anio = ? AND me.empresa_id = ? AND cl.empresa_id = ? AND cl.tipo='Mensual'
        """,
        (anio, empresa_id, empresa_id),
    ).fetchall()
    conn.close()

    pagos = {}
    for r in filas:
        pagos[(r['cliente_id'], r['mes'])] = r

    return render_template('mensualidades_anual.html', anio=anio, clientes=clientes, pagos=pagos)


@app.route('/mensualidades/cliente/<int:cliente_id>')
@require_auth
def mensualidades_cliente(cliente_id):
    anio = int(request.args.get('anio', datetime.now().year))
    conn = get_db()
    empresa_id = current_empresa_id_resolved(conn)
    conn = get_db()
    _sanear_facturacion_empresa(conn, empresa_id)
    _asegurar_mensualidades_anio(conn, anio)
    cur = conn.cursor()

    cliente = cur.execute('SELECT * FROM clientes WHERE id=? AND empresa_id=?', (cliente_id, empresa_id)).fetchone()
    if not cliente:
        conn.close()
        abort(404)

    regs = cur.execute(
        """
        SELECT id AS mensualidad_id, mes, pagado, fecha_pago
          FROM mensualidades
         WHERE cliente_id=? AND anio=? AND empresa_id=?
         ORDER BY mes
        """,
        (cliente_id, anio, empresa_id),
    ).fetchall()
    conn.close()

    por_mes = {r['mes']: r for r in regs}
    return render_template('mensualidades_cliente.html', anio=anio, cliente=cliente, por_mes=por_mes)


# ------- Gestionar consultas dentro de la mensualidad -------
@app.route('/mensualidades/gestionar/<int:mensualidad_id>')
@require_auth
def mensualidad_gestionar(mensualidad_id):
    conn = get_db()
    empresa_id = current_empresa_id_resolved(conn)
    conn = get_db()
    cur = conn.cursor()

    me = cur.execute(
        """
        SELECT me.*, cl.nombre AS cliente_nombre, cl.cuota_mensual
          FROM mensualidades me
          JOIN clientes cl ON cl.id=me.cliente_id AND cl.empresa_id=me.empresa_id
         WHERE me.id=? AND me.empresa_id=?
        """,
        (mensualidad_id, empresa_id),
    ).fetchone()
    if not me:
        conn.close()
        abort(404)

    ini, fin = _primer_y_ultimo_dia(me['anio'], me['mes'])

    citas = cur.execute(
        """
        SELECT a.id, a.fecha, a.hora, a.estado_pago, a.cobrada_mensualidad_id, a.precio, a.lugar,
               d.nombre AS doctor_nombre, an.nombre AS animal_nombre, m.nombre AS motivo_nombre
          FROM agenda a
          JOIN doctores d ON d.id = a.doctor_id AND d.empresa_id = a.empresa_id
          JOIN animales an ON an.id = a.animal_id AND an.empresa_id = a.empresa_id
          LEFT JOIN motivos m ON m.id = a.motivo_id AND m.empresa_id = a.empresa_id
         WHERE a.cliente_id=? AND a.empresa_id=? AND a.fecha BETWEEN ? AND ?
         ORDER BY a.fecha, a.hora
        """,
        (me['cliente_id'], empresa_id, ini, fin),
    ).fetchall()

    extras = cur.execute(
        'SELECT COALESCE(SUM(COALESCE(precio,0)),0) s FROM agenda WHERE cobrada_mensualidad_id=? AND empresa_id=?',
        (mensualidad_id, empresa_id),
    ).fetchone()['s']
    monto_cuota = me['monto_cuota'] if me['monto_cuota'] is not None else _calc_cuota_automatica(conn, me['cliente_id'])
    total = (monto_cuota or 0) + (extras or 0)
    saldo = max(total - (me['monto_pagado'] or 0), 0)

    conn.close()
    return render_template('mensualidad_gestion.html', mensualidad=me, citas=citas, extras=extras, monto_cuota=monto_cuota, total=total, saldo=saldo)


@app.route('/mensualidades/asignar_cita', methods=['POST'])
@require_auth
def mensualidad_asignar_cita():
    conn = get_db()
    empresa_id = current_empresa_id_resolved(conn)
    data = request.get_json(force=True)
    cita_id = data.get('cita_id')
    mensualidad_id = data.get('mensualidad_id')
    if not cita_id or not mensualidad_id:
        return jsonify({'success': False, 'error': 'Datos incompletos'}), 400

    conn = get_db()
    cur = conn.cursor()

    mensualidad = cur.execute('SELECT id, cliente_id, pagado FROM mensualidades WHERE id=? AND empresa_id=?', (mensualidad_id, empresa_id)).fetchone()
    if not mensualidad:
        conn.close()
        return jsonify({'success': False, 'error': 'Mensualidad no encontrada'}), 404

    cita = cur.execute('SELECT id, cliente_id FROM agenda WHERE id=? AND empresa_id=?', (cita_id, empresa_id)).fetchone()
    if not cita or cita['cliente_id'] != mensualidad['cliente_id']:
        conn.close()
        return jsonify({'success': False, 'error': 'La cita no pertenece a esta veterinaria o a ese cliente'}), 400

    cur.execute('UPDATE agenda SET cobrada_mensualidad_id=? WHERE id=? AND empresa_id=?', (mensualidad_id, cita_id, empresa_id))

    if mensualidad['pagado'] == 1:
        cur.execute("UPDATE agenda SET estado_pago='Pagado' WHERE id=? AND empresa_id=?", (cita_id, empresa_id))
        _actualizar_flag_deudor(conn, cita['cliente_id'])

    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/mensualidades/quitar_cita', methods=['POST'])
@require_auth
def mensualidad_quitar_cita():
    conn = get_db()
    empresa_id = current_empresa_id_resolved(conn)
    data = request.get_json(force=True)
    cita_id = data.get('cita_id')
    if not cita_id:
        return jsonify({'success': False, 'error': 'Datos incompletos'}), 400

    conn = get_db()
    cur = conn.cursor()
    row = cur.execute('SELECT cliente_id, COALESCE(precio,0) precio FROM agenda WHERE id=? AND empresa_id=?', (cita_id, empresa_id)).fetchone()
    if not row:
        conn.close()
        return jsonify({'success': False, 'error': 'Cita no encontrada'}), 404

    cur.execute('UPDATE agenda SET cobrada_mensualidad_id=NULL WHERE id=? AND empresa_id=?', (cita_id, empresa_id))
    cur.execute(
        "UPDATE agenda SET estado_pago = CASE WHEN ? > 0 THEN 'Debe' ELSE estado_pago END WHERE id=? AND empresa_id=?",
        (row['precio'], cita_id, empresa_id),
    )
    _actualizar_flag_deudor(conn, row['cliente_id'])

    conn.commit()
    conn.close()
    return jsonify({'success': True})


# ---------- PARTICULARES: Resumen y Detalle mensual ----------
@app.route('/particulares', methods=['GET'])
@require_auth
def particulares_resumen():
    hoy = datetime.now()
    anio = int(request.args.get('anio', hoy.year))
    mes = int(request.args.get('mes', hoy.month))
    q_nombre = (request.args.get('nombre') or '').strip()
    q_cedula = (request.args.get('cedula') or '').strip()
    conn = get_db()
    empresa_id = current_empresa_id_resolved(conn)

    ini, fin = _primer_y_ultimo_dia(anio, mes)
    conn = get_db()
    _sanear_facturacion_empresa(conn, empresa_id)
    params = [ini, fin, empresa_id]
    filtro_sql = ''
    if q_nombre:
        filtro_sql += ' AND c.nombre LIKE ?'
        params.append(f'%{q_nombre}%')
    if q_cedula:
        filtro_sql += ' AND c.cedula LIKE ?'
        params.append(f'%{q_cedula}%')

    filas = conn.execute(
        f"""
        SELECT c.id, c.nombre, c.cedula, c.telefono,
               COUNT(a.id) AS citas,
               COALESCE(SUM(COALESCE(a.precio,0)),0) AS total,
               COALESCE(SUM(CASE WHEN a.estado_pago='Pagado' THEN COALESCE(a.precio,0) ELSE 0 END),0) AS cobrado
          FROM clientes c
          LEFT JOIN agenda a
            ON a.cliente_id = c.id AND a.empresa_id = c.empresa_id AND a.fecha BETWEEN ? AND ?
         WHERE c.tipo='Particular' AND c.empresa_id=? {filtro_sql}
         GROUP BY c.id
         ORDER BY c.nombre COLLATE NOCASE
        """,
        params,
    ).fetchall()
    conn.close()

    registros = []
    for r in filas:
        total = float(r['total'] or 0)
        cobrado = float(r['cobrado'] or 0)
        registros.append({
            'id': r['id'],
            'nombre': r['nombre'],
            'cedula': r['cedula'],
            'telefono': r['telefono'],
            'citas': r['citas'],
            'total': total,
            'cobrado': cobrado,
            'saldo': max(total - cobrado, 0.0),
        })

    return render_template('particulares_resumen.html', anio=anio, mes=mes, registros=registros, filtros={'nombre': q_nombre, 'cedula': q_cedula})


@app.route('/particulares/cliente/<int:cliente_id>')
@require_auth
def particulares_cliente(cliente_id):
    hoy = datetime.now()
    anio = int(request.args.get('anio', hoy.year))
    mes = int(request.args.get('mes', hoy.month))
    conn = get_db()
    empresa_id = current_empresa_id_resolved(conn)
    ini, fin = _primer_y_ultimo_dia(anio, mes)

    conn = get_db()
    _sanear_facturacion_empresa(conn, empresa_id)
    cliente = conn.execute('SELECT * FROM clientes WHERE id=? AND empresa_id=?', (cliente_id, empresa_id)).fetchone()
    if not cliente or cliente['tipo'] != 'Particular':
        conn.close()
        abort(404)

    citas = conn.execute(
        """
        SELECT a.id, a.fecha, a.hora, a.estado_pago, a.precio, a.lugar,
               d.nombre AS doctor_nombre, an.nombre AS animal_nombre, m.nombre AS motivo_nombre
          FROM agenda a
          JOIN doctores d ON d.id=a.doctor_id AND d.empresa_id=a.empresa_id
          JOIN animales an ON an.id=a.animal_id AND an.empresa_id=a.empresa_id
          LEFT JOIN motivos m ON m.id=a.motivo_id AND m.empresa_id=a.empresa_id
         WHERE a.cliente_id=? AND a.empresa_id=? AND a.fecha BETWEEN ? AND ?
         ORDER BY a.fecha, a.hora
        """,
        (cliente_id, empresa_id, ini, fin),
    ).fetchall()

    tot = sum((c['precio'] or 0) for c in citas)
    cob = sum((c['precio'] or 0) for c in citas if c['estado_pago'] == 'Pagado')
    saldo = max(tot - cob, 0)

    conn.close()
    return render_template('particulares_cliente.html', cliente=cliente, anio=anio, mes=mes, citas=citas, total=tot, cobrado=cob, saldo=saldo)

# ---------- FERIADOS ----------
@app.route("/feriados", methods=["GET", "POST"])
@require_admin
def feriados():
    conn = get_db()
    cur = conn.cursor()

    if request.method == "POST":
        fechas_raw = (request.form.get("fechas") or "").strip()
        if fechas_raw:
            tokens = fechas_raw.replace(";", ",").replace("\n", ",").split(",")
            agregados = duplicados = invalidos = 0
            for t in tokens:
                t = t.strip()
                if not t:
                    continue
                try:
                    dt = datetime.strptime(t, "%Y-%m-%d")
                    fecha_iso = dt.date().isoformat()
                    try:
                        cur.execute("INSERT INTO feriados (fecha) VALUES (?)", (fecha_iso,))
                        agregados += 1
                    except sqlite3.IntegrityError:
                        duplicados += 1
                except Exception:
                    invalidos += 1

            conn.commit()
            if agregados > 0:
                flash(f"Feriados cargados: {agregados}. Duplicados: {duplicados}. Inválidos: {invalidos}.", "success")
            else:
                flash(f"No se agregaron feriados nuevos. Duplicados: {duplicados}. Inválidos: {invalidos}.", "warning")

        return redirect(url_for("feriados"))

    filas = cur.execute("SELECT id, fecha FROM feriados ORDER BY fecha").fetchall()
    dias = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
    items = []
    for r in filas:
        try:
            dow = dias[datetime.strptime(r["fecha"], "%Y-%m-%d").weekday()]
        except Exception:
            dow = ""
        items.append({"id": r["id"], "fecha": r["fecha"], "dow": dow})

    conn.close()
    return render_template("feriados.html", feriados=items)

@app.route("/feriados/eliminar/<int:id>", methods=["POST"])
@require_admin
def feriado_eliminar(id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM feriados WHERE id=?", (id,))
    conn.commit()
    conn.close()
    flash("Feriado eliminado.", "success")
    return redirect(url_for("feriados"))

# ---------- ADMIN ----------
@app.route("/admin/recalcular_deudores")
@require_admin
def admin_recalcular_deudores():
    conn = get_db()
    ids = [r['id'] for r in conn.execute("SELECT id FROM clientes WHERE empresa_id=?", (current_empresa_id(),)).fetchall()]
    for cid in ids:
        _actualizar_flag_deudor(conn, cid)
    conn.commit()
    conn.close()
    flash("Recalculados deudores de todos los clientes.", "success")
    return redirect(url_for("clientes"))

# ---------- TAREA: Recordatorios de vacunas ----------
@app.route("/tareas/vacunas")
def tareas_vacunas():
    # seguridad simple para el Programador de tareas
    key = request.args.get("key", "")
    if key != app.config.get("TASK_SECRET", "margay-task"):
        return "Forbidden", 403

    dias    = request.args.get("dias", 10, type=int)
    preview = request.args.get("preview", 0, type=int) == 1

    hoy = date.today()
    limite = hoy + timedelta(days=dias)
    hoy_iso = hoy.isoformat()
    lim_iso = limite.isoformat()

    conn = get_db()
    cur  = conn.cursor()

    # Motivos de vacunación (exactos: vacuna / vacunación / vacunacion)
    vacuna_motivos = _motivo_ids_vacunacion(conn)  # puede ser []

    # Vacunas que vencen en los próximos N días y aún no fueron notificadas
    vacs = cur.execute(f"""
        SELECT
            v.id            AS vacuna_id,
            v.animal_id     AS animal_id,
            v.fecha_vencimiento AS fecha_vencimiento,
            an.nombre       AS animal_nombre,
            c.id            AS cliente_id,
            c.nombre        AS cliente_nombre,
            c.telefono      AS telefono
        FROM vacunas v
        JOIN animales an ON an.id = v.animal_id
        JOIN clientes c  ON c.id  = an.cliente_id
        WHERE DATE(v.fecha_vencimiento) BETWEEN DATE(?) AND DATE(?)
          AND v.id NOT IN (SELECT vacuna_id FROM vacuna_recordatorios)
        ORDER BY c.id, v.fecha_vencimiento, an.nombre
    """, (hoy_iso, lim_iso)).fetchall()

    # Agrupar por cliente, filtrando los que ya tienen turno de vacunación reservado
    por_cliente = {}   # cliente_id -> dict(info, items)
    accents_ok = lambda s: (s or "").strip()

    for v in vacs:
        # Si hay turno futuro PENDIENTE para este animal con motivo EXACTO vacuna/vacunación, no se notifica
        tiene_turno_vac = False
        if vacuna_motivos:
            q = """
                SELECT 1
                FROM agenda
                WHERE animal_id=?
                  AND motivo_id IN ({})
                  AND atendida=0
                  AND DATE(fecha) >= DATE(?)
                LIMIT 1
            """.format(",".join("?"*len(vacuna_motivos)))
            params = [v['animal_id'], *vacuna_motivos, hoy_iso]
            tiene_turno_vac = cur.execute(q, params).fetchone() is not None

        if tiene_turno_vac:
            continue  # ya agendó para vacuna: no recordamos ahora

        # Validar teléfono
        phone = _uy_to_e164_digits(v['telefono'])
        if not phone:
            continue

        cid = v['cliente_id']
        if cid not in por_cliente:
            por_cliente[cid] = {
                "cliente": accents_ok(v["cliente_nombre"]),
                "telefono": phone,
                "items": [],           # [(animal_nombre, fecha_venc_iso, vacuna_id)]
            }
        por_cliente[cid]["items"].append(
            (accents_ok(v["animal_nombre"]), accents_ok(v["fecha_vencimiento"]), v["vacuna_id"])
        )

    # Nada para enviar
    if not por_cliente:
        conn.close()
        return jsonify({"ok": True, "enviados": 0, "clientes": 0, "detalle": []})

    # Insertar registros (solo si NO preview)
    total_vacunas_marcadas = 0
    if not preview:
        ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for cid, data in por_cliente.items():
            for _, fven, vac_id in data["items"]:
                try:
                    cur.execute("""
                        INSERT INTO vacuna_recordatorios (vacuna_id, animal_id, cliente_id, fecha_vencimiento, enviado_en)
                        SELECT v.id, v.animal_id, an.cliente_id, v.fecha_vencimiento, ?
                        FROM vacunas v
                        JOIN animales an ON an.id = v.animal_id
                        WHERE v.id = ?
                    """, (ahora, vac_id))
                    total_vacunas_marcadas += 1
                except sqlite3.IntegrityError:
                    # Ya estaba registrada, ignoramos
                    pass
        conn.commit()

    # Armar URLs de WhatsApp Web por cliente
    items = []
    for cid, data in por_cliente.items():
        text = build_vacunas_text(
            data["cliente"],
            [(nm, fv) for (nm, fv, _vid) in data["items"]]
        )
        wa_url = f"https://web.whatsapp.com/send?phone={data['telefono']}&text={quote(text)}"
        resumen = ", ".join([f"{nm} ({fv})" for (nm, fv, _vid) in data["items"]])
        items.append({
            "cliente": data["cliente"],
            "telefono": "+" + data["telefono"],  # solo visual
            "url": wa_url,
            "resumen": resumen
        })

    conn.close()

    # PREVIEW => no abrir WhatsApp
    if preview:
        return jsonify({
            "ok": True,
            "preview": True,
            "clientes": len(items),
            "vacunas": sum(len(d["items"]) for d in por_cliente.values()),
            "detalle": items
        })

    # Abrir secuencialmente pestañas de WhatsApp Web (puede requerir permitir popups)
    return render_template("whatsapp.bulk.html", items=items, total=len(items))


# === Panel de Facturación (nuevo) ===
@app.route("/facturacion")
def facturacion():
    today = date.today()
    # Permite querystring ?anio=YYYY&mes=M y defaults al mes/año actual
    try:
        anio = int(request.args.get("anio", today.year))
    except Exception:
        anio = today.year
    try:
        mes = int(request.args.get("mes", today.month))
    except Exception:
        mes = today.month
    return render_template("facturacion.html", anio=anio, mes=mes)



app.register_blueprint(recordatorios_bp)

# ---------- Alias para compatibilidad de templates antiguos ----------
@app.route("/mensajes")
def mensajes_alias():
    return redirect(url_for("mensualidades"))


application = app

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
