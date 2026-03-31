
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
import sqlite3, threading, time, datetime as dt, os, smtplib, ssl
from email.message import EmailMessage

bp = Blueprint("recordatorios", __name__, url_prefix="/recordatorios")
_db_env = os.environ.get("DATABASE_PATH")
if _db_env:
    DB_PATH = _db_env
elif os.environ.get("RENDER") or os.environ.get("PORT"):
    DB_PATH = "/tmp/veterinaria.db"
else:
    DB_PATH = "veterinaria.db"

# En Render, si la base en /tmp no existe todavía, copiamos la incluida en el proyecto
if DB_PATH.startswith("/tmp/") and not os.path.exists(DB_PATH):
    _seed_db = os.path.join(os.path.dirname(__file__), "veterinaria.db")
    if os.path.exists(_seed_db):
        import shutil
        shutil.copy(_seed_db, DB_PATH)
CLINICA = "VetCloud"
BATCH_SIZE = 20
PAUSA_ENTRE_ENVIOS = 0.6
LAST_TICK = None


def db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def empresa_actual():
    return session.get('empresa_id')


def _ensure_column(cur, table, colname, coldef):
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]
    if colname not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {coldef}")

def init_tables():
    conn = db_conn(); cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reminder_config (
        empresa_id INTEGER PRIMARY KEY,
        mensual_enabled INTEGER DEFAULT 1,
        mensual_template TEXT,
        mensual_hora TEXT DEFAULT '10:00',
        mensual_dia_mes INTEGER DEFAULT 1,
        vacunas_enabled INTEGER DEFAULT 1,
        vacunas_template TEXT,
        vacunas_hora TEXT DEFAULT '10:00',
        vacunas_dias_antes INTEGER DEFAULT 7,
        despa_enabled INTEGER DEFAULT 0,
        despa_template TEXT,
        despa_hora TEXT DEFAULT '10:00',
        despa_dias_antes INTEGER DEFAULT 7,
        despa_intervalo_dias INTEGER DEFAULT 90,
        part_enabled INTEGER DEFAULT 1,
        part_template TEXT,
        part_hora TEXT DEFAULT '10:00',
        part_dia_mes INTEGER DEFAULT 5,
        smtp_host TEXT,
        smtp_port INTEGER DEFAULT 587,
        smtp_user TEXT,
        smtp_pass TEXT,
        smtp_tls INTEGER DEFAULT 1,
        smtp_ssl INTEGER DEFAULT 0,
        smtp_from TEXT,
        smtp_from_name TEXT
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reminder_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id INTEGER NOT NULL DEFAULT 1,
        tipo TEXT NOT NULL,
        cliente_id INTEGER,
        animal_id INTEGER,
        vacuna_id INTEGER,
        referencia_fecha TEXT,
        email_destino TEXT,
        asunto TEXT,
        mensaje TEXT,
        programado_en TEXT,
        estado TEXT DEFAULT 'pendiente',
        intentos INTEGER DEFAULT 0,
        last_error TEXT,
        last_try_at TEXT,
        enviado_en TEXT
    )""")

    # Migraciones suaves para bases viejas
    _ensure_column(cur, 'reminder_config', 'empresa_id', "empresa_id INTEGER DEFAULT 1")
    _ensure_column(cur, 'reminder_config', 'mensual_enabled', "mensual_enabled INTEGER DEFAULT 1")
    _ensure_column(cur, 'reminder_config', 'mensual_template', "mensual_template TEXT")
    _ensure_column(cur, 'reminder_config', 'mensual_hora', "mensual_hora TEXT DEFAULT '10:00'")
    _ensure_column(cur, 'reminder_config', 'mensual_dia_mes', "mensual_dia_mes INTEGER DEFAULT 1")
    _ensure_column(cur, 'reminder_config', 'vacunas_enabled', "vacunas_enabled INTEGER DEFAULT 1")
    _ensure_column(cur, 'reminder_config', 'vacunas_template', "vacunas_template TEXT")
    _ensure_column(cur, 'reminder_config', 'vacunas_hora', "vacunas_hora TEXT DEFAULT '10:00'")
    _ensure_column(cur, 'reminder_config', 'vacunas_dias_antes', "vacunas_dias_antes INTEGER DEFAULT 7")
    _ensure_column(cur, 'reminder_config', 'despa_enabled', "despa_enabled INTEGER DEFAULT 0")
    _ensure_column(cur, 'reminder_config', 'despa_template', "despa_template TEXT")
    _ensure_column(cur, 'reminder_config', 'despa_hora', "despa_hora TEXT DEFAULT '10:00'")
    _ensure_column(cur, 'reminder_config', 'despa_dias_antes', "despa_dias_antes INTEGER DEFAULT 7")
    _ensure_column(cur, 'reminder_config', 'despa_intervalo_dias', "despa_intervalo_dias INTEGER DEFAULT 90")
    _ensure_column(cur, 'reminder_config', 'part_enabled', "part_enabled INTEGER DEFAULT 1")
    _ensure_column(cur, 'reminder_config', 'part_template', "part_template TEXT")
    _ensure_column(cur, 'reminder_config', 'part_hora', "part_hora TEXT DEFAULT '10:00'")
    _ensure_column(cur, 'reminder_config', 'part_dia_mes', "part_dia_mes INTEGER DEFAULT 5")
    _ensure_column(cur, 'reminder_config', 'smtp_host', "smtp_host TEXT")
    _ensure_column(cur, 'reminder_config', 'smtp_port', "smtp_port INTEGER DEFAULT 587")
    _ensure_column(cur, 'reminder_config', 'smtp_user', "smtp_user TEXT")
    _ensure_column(cur, 'reminder_config', 'smtp_pass', "smtp_pass TEXT")
    _ensure_column(cur, 'reminder_config', 'smtp_tls', "smtp_tls INTEGER DEFAULT 1")
    _ensure_column(cur, 'reminder_config', 'smtp_ssl', "smtp_ssl INTEGER DEFAULT 0")
    _ensure_column(cur, 'reminder_config', 'smtp_from', "smtp_from TEXT")
    _ensure_column(cur, 'reminder_config', 'smtp_from_name', "smtp_from_name TEXT")

    _ensure_column(cur, 'reminder_queue', 'empresa_id', "empresa_id INTEGER NOT NULL DEFAULT 1")
    _ensure_column(cur, 'reminder_queue', 'tipo', "tipo TEXT")
    _ensure_column(cur, 'reminder_queue', 'cliente_id', "cliente_id INTEGER")
    _ensure_column(cur, 'reminder_queue', 'animal_id', "animal_id INTEGER")
    _ensure_column(cur, 'reminder_queue', 'vacuna_id', "vacuna_id INTEGER")
    _ensure_column(cur, 'reminder_queue', 'referencia_fecha', "referencia_fecha TEXT")
    _ensure_column(cur, 'reminder_queue', 'email_destino', "email_destino TEXT")
    _ensure_column(cur, 'reminder_queue', 'asunto', "asunto TEXT")
    _ensure_column(cur, 'reminder_queue', 'mensaje', "mensaje TEXT")
    _ensure_column(cur, 'reminder_queue', 'programado_en', "programado_en TEXT")
    _ensure_column(cur, 'reminder_queue', 'estado', "estado TEXT DEFAULT 'pendiente'")
    _ensure_column(cur, 'reminder_queue', 'intentos', "intentos INTEGER DEFAULT 0")
    _ensure_column(cur, 'reminder_queue', 'last_error', "last_error TEXT")
    _ensure_column(cur, 'reminder_queue', 'last_try_at', "last_try_at TEXT")
    _ensure_column(cur, 'reminder_queue', 'enviado_en', "enviado_en TEXT")

    cur.execute("UPDATE reminder_config SET empresa_id = 1 WHERE empresa_id IS NULL")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_reminder_config_empresa ON reminder_config(empresa_id)")
    cur.execute("UPDATE reminder_queue SET empresa_id = 1 WHERE empresa_id IS NULL")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_queue_estado_empresa ON reminder_queue(empresa_id, estado, programado_en)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_mensual ON reminder_queue(empresa_id, tipo, cliente_id, referencia_fecha)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_vacuna ON reminder_queue(empresa_id, tipo, vacuna_id, referencia_fecha)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_despa ON reminder_queue(empresa_id, tipo, animal_id, referencia_fecha)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_part ON reminder_queue(empresa_id, tipo, cliente_id, referencia_fecha)")
    conn.commit(); conn.close()


def ensure_empresa_config(empresa_id:int):
    conn = db_conn(); cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO reminder_config (empresa_id, mensual_template, vacunas_template, despa_template, part_template) VALUES (?, ?, ?, ?, ?)", (
        empresa_id,
        "Hola {CLIENTE}, te recordamos la mensualidad de {MES}/{ANIO}.",
        "Hola {CLIENTE}, estas vacunas están próximas a vencer:\n\n{LISTADO}",
        "Hola {CLIENTE}, estas desparasitaciones están próximas a vencer:\n\n{LISTADO}",
        "Hola {CLIENTE}, registramos impagos pendientes de consultas particulares."
    ))
    conn.commit(); conn.close()


def _today():
    return dt.datetime.now()


def _str2time(hhmm):
    try:
        h,m = (hhmm or '10:00').split(':',1)
        return dt.time(int(h), int(m))
    except Exception:
        return dt.time(10,0)


def _dt_on(date_obj, hhmm):
    return dt.datetime.combine(date_obj, _str2time(hhmm))


def _empresa_nombre(conn, empresa_id):
    row = conn.execute("SELECT nombre FROM empresas WHERE id=?", (empresa_id,)).fetchone()
    return row['nombre'] if row else CLINICA



def _render_placeholders(template, **vals):
    msg = template or ''
    for k, v in vals.items():
        val = '' if v is None else str(v)
        msg = msg.replace('{' + k + '}', val)
        msg = msg.replace('{' + k.upper() + '}', val)
    return msg

def _cliente_doc(conn, cliente_id):
    row = conn.execute("SELECT cedula FROM clientes WHERE id=?", (cliente_id,)).fetchone()
    return (row['cedula'] if row else '') or ''


def _enqueue(conn, empresa_id, tipo, cliente_id=None, animal_id=None, vacuna_id=None, referencia_fecha=None, email_destino=None, asunto=None, mensaje=None, programado_en=None):
    conn.execute("""
        INSERT OR IGNORE INTO reminder_queue
        (empresa_id, tipo, cliente_id, animal_id, vacuna_id, referencia_fecha, email_destino, asunto, mensaje, programado_en)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (empresa_id, tipo, cliente_id, animal_id, vacuna_id, referencia_fecha, email_destino, asunto, mensaje, programado_en))


def _smtp_send(cfg, to_email, subject, body):
    if not cfg['smtp_host'] or not cfg['smtp_from']:
        raise RuntimeError('Falta configurar SMTP y remitente.')
    msg = EmailMessage()
    from_name = (cfg['smtp_from_name'] or '').strip()
    sender = f"{from_name} <{cfg['smtp_from']}>" if from_name else cfg['smtp_from']
    msg['From'] = sender
    msg['To'] = to_email
    msg['Subject'] = subject or 'Recordatorio'
    msg.set_content(body or '')

    host = (cfg['smtp_host'] or '').strip()
    port = int(cfg['smtp_port'] or 587)
    cfg = dict(cfg) if not isinstance(cfg, dict) else cfg
    user = (cfg.get('smtp_user') or '').strip()
    password = cfg.get('smtp_pass') or ''
    use_tls = int(cfg.get('smtp_tls') or 0) == 1
    use_ssl = int(cfg.get('smtp_ssl') or 0) == 1 or port == 465

    timeout = 15
    try:
        if use_ssl:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host=host, port=port, timeout=timeout, context=context) as server:
                server.ehlo()
                if user:
                    server.login(user, password)
                server.send_message(msg)
                return
        else:
            with smtplib.SMTP(host=host, port=port, timeout=timeout) as server:
                server.ehlo()
                if use_tls:
                    server.starttls(context=ssl.create_default_context())
                    server.ehlo()
                if user:
                    server.login(user, password)
                server.send_message(msg)
                return
    except smtplib.SMTPAuthenticationError as e:
        raise RuntimeError('Autenticación SMTP rechazada. Revisá usuario y contraseña de aplicación.') from e
    except smtplib.SMTPConnectError as e:
        raise RuntimeError(f'No se pudo conectar al servidor SMTP ({host}:{port}).') from e
    except TimeoutError as e:
        raise RuntimeError(f'Timeout al conectar con {host}:{port}.') from e
    except OSError as e:
        raise RuntimeError(f'Error de red SMTP: {e}') from e
    except smtplib.SMTPException as e:
        raise RuntimeError(f'Error SMTP: {e}') from e

def _gen_mensual_auto(conn, empresa_id, today, cfg):
    if not int(cfg['mensual_enabled'] or 0):
        return
    if today.day != int(cfg['mensual_dia_mes'] or 1):
        return
    programado = _dt_on(today, cfg['mensual_hora'] or '10:00').strftime('%Y-%m-%d %H:%M')
    referencia = f"{today.year}-{today.month:02d}"
    clinica = _empresa_nombre(conn, empresa_id)
    rows = conn.execute("SELECT id, nombre, email FROM clientes WHERE empresa_id=? AND tipo='Mensual' AND COALESCE(activo,1)=1 AND COALESCE(email,'')<>''", (empresa_id,)).fetchall()
    tpl = (cfg['mensual_template'] or "Hola {CLIENTE}, te recordamos la mensualidad de {MES}/{ANIO}.")
    for r in rows:
        asunto = f"{clinica} - Recordatorio de mensualidad"
        msg = _render_placeholders(
            tpl,
            cliente=r['nombre'],
            mes=f"{today.month:02d}",
            anio=str(today.year),
            empresa=clinica,
        )
        _enqueue(conn, empresa_id, 'mensual', cliente_id=r['id'], referencia_fecha=referencia, email_destino=r['email'], asunto=asunto, mensaje=msg, programado_en=programado)


def _gen_vacunas_auto(conn, empresa_id, today, cfg):
    if not int(cfg['vacunas_enabled'] or 0):
        return
    dias = int(cfg['vacunas_dias_antes'] or 7)
    objetivo = today + dt.timedelta(days=dias)
    programado = _dt_on(today, cfg['vacunas_hora'] or '10:00').strftime('%Y-%m-%d %H:%M')
    clinica = _empresa_nombre(conn, empresa_id)
    filas = conn.execute("""
        SELECT v.id AS vacuna_id, v.fecha_vencimiento, a.id AS animal_id, a.nombre AS animal_nombre,
               c.id AS cliente_id, c.nombre AS cliente_nombre, c.email
        FROM vacunas v
        JOIN animales a ON a.id=v.animal_id
        JOIN clientes c ON c.id=a.cliente_id
        WHERE v.empresa_id=? AND c.empresa_id=? AND DATE(v.fecha_vencimiento)=DATE(?) AND COALESCE(c.email,'')<>''
        ORDER BY c.nombre, a.nombre
    """, (empresa_id, empresa_id, objetivo.strftime('%Y-%m-%d'))).fetchall()
    tpl = cfg['vacunas_template'] or "Hola {CLIENTE}, estas vacunas están próximas a vencer:\n\n{LISTADO}"
    for r in filas:
        referencia = r['fecha_vencimiento']
        asunto = f"{clinica} - Vacunas próximas a vencer"
        fecha_vto = r['fecha_vencimiento']
        listado = f"- {r['animal_nombre']}: vence {fecha_vto}"
        msg = _render_placeholders(
            tpl,
            cliente=r['cliente_nombre'],
            animal=r['animal_nombre'],
            fecha=fecha_vto,
            listado=listado,
            empresa=clinica,
            tipo='vacuna',
        )
        _enqueue(conn, empresa_id, 'vacuna', cliente_id=r['cliente_id'], animal_id=r['animal_id'], vacuna_id=r['vacuna_id'], referencia_fecha=referencia, email_destino=r['email'], asunto=asunto, mensaje=msg, programado_en=programado)


def _gen_despa_auto(conn, empresa_id, today, cfg):
    if not int(cfg['despa_enabled'] or 0):
        return
    dias = int(cfg['despa_dias_antes'] or 7)
    intervalo = int(cfg['despa_intervalo_dias'] or 90)
    objetivo = today + dt.timedelta(days=dias)
    programado = _dt_on(today, cfg['despa_hora'] or '10:00').strftime('%Y-%m-%d %H:%M')
    clinica = _empresa_nombre(conn, empresa_id)
    filas = conn.execute("""
        SELECT a.id AS animal_id, a.nombre AS animal_nombre, a.ultima_desparasitacion,
               c.id AS cliente_id, c.nombre AS cliente_nombre, c.email
        FROM animales a
        JOIN clientes c ON c.id=a.cliente_id
        WHERE a.empresa_id=? AND c.empresa_id=? AND COALESCE(a.ultima_desparasitacion,'')<>'' AND COALESCE(c.email,'')<>''
        ORDER BY c.nombre, a.nombre
    """, (empresa_id, empresa_id)).fetchall()
    tpl = cfg['despa_template'] or "Hola {CLIENTE}, estas desparasitaciones están próximas a vencer:\n\n{LISTADO}"
    for r in filas:
        try:
            ultima = dt.datetime.strptime(r['ultima_desparasitacion'][:10], '%Y-%m-%d').date()
        except Exception:
            continue
        vence = ultima + dt.timedelta(days=intervalo)
        if vence != objetivo:
            continue
        referencia = vence.strftime('%Y-%m-%d')
        asunto = f"{clinica} - Desparasitación próxima a vencer"
        listado = f"- {r['animal_nombre']}: vence {referencia}"
        msg = _render_placeholders(
            tpl,
            cliente=r['cliente_nombre'],
            animal=r['animal_nombre'],
            fecha=referencia,
            listado=listado,
            empresa=clinica,
            tipo='desparasitación',
        )
        _enqueue(conn, empresa_id, 'desparasitacion', cliente_id=r['cliente_id'], animal_id=r['animal_id'], referencia_fecha=referencia, email_destino=r['email'], asunto=asunto, mensaje=msg, programado_en=programado)


def _gen_part_impagos_auto(conn, empresa_id, today, cfg):
    if not int(cfg['part_enabled'] or 0):
        return
    if today.day != int(cfg['part_dia_mes'] or 5):
        return
    programado = _dt_on(today, cfg['part_hora'] or '10:00').strftime('%Y-%m-%d %H:%M')
    referencia = f"{today.year}-{today.month:02d}"
    clinica = _empresa_nombre(conn, empresa_id)
    mes_ini = today.replace(day=1).strftime('%Y-%m-%d')
    mes_fin = ((today.replace(day=28) + dt.timedelta(days=10)).replace(day=1) - dt.timedelta(days=1)).strftime('%Y-%m-%d')
    filas = conn.execute("""
        SELECT c.id, c.nombre, c.email,
               COALESCE(SUM(COALESCE(a.precio,0)),0) AS total,
               COALESCE(SUM(CASE WHEN a.estado_pago='Pagado' THEN COALESCE(a.precio,0) ELSE 0 END),0) AS cobrado
        FROM clientes c
        LEFT JOIN agenda a ON a.cliente_id=c.id AND a.empresa_id=c.empresa_id AND a.fecha BETWEEN ? AND ?
        WHERE c.empresa_id=? AND c.tipo='Particular' AND COALESCE(c.email,'')<>''
        GROUP BY c.id
        HAVING (total - cobrado) > 0
        ORDER BY c.nombre
    """, (mes_ini, mes_fin, empresa_id)).fetchall()
    tpl = cfg['part_template'] or "Hola {CLIENTE}, registramos impagos pendientes de consultas particulares."
    for r in filas:
        asunto = f"{clinica} - Recordatorio de impagos"
        msg = _render_placeholders(
            tpl,
            cliente=r['nombre'],
            empresa=clinica,
        )
        _enqueue(conn, empresa_id, 'particular', cliente_id=r['id'], referencia_fecha=referencia, email_destino=r['email'], asunto=asunto, mensaje=msg, programado_en=programado)


def _auto_generate_tasks_if_needed():
    conn = db_conn()
    today = _today().date()
    empresas = conn.execute('SELECT id FROM empresas WHERE COALESCE(activa,1)=1').fetchall()
    for e in empresas:
        empresa_id = e['id']
        ensure_empresa_config(empresa_id)
        cfg = conn.execute('SELECT * FROM reminder_config WHERE empresa_id=?', (empresa_id,)).fetchone()
        if not cfg:
            continue
        _gen_mensual_auto(conn, empresa_id, today, cfg)
        _gen_vacunas_auto(conn, empresa_id, today, cfg)
        _gen_despa_auto(conn, empresa_id, today, cfg)
        _gen_part_impagos_auto(conn, empresa_id, today, cfg)
    conn.commit(); conn.close()


def _process_pending_batch(force_empresa_id=None, ignore_schedule=False):
    conn = db_conn(); cur = conn.cursor()
    where = ["q.estado='pendiente'"]
    params = []
    if not ignore_schedule:
        where.append("DATETIME(q.programado_en) <= DATETIME('now')")
    if force_empresa_id is not None:
        where.append("q.empresa_id=?")
        params.append(force_empresa_id)
    sql = f"""
        SELECT q.*, rc.smtp_host, rc.smtp_port, rc.smtp_user, rc.smtp_pass, rc.smtp_tls, rc.smtp_ssl, rc.smtp_from, rc.smtp_from_name
        FROM reminder_queue q
        LEFT JOIN reminder_config rc ON rc.empresa_id=q.empresa_id
        WHERE {' AND '.join(where)}
        ORDER BY q.programado_en ASC, q.id ASC
        LIMIT ?
    """
    params.append(BATCH_SIZE)
    rows = cur.execute(sql, tuple(params)).fetchall()
    for r in rows:
        try:
            cur.execute("UPDATE reminder_queue SET estado='enviando', last_try_at=datetime('now') WHERE id=?", (r['id'],))
            conn.commit()
            _smtp_send(r, r['email_destino'], r['asunto'], r['mensaje'])
            cur.execute("UPDATE reminder_queue SET estado='enviado', enviado_en=datetime('now'), last_error=NULL WHERE id=?", (r['id'],))
            conn.commit()
            time.sleep(PAUSA_ENTRE_ENVIOS)
        except Exception as e:
            cur.execute("UPDATE reminder_queue SET estado='error', intentos=intentos+1, last_error=? WHERE id=?", (str(e), r['id']))
            conn.commit()
    conn.close()


def _scheduler_loop():
    global LAST_TICK
    while True:
        try:
            _auto_generate_tasks_if_needed()
            _process_pending_batch()
            LAST_TICK = dt.datetime.now()
        except Exception:
            pass
        time.sleep(30)


def start_scheduler():
    t = threading.Thread(target=_scheduler_loop, daemon=True)
    t.start()


init_tables()
start_scheduler()

@bp.after_request
def _no_cache(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


@bp.route('/diag')
def diag():
    conn = db_conn()
    emp = empresa_actual() or 0
    pend = conn.execute("SELECT COUNT(*) c FROM reminder_queue WHERE empresa_id=? AND estado='pendiente'", (emp,)).fetchone()['c'] if emp else 0
    err = conn.execute("SELECT COUNT(*) c FROM reminder_queue WHERE empresa_id=? AND estado='error'", (emp,)).fetchone()['c'] if emp else 0
    env = conn.execute("SELECT COUNT(*) c FROM reminder_queue WHERE empresa_id=? AND estado='enviado'", (emp,)).fetchone()['c'] if emp else 0
    conn.close()
    return jsonify({"empresa_id": emp, "last_tick": LAST_TICK.strftime('%Y-%m-%d %H:%M:%S') if LAST_TICK else None, "pendientes": pend, "errores": err, "enviados": env, "db": DB_PATH})


@bp.route('/')
def dashboard():
    emp = empresa_actual()
    if not emp:
        return redirect(url_for('login'))
    ensure_empresa_config(emp)
    conn = db_conn()
    cfg = conn.execute('SELECT * FROM reminder_config WHERE empresa_id=?', (emp,)).fetchone()
    pendientes = conn.execute("""
        SELECT q.*, c.nombre AS cliente_nombre
        FROM reminder_queue q
        LEFT JOIN clientes c ON c.id=q.cliente_id
        WHERE q.empresa_id=? AND q.estado IN ('pendiente','enviando','error')
        ORDER BY q.programado_en ASC, q.id ASC
    """, (emp,)).fetchall()
    enviados = conn.execute("""
        SELECT q.*, c.nombre AS cliente_nombre
        FROM reminder_queue q
        LEFT JOIN clientes c ON c.id=q.cliente_id
        WHERE q.empresa_id=? AND q.estado='enviado'
        ORDER BY q.enviado_en DESC, q.id DESC LIMIT 50
    """, (emp,)).fetchall()
    pend = [dict(r) | {'cliente_doc': _cliente_doc(conn, r['cliente_id']) if r['cliente_id'] else ''} for r in pendientes]
    env = [dict(r) | {'cliente_doc': _cliente_doc(conn, r['cliente_id']) if r['cliente_id'] else ''} for r in enviados]
    conn.close()
    return render_template('recordatorios.html', cfg=cfg, pendientes=pend, enviados=env)


@bp.route('/config', methods=['POST'])
def config():
    emp = empresa_actual()
    if not emp:
        return redirect(url_for('login'))
    ensure_empresa_config(emp)
    f = request.form
    values = (
        1 if f.get('mensual_enabled') else 0,
        (f.get('mensual_template') or '').strip(),
        (f.get('mensual_hora') or '10:00').strip(),
        int(f.get('mensual_dia_mes') or 1),
        1 if f.get('vacunas_enabled') else 0,
        (f.get('vacunas_template') or '').strip(),
        (f.get('vacunas_hora') or '10:00').strip(),
        int(f.get('vacunas_dias_antes') or 7),
        1 if f.get('despa_enabled') else 0,
        (f.get('despa_template') or '').strip(),
        (f.get('despa_hora') or '10:00').strip(),
        int(f.get('despa_dias_antes') or 7),
        int(f.get('despa_intervalo_dias') or 90),
        1 if f.get('part_enabled') else 0,
        (f.get('part_template') or '').strip(),
        (f.get('part_hora') or '10:00').strip(),
        int(f.get('part_dia_mes') or 5),
        (f.get('smtp_host') or '').strip(),
        int(f.get('smtp_port') or 587),
        (f.get('smtp_user') or '').strip(),
        (f.get('smtp_pass') or '').strip(),
        1 if f.get('smtp_tls') else 0,
        1 if f.get('smtp_ssl') else 0,
        (f.get('smtp_from') or '').strip(),
        (f.get('smtp_from_name') or '').strip(),
        emp
    )
    conn = db_conn(); conn.execute("""
        UPDATE reminder_config SET
            mensual_enabled=?, mensual_template=?, mensual_hora=?, mensual_dia_mes=?,
            vacunas_enabled=?, vacunas_template=?, vacunas_hora=?, vacunas_dias_antes=?,
            despa_enabled=?, despa_template=?, despa_hora=?, despa_dias_antes=?, despa_intervalo_dias=?,
            part_enabled=?, part_template=?, part_hora=?, part_dia_mes=?,
            smtp_host=?, smtp_port=?, smtp_user=?, smtp_pass=?, smtp_tls=?, smtp_ssl=?, smtp_from=?, smtp_from_name=?
        WHERE empresa_id=?
    """, values)
    conn.commit(); conn.close()
    flash('Configuración guardada.', 'success')
    return redirect(url_for('recordatorios.dashboard'))


@bp.route('/enviar_pendientes')
def enviar_pendientes():
    emp = empresa_actual()
    _auto_generate_tasks_if_needed()
    _process_pending_batch(force_empresa_id=emp, ignore_schedule=True)
    flash('Procesamiento manual ejecutado. Se intentaron enviar los pendientes de esta veterinaria.', 'info')
    return redirect(url_for('recordatorios.dashboard'))


@bp.route('/smtp_test', methods=['POST'])
def smtp_test():
    emp = empresa_actual()
    if not emp:
        return redirect(url_for('login'))
    f = request.form
    cfg = {
        'smtp_host': (f.get('smtp_host') or '').strip(),
        'smtp_port': int(f.get('smtp_port') or 587),
        'smtp_user': (f.get('smtp_user') or '').strip(),
        'smtp_pass': (f.get('smtp_pass') or '').strip(),
        'smtp_tls': 1 if f.get('smtp_tls') else 0,
        'smtp_ssl': 1 if f.get('smtp_ssl') else 0,
        'smtp_from': (f.get('smtp_from') or '').strip(),
        'smtp_from_name': (f.get('smtp_from_name') or '').strip(),
    }
    test_email = (f.get('test_email') or '').strip() or cfg['smtp_from']
    if not test_email:
        flash('Ingresá un email de prueba o un remitente válido.', 'danger')
        return redirect(url_for('recordatorios.dashboard'))
    try:
        _smtp_send(cfg, test_email, 'Prueba SMTP VetCloud', 'Esta es una prueba de configuración SMTP desde VetCloud.')
        flash(f'Prueba SMTP exitosa. Se envió un correo a {test_email}.', 'success')
    except Exception as e:
        flash(f'Error SMTP: {e}', 'danger')
    return redirect(url_for('recordatorios.dashboard'))


@bp.route('/cola/reintentar/<int:id>', methods=['POST'])
def reintentar(id):
    emp = empresa_actual(); conn = db_conn(); conn.execute("UPDATE reminder_queue SET estado='pendiente', last_error=NULL WHERE id=? AND empresa_id=?", (id, emp)); conn.commit(); conn.close(); flash('Reintento marcado.', 'info'); return redirect(url_for('recordatorios.dashboard'))


@bp.route('/cola/cancelar/<int:id>', methods=['POST'])
def cancelar(id):
    emp = empresa_actual(); conn = db_conn(); conn.execute("UPDATE reminder_queue SET estado='cancelado', last_error=NULL WHERE id=? AND empresa_id=?", (id, emp)); conn.commit(); conn.close(); flash('Envío cancelado.', 'info'); return redirect(url_for('recordatorios.dashboard'))


@bp.route('/cola/eliminar/<int:id>', methods=['POST'])
def eliminar(id):
    emp = empresa_actual(); conn = db_conn(); conn.execute("DELETE FROM reminder_queue WHERE id=? AND empresa_id=?", (id, emp)); conn.commit(); conn.close(); flash('Envío eliminado.', 'warning'); return redirect(url_for('recordatorios.dashboard'))


@bp.route('/cola/reprogramar/<int:id>', methods=['POST'])
def reprogramar(id):
    emp = empresa_actual(); fecha = request.form.get('fecha'); hora = request.form.get('hora')
    if not fecha or not hora:
        flash('Fecha u hora inválidas.', 'danger')
        return redirect(url_for('recordatorios.dashboard'))
    conn = db_conn(); conn.execute("UPDATE reminder_queue SET programado_en=? WHERE id=? AND empresa_id=?", (f"{fecha} {hora}", id, emp)); conn.commit(); conn.close(); flash('Reprogramado.', 'success'); return redirect(url_for('recordatorios.dashboard'))


@bp.route('/nuevo', methods=['POST'])
def nuevo():
    emp = empresa_actual(); email = (request.form.get('email') or '').strip(); msg = (request.form.get('mensaje') or '').strip(); fecha = request.form.get('fecha'); hora = request.form.get('hora'); asunto = (request.form.get('asunto') or 'Recordatorio manual').strip()
    if not (emp and email and msg and fecha and hora):
        flash('Completá email, asunto, mensaje, fecha y hora.', 'danger')
        return redirect(url_for('recordatorios.dashboard'))
    conn = db_conn(); conn.execute("INSERT INTO reminder_queue (empresa_id, tipo, email_destino, asunto, mensaje, programado_en) VALUES (?, 'manual', ?, ?, ?, ?)", (emp, email, asunto, msg, f"{fecha} {hora}")); conn.commit(); conn.close(); flash('Programado.', 'success'); return redirect(url_for('recordatorios.dashboard'))
