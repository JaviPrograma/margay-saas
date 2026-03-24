# recordatorios.py — Usa la MISMA ventana de Chrome ya abierta (debugger 9222) y envía en WhatsApp Web
# Requisitos:
#  - Abrir Chrome con:  --remote-debugging-port=9222  (mismo perfil que usás)
#  - Tener WhatsApp Web abierto/logueado en esa ventana
#  - pip install selenium webdriver-manager
#
# Características:
#  - NO abre otro Chrome: se adjunta a tu ventana (STRICT_ATTACH_ONLY=True).
#  - Busca una pestaña con WhatsApp Web; si no hay, abre una nueva PESTAÑA en esa MISMA ventana.
#  - Escribe, envía (botón/ENTER) y verifica la burbuja.
#  - Scheduler interno (cada 30s), reintentos, reprogramar/cancelar/eliminar, “Procesar ahora”.
#  - Endpoints consistentes con el template (recordatorios.config, etc.).

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
import sqlite3, threading, time, datetime as dt, re, os
from urllib.parse import quote

bp = Blueprint("recorditorios_fix_name", __name__, url_prefix="/recordatorios")
# ¡OJO!: Si tu app ya lo importa como `from recordatorios import bp as recordatorios_bp`,
#        dejá el nombre del blueprint como "recordatorios".
bp.name = "recordatorios"  # fuerza el nombre esperado por app.py / templates

# ---------- Config ----------
DB_PATH = "veterinaria.db"
CLINICA = "MARGAY"

BATCH_SIZE = 15
PAUSA_ENTRE_ENVIOS = 1.2
RETRY_DELAY_MIN = 3
MAX_RETRIES = 6

# Conectarse al Chrome YA ABIERTO (no crear otro):
DEBUGGER_ADDRESS = os.environ.get("WA_DEBUG_ADDR", "127.0.0.1:9222")
STRICT_ATTACH_ONLY = True  # si no puede adjuntar, no manda (para NO abrir ventana nueva)

# ---------- Utilidades DB ----------
def db_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)

def dict_row(conn):
    conn.row_factory = sqlite3.Row
    return conn

def init_tables():
    conn = dict_row(db_conn()); cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS reminder_config (
        id INTEGER PRIMARY KEY CHECK(id=1),
        mensual_enabled INTEGER DEFAULT 1,
        mensual_template TEXT,
        mensual_hora TEXT DEFAULT '10:00',
        mensual_dia_mes INTEGER DEFAULT 1,
        vacunas_enabled INTEGER DEFAULT 1,
        vacunas_template TEXT,
        vacunas_hora TEXT DEFAULT '10:00',
        vacunas_dias_antes INTEGER DEFAULT 7,
        part_enabled INTEGER DEFAULT 1,
        part_template TEXT,
        part_hora TEXT DEFAULT '10:00',
        part_dia_mes INTEGER DEFAULT 5
    )""")
    cur.execute("INSERT OR IGNORE INTO reminder_config(id) VALUES(1)")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS reminder_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo TEXT NOT NULL,                 -- mensual|vacuna|particular|manual
        cliente_id INTEGER,
        animal_id INTEGER,
        vacuna_id INTEGER,
        telefono TEXT,
        mensaje TEXT,
        programado_en TEXT,                 -- "YYYY-MM-DD HH:MM"
        estado TEXT DEFAULT 'pendiente',    -- pendiente|enviando|enviado|cancelado|error
        intentos INTEGER DEFAULT 0,
        last_error TEXT,
        last_try_at TEXT,
        enviado_en TEXT
    )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_queue_estado ON reminder_queue(estado)")
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_queue_mensual ON reminder_queue (tipo, cliente_id, programado_en)")
    except Exception:
        pass
    conn.commit(); conn.close()

init_tables()

# ---------- Helpers ----------
def _uy_to_digits(tel):
    tel = re.sub(r"\\D+", "", (str(tel or "")).strip())
    if tel.startswith("598"):
        pass
    elif tel.startswith("0"):
        tel = "598" + tel[1:]
    elif tel.startswith("+598"):
        tel = tel[1:]
    elif tel.startswith("9"):
        tel = "598" + tel
    return tel if len(tel) >= 11 else None

def _hoy(): return dt.datetime.now()

def _str2time(hhmm: str):
    s = str(hhmm or '').strip()
    try:
        import re as _re
        m = _re.match(r'^(\d{1,2})(?::|_|\.|h)?(\d{2})$', s)
        if m:
            h = int(m.group(1)); mnt = int(m.group(2))
            if 0 <= h < 24 and 0 <= mnt < 60:
                return dt.time(h, mnt)
        if ":" in s:
            h, mnt = s.split(":", 1)
            return dt.time(int(h), int(mnt))
    except Exception:
        pass
    return dt.time(10,0)
def _dt_from_date_and_hhmm(date_, hhmm: str): return dt.datetime.combine(date_, _str2time(hhmm))
def _short(t, n=220): t=(t or "").strip(); return t if len(t)<=n else t[:n]+"…"

def _get_cliente_info(cliente_id):
    if not cliente_id: return {"nombre":"-","doc":"-"}
    conn = dict_row(db_conn())
    # Leer columnas existentes de clientes para no romper con nombres ausentes
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(clientes)")]
    name_col = "nombre" if "nombre" in cols else None
    # documento puede ser 'cedula' o 'documento' (no usamos 'ci' para evitar errores)
    doc_col  = "cedula" if "cedula" in cols else ("documento" if "documento" in cols else None)
    sel_name = name_col if name_col else "'-'"
    sel_doc  = doc_col  if doc_col  else "''"
    r = conn.execute(f"SELECT {sel_name} AS nombre, {sel_doc} AS doc, telefono FROM clientes WHERE id=?", (cliente_id,)).fetchone()
    conn.close()
    return {"nombre": r["nombre"] if r else "-", "doc": r["doc"] if r else "-", "telefono": r["telefono"] if r else None}

# === NUEVO helper para armar cliente_doc sin asumir columnas ===
def _add_cliente_doc(rows):
    out = []
    for r in rows:
        info = _get_cliente_info(r["cliente_id"])
        d = dict(r)
        d["cliente_doc"] = info.get("doc", "") or ""
        out.append(d)
    return out

# ---------- Selenium: ADJUNTAR a Chrome ya abierto ----------
USE_SELENIUM = True
_sender_lock = threading.Lock()

class _WhatsSender:
    def __init__(self):
        self._driver = None
        self.By = None
        self.EC = None
        self.WebDriverWait = None
        self.attach_mode = False
        self.attach_error = None
        self.wa_handle = None

    def _ensure_driver(self):
        if self._driver:
            return
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            self.By = By; self.EC = EC; self.WebDriverWait = WebDriverWait

            # Adjuntar al Chrome abierto por remote debugging
            opts = webdriver.ChromeOptions()
            opts.debugger_address = DEBUGGER_ADDRESS

            self._driver = webdriver.Chrome(options=opts)
            self.attach_mode = True
            self.attach_error = None

            # Buscar pestaña de WA; si no existe, abrir una en esta MISMA ventana
            handles = self._driver.window_handles
            found = None
            for h in handles:
                self._driver.switch_to.window(h)
                if "web.whatsapp.com" in (self._driver.current_url or ""):
                    found = h; break
            if not found:
                self._driver.execute_script("window.open('https://web.whatsapp.com','_blank');")
                time.sleep(0.5)
                self._driver.switch_to.window(self._driver.window_handles[-1])
            self.wa_handle = self._driver.current_window_handle

        except Exception as e:
            self.attach_mode = False
            self.attach_error = str(e)
            if STRICT_ATTACH_ONLY:
                raise
            else:
                # fallback: abrir su propio Chrome (no recomendado si querés 1 sola ventana)
                from selenium import webdriver
                self._driver = webdriver.Chrome()
                self._driver.get("https://web.whatsapp.com")
                self.wa_handle = self._driver.current_window_handle

    def _normalize(self, s): return re.sub(r"\\s+", " ", (s or "").strip())

    def _find_editor(self, timeout=35):
        W = self.WebDriverWait; By = self.By; EC = self.EC
        # Asegurar que el panel principal del chat esté
        W(self._driver, timeout).until(EC.presence_of_element_located((By.ID, "main")))
        # Selectores del editor SOLO en el footer del chat (evita barra de búsqueda)
        candidates = [
            (By.CSS_SELECTOR, "#main footer div[contenteditable='true'][data-lexical-editor='true']:not([aria-hidden='true'])"),
            (By.CSS_SELECTOR, "#main footer div[contenteditable='true'][role='textbox']:not([aria-hidden='true'])"),
            (By.CSS_SELECTOR, "#main footer [data-testid='conversation-compose-box-input']"),
            (By.XPATH, "//div[@id='main']//footer//div[@contenteditable='true' and not(@aria-hidden='true')]"),
        ]
        last = None
        for how, sel in candidates:
            try:
                el = W(self._driver, 10).until(EC.presence_of_element_located((how, sel)))
                W(self._driver, 10).until(EC.element_to_be_clickable((how, sel)))
                return el
            except Exception as e:
                last = e
        raise last or Exception("No se encontró el editor del chat en el footer")

    def _click_send_button(self, timeout=10) -> bool:
        By = self.By; EC = self.EC; W = self.WebDriverWait
        selectors = [
            (By.CSS_SELECTOR, "button[data-testid='compose-btn-send']"),
            (By.CSS_SELECTOR, "#main footer button[data-testid='compose-btn-send']"),
            (By.CSS_SELECTOR, "button[aria-label='Enviar']"),
            (By.CSS_SELECTOR, "button[aria-label='Send']"),
            (By.XPATH, "//div[@id='main']//footer//button[@aria-label='Enviar' or @aria-label='Send']"),
            (By.CSS_SELECTOR, "#main footer span[data-icon='send']"),
        ]
        for how, sel in selectors:
            try:
                btn = W(self._driver, timeout).until(EC.element_to_be_clickable((how, sel)))
                try:
                    self._driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                except Exception:
                    pass
                try:
                    self._driver.execute_script("arguments[0].click();", btn)
                except Exception:
                    btn.click()
                return True
            except Exception:
                continue
        return False

    def send_and_verify(self, phone_digits: str, text: str) -> bool:
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.common.action_chains import ActionChains

        with _sender_lock:
            self._ensure_driver()
            self._driver.switch_to.window(self.wa_handle)

            # Abrir chat con el TEXTO pre-rellenado
            prefill = (text or "").strip()
            url = f"https://web.whatsapp.com/send/?phone={phone_digits}&text={quote(prefill)}&type=phone_number&app_absent=0"
            self._driver.get(url)

            # Esperar panel principal y sacar foco de búsqueda
            try:
                main = self.WebDriverWait(self._driver, 35).until(
                    self.EC.presence_of_element_located((self.By.ID, "main"))
                )
                try:
                    main.click()
                except Exception:
                    pass
            except Exception:
                return False

            # Editor del footer
            try:
                editor = self._find_editor(timeout=35)
            except Exception:
                return False

            # Asegurar foco y revisar si el prefill está
            try:
                self._driver.execute_script("arguments[0].focus();", editor)
            except Exception:
                pass

            try:
                current_txt = editor.text or ""
            except Exception:
                current_txt = ""

            if self._normalize(current_txt) != self._normalize(prefill):
                # Limpiar y escribir manual
                try:
                    ActionChains(self._driver)\
                        .key_down(Keys.CONTROL).send_keys("a").key_up(Keys.CONTROL)\
                        .send_keys(Keys.DELETE).perform()
                except Exception:
                    pass
                lines = (prefill or "").split("\\n")
                for i, part in enumerate(lines):
                    if part:
                        editor.send_keys(part)
                    if i < len(lines) - 1:
                        editor.send_keys(Keys.SHIFT, Keys.ENTER)

            # Enviar
            sent = self._click_send_button()
            if not sent:
                try:
                    editor.send_keys(Keys.ENTER)
                    sent = True
                except Exception:
                    sent = False
            if not sent:
                return False

            # Verificar burbuja saliente
            target = self._normalize(prefill)
            t0 = time.time()
            while time.time() - t0 < 25:
                bubbles = []
                try:
                    bubbles += self._driver.find_elements(self.By.CSS_SELECTOR, "#main div.message-out")
                except Exception:
                    pass
                try:
                    bubbles += self._driver.find_elements(self.By.CSS_SELECTOR, "#main div[data-testid='msg-container'] div.message-out")
                except Exception:
                    pass

                if bubbles:
                    last = bubbles[-1]
                    try:
                        spans = last.find_elements(
                            self.By.CSS_SELECTOR,
                            "span[dir='ltr'], span[dir='auto'], span.selectable-text, div[dir='auto']"
                        )
                        bubble_txt = self._normalize(" ".join(s.text for s in spans))
                        if bubble_txt.endswith(target) or bubble_txt == target or target.endswith(bubble_txt):
                            return True
                    except Exception:
                        pass
                time.sleep(0.5)
            return False

# Inicializar sender
try:
    SENDER = _WhatsSender() if USE_SELENIUM else None
except Exception:
    SENDER = None
    USE_SELENIUM = False

# ---------- Autogeneración (según tu DB) ----------
def _get_cfg():
    conn = dict_row(db_conn())
    r = conn.execute("SELECT * FROM reminder_config WHERE id=1").fetchone()
    conn.close()
    return r

def _gen_mensual_auto(today, cfg):
    if not cfg["mensual_enabled"]: return
    if today.day != (cfg["mensual_dia_mes"] or 1): return
    hhmm = cfg["mensual_hora"] or "10:00"
    programado = _dt_from_date_and_hhmm(today, hhmm).strftime("%Y-%m-%d %H:%M")
    conn = dict_row(db_conn()); cur = conn.cursor()
    try:
        rows = cur.execute("SELECT id, nombre, telefono FROM clientes WHERE tipo='Mensual' AND COALESCE(activo,1)=1").fetchall()
    except Exception:
        rows = []
    tpl = (cfg["mensual_template"] or f"Hola {{CLIENTE}}, te escribe {CLINICA} 🐾. Recordatorio de tu mensualidad de {{MES}}/{{ANIO}}. ¡Gracias!")
    mes = today.month; anio = today.year
    for r in rows:
        phone = _uy_to_digits(r["telefono"])
        if not phone: continue
        msg = tpl.replace("{CLIENTE}", r["nombre"]).replace("{MES}", f"{mes:02d}").replace("{ANIO}", f"{anio}")
        try:
            cur.execute("""
    INSERT INTO reminder_queue (tipo, cliente_id, telefono, mensaje, programado_en)
    SELECT 'mensual', ?, ?, ?, ?
    WHERE NOT EXISTS (
      SELECT 1 FROM reminder_queue
      WHERE tipo='mensual' AND cliente_id=? AND programado_en=?
    )
""", (r["id"], phone, msg, programado, r["id"], programado))
        except Exception:
            pass
    conn.commit(); conn.close()

def _gen_vacunas_auto(today, cfg):
    if not cfg["vacunas_enabled"]: return
    hhmm = cfg["vacunas_hora"] or "10:00"
    dias = int(cfg["vacunas_dias_antes"] or 7)
    objetivo = today + dt.timedelta(days=dias)
    programado = _dt_from_date_and_hhmm(today, hhmm).strftime("%Y-%m-%d %H:%M")

    conn = dict_row(db_conn()); cur = conn.cursor()
    try:
        filas = cur.execute("""
            SELECT v.id AS vacuna_id, a.id AS animal_id, a.nombre AS animal, c.id AS cliente_id, c.nombre AS cliente, c.telefono AS tel, v.fecha_vencimiento AS vence
            FROM vacunas v
            JOIN animales a ON a.id=v.animal_id
            JOIN clientes c ON c.id=a.cliente_id
            WHERE DATE(v.fecha_vencimiento)=DATE(?)
            ORDER BY c.nombre COLLATE NOCASE, a.nombre COLLATE NOCASE
        """, (objetivo.strftime("%Y-%m-%d"),)).fetchall()
    except Exception:
        filas = []

    tpl_default = "Hola {CLIENTE}, te escribe " + CLINICA + " 🐾. {LISTADO}"
    tpl = (cfg["vacunas_template"] or tpl_default)

    for r in filas:
        phone = _uy_to_digits(r["tel"])
        if not phone:
            continue
        listado = f"La vacuna de {r['animal']} vence el {r['vence']}."
        msg = tpl.replace("{CLIENTE}", r["cliente"]).replace("{LISTADO}", listado)
        try:
            cur.execute("""
                INSERT INTO reminder_queue
                (tipo, cliente_id, animal_id, vacuna_id, telefono, mensaje, programado_en)
                VALUES ('vacuna', ?, ?, ?, ?, ?, ?)
            """, (r["cliente_id"], r["animal_id"], r["vacuna_id"], phone, msg, programado))
        except Exception:
            pass
    conn.commit(); conn.close()

def _gen_part_impagos_auto(today, cfg):
    if not cfg["part_enabled"]: return
    if today.day != (cfg["part_dia_mes"] or 5): return
    hhmm = cfg["part_hora"] or "10:00"
    programado = _dt_from_date_and_hhmm(today, hhmm).strftime("%Y-%m-%d %H:%M")

    mes_ini = today.replace(day=1).strftime("%Y-%m-%d")
    mes_fin = (today.replace(day=28) + dt.timedelta(days=10)).replace(day=1) - dt.timedelta(days=1)
    mes_fin = mes_fin.strftime("%Y-%m-%d")

    conn = dict_row(db_conn()); cur = conn.cursor()
    try:
        filas = cur.execute("""
        SELECT c.id, c.nombre, c.telefono,
               COALESCE(SUM(COALESCE(a.precio,0)),0) AS total,
               COALESCE(SUM(CASE WHEN a.estado_pago='Pagado' THEN COALESCE(a.precio,0) ELSE 0 END),0) AS cobrado
        FROM clientes c
        LEFT JOIN agenda a
           ON a.cliente_id=c.id AND a.fecha BETWEEN ? AND ?
        WHERE c.tipo='Particular'
        GROUP BY c.id
        HAVING (total - cobrado) > 0
        ORDER BY c.nombre COLLATE NOCASE
    """, (mes_ini, mes_fin)).fetchall()
    except Exception:
        filas = []

    tpl = (cfg["part_template"] or f"Hola {{CLIENTE}}, te escribe {CLINICA} 🐾. Detectamos facturas impagas del mes. ¡Gracias!")
    for r in filas:
        phone = _uy_to_digits(r["telefono"])
        if not phone:
            continue
        msg = tpl.replace("{CLIENTE}", r["nombre"])
        try:
            cur.execute("""
                INSERT INTO reminder_queue (tipo, cliente_id, telefono, mensaje, programado_en)
                VALUES ('particular', ?, ?, ?, ?)
            """, (r["id"], phone, msg, programado))
        except Exception:
            pass
    conn.commit(); conn.close()

def _auto_generate_tasks_if_needed():
    cfg = _get_cfg(); today = _hoy().date()
    if not cfg: return
    if cfg["mensual_enabled"]:
        _gen_mensual_auto(today, cfg)
    if cfg["vacunas_enabled"]:
        _gen_vacunas_auto(today, cfg)
    if cfg["part_enabled"]:
        _gen_part_impagos_auto(today, cfg)

# ---------- Scheduler ----------
LAST_TICK = None



def _reset_errors_to_pending():
    conn = dict_row(db_conn()); cur = conn.cursor()
    try:
        cur.execute("UPDATE reminder_queue SET estado='pendiente', intentos=0, last_error=NULL WHERE estado='error'")
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()
def _process_pending_batch():
    conn = dict_row(db_conn()); cur = conn.cursor()
    rows = cur.execute("""
        SELECT * FROM reminder_queue
        WHERE estado='pendiente' AND DATETIME(programado_en) <= DATETIME('now')
        ORDER BY programado_en ASC
        LIMIT ?
    """, (BATCH_SIZE,)).fetchall()

    for r in rows:
        try:
            cur.execute("UPDATE reminder_queue SET estado='enviando', last_error=NULL, last_try_at=datetime('now') WHERE id=?", (r["id"],))
            conn.commit()
            ok = False
            if USE_SELENIUM and SENDER:
                ok = SENDER.send_and_verify(_uy_to_digits(r["telefono"]), r["mensaje"])
            if ok:
                cur.execute("UPDATE reminder_queue SET estado='enviado', enviado_en=datetime('now') WHERE id=?", (r["id"],))
            else:
                cur.execute("""
                    UPDATE reminder_queue
                       SET estado='error',
                           intentos=intentos+1,
                           last_error=COALESCE(last_error,'Fallo') || ' @' || strftime('%H:%M')
                     WHERE id=?
                """, (r["id"],))
            conn.commit()
            time.sleep(PAUSA_ENTRE_ENVIOS)
        except Exception as e:
            try:
                cur.execute("""
                    UPDATE reminder_queue
                       SET estado='error', intentos=intentos+1, last_error=?
                     WHERE id=?
                """, (str(e), r["id"]))
                conn.commit()
            except Exception:
                pass
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

start_scheduler()

# ---------- Rutas ----------


@bp.route("/reintentar_todos")
def reintentar_todos():
    try:
        _reset_errors_to_pending()
        flash("Todos los errores fueron reprogramados como 'pendiente'.", "info")
    except Exception as e:
        flash(f"No se pudo reprogramar: {e}", "danger")
    return redirect(url_for("recordatorios.dashboard"))

@bp.after_request
def _no_cache(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

@bp.route("/diag")
def diag():
    conn = dict_row(db_conn())
    pend = conn.execute("SELECT COUNT(*) c FROM reminder_queue WHERE estado='pendiente'").fetchone()["c"]
    err  = conn.execute("SELECT COUNT(*) c FROM reminder_queue WHERE estado='error'").fetchone()["c"]
    env  = conn.execute("SELECT COUNT(*) c FROM reminder_queue WHERE estado='enviado'").fetchone()["c"]
    conn.close()
    return jsonify({
        "attached": getattr(SENDER, "attach_mode", False),
        "attach_error": getattr(SENDER, "attach_error", None),
        "debugger": DEBUGGER_ADDRESS,
        "last_tick": LAST_TICK.strftime("%Y-%m-%d %H:%M:%S") if LAST_TICK else None,
        "pendientes": pend, "errores": err, "enviados": env
    })

@bp.route("/")
def dashboard():
    cfg_conn = dict_row(db_conn())
    cfg = cfg_conn.execute("SELECT * FROM reminder_config WHERE id=1").fetchone()
    cfg_conn.close()

    conn = dict_row(db_conn()); cur = conn.cursor()

    # No referenciamos columnas que pueden no existir (c.ci, c.documento, etc.)
    pend_rows = cur.execute("""
        SELECT q.*,
               c.nombre AS cliente_nombre
        FROM reminder_queue q
        LEFT JOIN clientes c ON c.id=q.cliente_id
        WHERE q.estado IN ('pendiente','enviando','error')
        ORDER BY q.programado_en ASC, q.id ASC
    """).fetchall()

    env_rows = cur.execute("""
        SELECT q.*,
               c.nombre AS cliente_nombre
        FROM reminder_queue q
        LEFT JOIN clientes c ON c.id=q.cliente_id
        WHERE q.estado='enviado'
        ORDER BY q.enviado_en DESC
        LIMIT 50
    """).fetchall()

    conn.close()

    # Agregamos 'cliente_doc' seguro
    pendientes = _add_cliente_doc(pend_rows)
    enviados   = _add_cliente_doc(env_rows)

    return render_template("recordatorios.html", cfg=cfg, pendientes=pendientes, enviados=enviados)

@bp.route("/config", methods=["POST"])
def config():
    mensual_enabled = 1 if request.form.get("mensual_enabled") else 0
    mensual_template = (request.form.get("mensual_template") or "").strip()
    mensual_hora = (request.form.get("mensual_hora") or "10:00").strip()
    mensual_dia_mes = int(request.form.get("mensual_dia_mes") or 1)

    vacunas_enabled = 1 if request.form.get("vacunas_enabled") else 0
    vacunas_template = (request.form.get("vacunas_template") or "").strip()
    vacunas_hora = (request.form.get("vacunas_hora") or "10:00").strip()
    vacunas_dias_antes = int(request.form.get("vacunas_dias_antes") or 7)

    part_enabled = 1 if request.form.get("part_enabled") else 0
    part_template = (request.form.get("part_template") or "").strip()
    part_hora = (request.form.get("part_hora") or "10:00").strip()
    part_dia_mes = int(request.form.get("part_dia_mes") or 5)

    conn = db_conn(); cur = conn.cursor()
    cur.execute("""
        UPDATE reminder_config
           SET mensual_enabled=?, mensual_template=?, mensual_hora=?, mensual_dia_mes=?,
               vacunas_enabled=?, vacunas_template=?, vacunas_hora=?, vacunas_dias_antes=?,
               part_enabled=?, part_template=?, part_hora=?, part_dia_mes=?
         WHERE id=1
    """, (mensual_enabled, mensual_template, mensual_hora, mensual_dia_mes,
          vacunas_enabled, vacunas_template, vacunas_hora, vacunas_dias_antes,
          part_enabled, part_template, part_hora, part_dia_mes))
    conn.commit(); conn.close()
    flash("Configuración guardada.", "success")
    return redirect(url_for("recordatorios.dashboard"))

@bp.route("/enviar_pendientes")
def enviar_pendientes():
    # 1) Reprograma errores a 'pendiente' para reintentar
    try:
        _reset_errors_to_pending()
    except Exception:
        pass
    # 2) Genera automáticamente según configuración
    try:
        _auto_generate_tasks_if_needed()
    except Exception:
        pass
    # 3) Envía
    _process_pending_batch()
    flash("Procesamiento manual ejecutado.", "info")
    return redirect(url_for("recordatorios.dashboard"))

@bp.route("/cola/reintentar/<int:id>", methods=["POST"])
def reintentar(id):
    conn = db_conn()
    conn.execute("UPDATE reminder_queue SET estado='pendiente', last_error=NULL WHERE id=?", (id,))
    conn.commit(); conn.close()
    flash("Reintento marcado.", "info")
    return redirect(url_for("recordatorios.dashboard"))

@bp.route("/cola/cancelar/<int:id>", methods=["POST"])
def cancelar(id):
    conn = db_conn()
    conn.execute("UPDATE reminder_queue SET estado='cancelado', last_error=NULL WHERE id=?", (id,))
    conn.commit(); conn.close()
    flash("Envío cancelado.", "info")
    return redirect(url_for("recordatorios.dashboard"))

@bp.route("/cola/eliminar/<int:id>", methods=["POST"])
def eliminar(id):
    conn = db_conn()
    conn.execute("DELETE FROM reminder_queue WHERE id=?", (id,))
    conn.commit(); conn.close()
    flash("Envío eliminado.", "warning")
    return redirect(url_for("recordatorios.dashboard"))

@bp.route("/cola/reprogramar/<int:id>", methods=["POST"])
def reprogramar(id):
    fecha = request.form.get("fecha"); hora = request.form.get("hora")
    if not fecha or not hora:
        flash("Fecha u hora inválidas.", "danger")
        return redirect(url_for("recordatorios.dashboard"))
    conn = db_conn()
    conn.execute("UPDATE reminder_queue SET programado_en=?, last_try_at=NULL WHERE id=?", (f"{fecha} {hora}", id))
    conn.commit(); conn.close()
    flash("Reprogramado.", "success")
    return redirect(url_for("recordatorios.dashboard"))

@bp.route("/cola/abrir_chat/<int:id>")
def abrir_chat(id):
    # Abre el chat EN LA MISMA VENTANA ADJUNTA (sin webbrowser)
    conn = dict_row(db_conn())
    row = conn.execute("SELECT * FROM reminder_queue WHERE id=?", (id,)).fetchone()
    conn.close()
    if not row:
        flash("No encontrado.", "danger")
        return redirect(url_for("recordatorios.dashboard"))

    tel = _uy_to_digits(row["telefono"])
    if not tel:
        flash("Teléfono inválido.", "danger")
        return redirect(url_for("recordatorios.dashboard"))

    if USE_SELENIUM and SENDER:
        try:
            SENDER._ensure_driver()
            SENDER._driver.switch_to.window(SENDER.wa_handle)
            url = f"https://web.whatsapp.com/send?phone={tel}&app_absent=0"
            SENDER._driver.get(url)
            flash("Chat abierto en la ventana de WhatsApp.", "success")
        except Exception as e:
            flash(f"No se pudo abrir el chat: {e}", "danger")
    else:
        flash("Módulo de WhatsApp deshabilitado.", "danger")
    return redirect(url_for("recordatorios.dashboard"))

@bp.route("/nuevo", methods=["POST"])
def nuevo():
    tel = _uy_to_digits(request.form.get("tel"))
    msg = (request.form.get("mensaje") or "").strip()
    fecha = request.form.get("fecha"); hora = request.form.get("hora")
    if not (tel and msg and fecha and hora):
        flash("Completá teléfono, mensaje, fecha y hora.", "danger")
        return redirect(url_for("recordatorios.dashboard"))
    when = f"{fecha} {hora}"
    conn = db_conn(); cur = conn.cursor()
    cur.execute("INSERT INTO reminder_queue (tipo, cliente_id, telefono, mensaje, programado_en) VALUES ('manual', NULL, ?, ?, ?)", (tel, msg, when))
    conn.commit(); conn.close()
    flash("Programado.", "success")
    return redirect(url_for("recordatorios.dashboard"))
