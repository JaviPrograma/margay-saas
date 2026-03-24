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
#  - Endpoints consistentes con el template (recordatorios.config, etc).

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
import sqlite3, threading, time, datetime as dt, re, os, json
from urllib.parse import quote

bp = Blueprint("recordatorios", __name__, url_prefix="/recordatorios")

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
        mensaje TEXT NOT NULL,
        programado_en TEXT NOT NULL,
        enviado_en TEXT,
        estado TEXT DEFAULT 'pendiente',    -- pendiente|enviado|error|cancelado
        intentos INTEGER DEFAULT 0,
        last_try_at TEXT,
        last_error TEXT
    )""")
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_queue_unique
    ON reminder_queue(tipo, cliente_id, date(programado_en))
    """)
    conn.commit(); conn.close()
init_tables()

# ---------- Utils ----------
def _uy_to_digits(raw: str) -> str | None:
    if not raw: return None
    digits = re.sub(r"\D", "", raw)
    if not digits: return None
    if digits.startswith("598"): return digits
    if digits.startswith("0"):   return "598" + digits.lstrip("0")
    if len(digits) >= 8:         return "598" + digits
    return None

def _hoy(): return dt.datetime.now()
def _str2time(hhmm: str):
    try:
        h, m = hhmm.split(":"); return dt.time(int(h), int(m))
    except Exception:
        return dt.time(10,0)
def _dt_from_date_and_hhmm(date_, hhmm: str): return dt.datetime.combine(date_, _str2time(hhmm))
def _short(t, n=220): t=(t or "").strip(); return t if len(t)<=n else t[:n]+"…"

def _get_cliente_info(cliente_id):
    if not cliente_id: return {"nombre":"-","doc":"-"}
    conn = dict_row(db_conn())
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(clientes)")]
    name_col = "nombre" if "nombre" in cols else None
    doc_col  = "cedula" if "cedula" in cols else ("ci" if "ci" in cols else ("documento" if "documento" in cols else None))
    sel_name = name_col if name_col else "'-'"
    sel_doc  = doc_col  if doc_col  else "''"
    r = conn.execute(f"SELECT {sel_name} AS nombre, {sel_doc} AS doc, telefono FROM clientes WHERE id=?", (cliente_id,)).fetchone()
    conn.close()
    return {"nombre": r["nombre"] if r else "-", "doc": r["doc"] if r else "-", "telefono": r["telefono"] if r else None}

# ---------- Selenium: ADJUNTAR a Chrome ya abierto ----------
USE_SELENIUM = True
_sender_lock = threading.Lock()

class _WhatsSender:
    def __init__(self):
        self._driver = None
        self.By = None
        self.WebDriverWait = None
        self.EC = None
        self.attach_mode = False
        self.attach_error = None
        self.wa_handle = None

    def _ensure_driver(self):
        if self._driver: return
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service as ChromeService
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        self.By = By; self.WebDriverWait = WebDriverWait; self.EC = EC

        # Intentar adjuntarse a la ventana EXISTENTE
        try:
            opts = webdriver.ChromeOptions()
            opts.add_experimental_option("debuggerAddress", DEBUGGER_ADDRESS)
            self._driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
            self.attach_mode = True
            self.attach_error = None
            print(f"[recordatorios] Adjuntado a Chrome en {DEBUGGER_ADDRESS}")
        except Exception as e:
            self.attach_error = str(e)
            self._driver = None
            self.attach_mode = False

        if not self._driver and STRICT_ATTACH_ONLY:
            raise RuntimeError(
                f"No pude adjuntarme a Chrome en {DEBUGGER_ADDRESS}. "
                "Abrí Chrome con --remote-debugging-port=9222 (mismo perfil) y dejá WhatsApp Web logueado."
            )

        # Buscar pestaña con WhatsApp Web; si no hay, abrir una pestaña NUEVA en la MISMA ventana
        wa = None
        for h in self._driver.window_handles[:]:
            self._driver.switch_to.window(h)
            try:
                url = self._driver.current_url or ""
            except Exception:
                url = ""
            if "web.whatsapp.com" in url:
                wa = h; break

        if not wa:
            # abrir una pestaña en la misma ventana
            self._driver.execute_script("window.open('https://web.whatsapp.com','_blank');")
            time.sleep(0.5)
            wa = self._driver.window_handles[-1]

        self.wa_handle = wa
        self._driver.switch_to.window(self.wa_handle)
        # Esperar que cargue (si ya está logueado, aparece pane-side)
        self.WebDriverWait(self._driver, 180).until(
            self.EC.presence_of_element_located((self.By.TAG_NAME, "body"))
        )

    def _normalize(self, s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip())

    def _find_editor(self, timeout=35):
        W = self.WebDriverWait; By = self.By; EC = self.EC
        candidates = [
            (By.CSS_SELECTOR, "div[contenteditable='true'][data-lexical-editor='true']:not([aria-hidden='true'])"),
            (By.CSS_SELECTOR, "div[contenteditable='true'][role='textbox']:not([aria-hidden='true'])"),
            (By.CSS_SELECTOR, "div[contenteditable='true'][data-tab='10']"),
            (By.CSS_SELECTOR, "div[contenteditable='true'][data-tab='6']"),
        ]
        last = None
        for how, sel in candidates:
            try:
                el = W(self._driver, timeout).until(EC.presence_of_element_located((how, sel)))
                W(self._driver, 10).until(EC.element_to_be_clickable((how, sel)))
                return el
            except Exception as e:
                last = e
        raise last or Exception("No se encontró el editor de mensajes")

    def _click_send_button(self):
        By = self.By; EC = self.EC; W = self.WebDriverWait
        sels = [
            ("css", "button[aria-label*='Enviar']"),
            ("css", "button span[data-icon='send']"),
            ("xpath", "//button[.//span[@data-icon='send']]"),
            ("xpath", "//div[@role='button' and contains(@aria-label,'Enviar')]"),
        ]
        for mode, sel in sels:
            try:
                el = W(self._driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, sel)) if mode=="css"
                    else EC.element_to_be_clickable((By.XPATH, sel))
                )
                # a veces el ícono no es <button>; subir al botón
                try:
                    if el.tag_name.lower() != "button":
                        el = el.find_element(By.XPATH, "./ancestor::button")
                except Exception:
                    pass
                el.click()
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

            # Abrir chat de ese número en ESTA pestaña
            url = f"https://web.whatsapp.com/send?phone={phone_digits}&app_absent=0"
            self._driver.get(url)

            # Editor
            try:
                editor = self._find_editor(timeout=35)
            except Exception:
                return False

            # Foco y limpiar
            try:
                self._driver.execute_script("arguments[0].focus();", editor)
            except Exception:
                pass
            try:
                ActionChains(self._driver).key_down(Keys.CONTROL).send_keys("a").key_up(Keys.CONTROL).send_keys(Keys.DELETE).perform()
            except Exception:
                pass

            # Escribir (respetando saltos)
            lines = (text or "").split("\n")
            for i, part in enumerate(lines):
                if part: editor.send_keys(part)
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

            # Verificar burbuja
            target = self._normalize(text or "")
            t0 = time.time()
            while time.time() - t0 < 25:
                outs = self._driver.find_elements(self.By.CSS_SELECTOR, "div.message-out")
                if outs:
                    last = outs[-1]
                    spans = last.find_elements(self.By.CSS_SELECTOR, "span[dir='ltr'], span[dir='auto'], span.selectable-text")
                    bubble_txt = self._normalize(" ".join(s.text for s in spans))
                    if bubble_txt.endswith(target) or bubble_txt == target or target.endswith(bubble_txt):
                        return True
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
    if today.day != (cfg["mensual_dia_mes"] or 1): return
    hhmm = cfg["mensual_hora"] or "10:00"
    programado = _dt_from_date_and_hhmm(today, hhmm).strftime("%Y-%m-%d %H:%M")
    conn = dict_row(db_conn()); cur = conn.cursor()
    rows = cur.execute("""
        SELECT id, nombre, telefono
        FROM clientes
        WHERE tipo='Mensual' AND COALESCE(activo,1)=1
    """).fetchall()
    tpl = (cfg["mensual_template"] or f"Hola {{CLIENTE}}, te escribe {CLINICA} 🐾. Recordatorio de tu mensualidad de {{MES}}/{{ANIO}}. ¡Gracias!")
    mes = today.month; anio = today.year
    for r in rows:
        phone = _uy_to_digits(r["telefono"])
        if not phone:
            continue
        msg = tpl.replace("{CLIENTE}", r["nombre"]).replace("{MES}", f"{mes:02d}").replace("{ANIO}", f"{anio}")
        try:
            cur.execute("""
                INSERT OR IGNORE INTO reminder_queue
                (tipo, cliente_id, telefono, mensaje, programado_en)
                VALUES ('mensual', ?, ?, ?, ?)
            """, (r["id"], phone, msg, programado))
        except Exception:
            pass
    conn.commit(); conn.close()

def _gen_vacunas_auto(today, cfg):
    anticip = int(cfg["vacunas_dias_antes"] or 7)
    objetivo = today + dt.timedelta(days=anticip)
    hhmm = cfg["vacunas_hora"] or "10:00"
    programado = _dt_from_date_and_hhmm(today, hhmm).strftime("%Y-%m-%d %H:%M")
    conn = dict_row(db_conn()); cur = conn.cursor()
    vacs = cur.execute("""
        SELECT v.id AS vacuna_id, v.fecha_vencimiento,
               a.id AS animal_id, a.nombre AS animal_nombre,
               c.id AS cliente_id, c.nombre AS cliente_nombre, c.telefono
        FROM vacunas v
        JOIN animales a ON a.id=v.animal_id
        JOIN clientes c ON c.id=a.cliente_id
        WHERE date(v.fecha_vencimiento)=date(?)
    """, (objetivo.isoformat(),)).fetchall()

    por_cliente = {}
    for r in vacs:
        por_cliente.setdefault((r["cliente_id"], r["cliente_nombre"], r["telefono"]), []).append(
            (r["animal_nombre"], r["fecha_vencimiento"])
        )

    tpl = (cfg["vacunas_template"] or f"Hola {{CLIENTE}}, te escribe {CLINICA} 🐾.\nRecordatorio:\n{{LISTADO}}\n¡Gracias!")
    for (cid, cname, tel), items in por_cliente.items():
        phone = _uy_to_digits(tel)
        if not phone:
            continue
        listado = "\n".join([f"• {nom}: vence {dt.datetime.strptime(fv,'%Y-%m-%d').strftime('%d/%m/%Y')}" for nom, fv in items])
        msg = tpl.replace("{CLIENTE}", cname).replace("{LISTADO}", listado)
        try:
            cur.execute("""
                INSERT OR IGNORE INTO reminder_queue
                (tipo, cliente_id, telefono, mensaje, programado_en)
                VALUES ('vacuna', ?, ?, ?, ?)
            """, (cid, phone, msg, programado))
        except Exception:
            pass
    conn.commit(); conn.close()

def _gen_part_auto(today, cfg):
    if today.day != (cfg["part_dia_mes"] or 5): return
    hhmm = cfg["part_hora"] or "10:00"
    programado = _dt_from_date_and_hhmm(today, hhmm).strftime("%Y-%m-%d %H:%M")
    ini = today.replace(day=1).isoformat()
    fin = (today.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
    fin = fin.isoformat()
    conn = dict_row(db_conn()); cur = conn.cursor()
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
    """, (ini, fin)).fetchall()
    tpl = (cfg["part_template"] or f"Hola {{CLIENTE}}, te escribe {CLINICA} 🐾. Detectamos facturas impagas del mes. ¡Gracias!")
    for r in filas:
        phone = _uy_to_digits(r["telefono"])
        if not phone:
            continue
        msg = tpl.replace("{CLIENTE}", r["nombre"])
        try:
            cur.execute("""
                INSERT OR IGNORE INTO reminder_queue
                (tipo, cliente_id, telefono, mensaje, programado_en)
                VALUES ('particular', ?, ?, ?, ?)
            """, (r["id"], phone, msg, programado))
        except Exception:
            pass
    conn.commit(); conn.close()

def _auto_generate_tasks_if_needed():
    cfg = _get_cfg(); today = _hoy().date()
    if not cfg: return
    if cfg["mensual_enabled"]: _gen_mensual_auto(today, cfg)
    if cfg["vacunas_enabled"]: _gen_vacunas_auto(today, cfg)
    if cfg["part_enabled"]:    _gen_part_auto(today, cfg)

# ---------- Envío ----------
def _eligible(row):
    if not row["last_try_at"]: return True
    try:
        t = dt.datetime.strptime(row["last_try_at"], "%Y-%m-%d %H:%M:%S")
        return (_hoy() - t) >= dt.timedelta(minutes=RETRY_DELAY_MIN)
    except Exception:
        return True

def _send_one(r):
    phone = r["telefono"]
    if not phone: return False, "Teléfono vacío/inválido"
    if not SENDER: return False, "Selenium no inicializado"
    try:
        ok = SENDER.send_and_verify(phone, r["mensaje"])
        return (True, "") if ok else (False, "No se verificó burbuja enviada")
    except Exception as e:
        # Reintento con reinicio de adjunte
        try:
            if getattr(SENDER, "_driver", None):
                SENDER._driver.quit()
        except Exception:
            pass
        try:
            SENDER._driver = None
            ok = SENDER.send_and_verify(phone, r["mensaje"])
            return (True, "") if ok else (False, "No se verificó tras reinicio")
        except Exception as e2:
            return False, _short(f"{e2}")

def _send_due():
    now_iso = _hoy().strftime("%Y-%m-%d %H:%M")
    conn = dict_row(db_conn()); cur = conn.cursor()
    rows = cur.execute("""
        SELECT * FROM reminder_queue
        WHERE (estado='pendiente' OR estado='error')
          AND programado_en<=?
        ORDER BY programado_en ASC, id ASC
        LIMIT ?
    """, (now_iso, BATCH_SIZE)).fetchall()

    for r in rows:
        if not _eligible(r): continue
        cur.execute("UPDATE reminder_queue SET last_try_at=datetime('now') WHERE id=?", (r["id"],))
        conn.commit()

        ok, err = _send_one(r)
        time.sleep(PAUSA_ENTRE_ENVIOS)

        if ok:
            cur.execute("""
                UPDATE reminder_queue
                   SET estado='enviado', enviado_en=datetime('now'),
                       intentos=intentos+1, last_error=NULL
                 WHERE id=?""", (r["id"],))
        else:
            cur.execute("""
                UPDATE reminder_queue
                   SET estado='error', intentos=intentos+1, last_error=?
                 WHERE id=?""", (_short(err), r["id"]))
        conn.commit()
    conn.close()

# ---------- Scheduler ----------
_scheduler_started = False
_stop_flag = False
LAST_TICK = None

def _scheduler_loop():
    global LAST_TICK
    while not _stop_flag:
        try:
            LAST_TICK = _hoy()
            _auto_generate_tasks_if_needed()
            _send_due()
        except Exception as e:
            print("[recordatorios] scheduler error:", repr(e))
        time.sleep(30)

def start_scheduler():
    global _scheduler_started
    if _scheduler_started: return
    threading.Thread(target=_scheduler_loop, daemon=True).start()
    _scheduler_started = True
start_scheduler()

# ---------- Rutas ----------
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

    conn = dict_row(db_conn())
    pend_rows = conn.execute("""
        SELECT * FROM reminder_queue 
        WHERE estado IN ('pendiente','error')
        ORDER BY programado_en, id
    """).fetchall()
    env_rows = conn.execute("""
        SELECT * FROM reminder_queue 
        WHERE estado='enviado'
        ORDER BY enviado_en DESC, id DESC LIMIT 50
    """).fetchall()
    conn.close()

    def enrich(rows):
        out=[]
        for r in rows:
            d=dict(r)
            info=_get_cliente_info(d.get("cliente_id"))
            d["cliente_nombre"]=info["nombre"]; d["cliente_doc"]=info["doc"]
            out.append(d)
        return out

    return render_template("recordatorios.html",
                           cfg=cfg,
                           pendientes=enrich(pend_rows),
                           enviados=enrich(env_rows),
                           retry_delay_min=RETRY_DELAY_MIN,
                           max_retries=MAX_RETRIES)

# endpoint="config" para que el HTML pueda usar url_for('recordatorios.config')
@bp.route("/config", methods=["POST"], endpoint="config")
def guardar_config():
    f = request.form
    def onoff(name): return 1 if f.get(name) == "on" else 0
    conn = db_conn(); cur = conn.cursor()
    cur.execute("""
        UPDATE reminder_config SET
        mensual_enabled=?, mensual_template=?, mensual_hora=?, mensual_dia_mes=?,
        vacunas_enabled=?, vacunas_template=?, vacunas_hora=?, vacunas_dias_antes=?,
        part_enabled=?, part_template=?, part_hora=?, part_dia_mes=?
        WHERE id=1
    """, (
        onoff("mensual_enabled"), f.get("mensual_template") or None, f.get("mensual_hora") or "10:00", int(f.get("mensual_dia_mes") or 1),
        onoff("vacunas_enabled"), f.get("vacunas_template") or None, f.get("vacunas_hora") or "10:00", int(f.get("vacunas_dias_antes") or 7),
        onoff("part_enabled"),    f.get("part_template") or None,    f.get("part_hora") or "10:00",  int(f.get("part_dia_mes") or 5),
    ))
    conn.commit(); conn.close()
    flash("Configuración guardada.", "success")
    return redirect(url_for("recordatorios.dashboard"))

@bp.route("/enviar_pendientes")
def enviar_pendientes():
    _send_due()
    flash("Pendientes procesados ahora.", "success")
    return redirect(url_for("recordatorios.dashboard"))

@bp.route("/cola/reintentar/<int:id>", methods=["POST"])
def reintentar(id):
    conn = db_conn()
    conn.execute("UPDATE reminder_queue SET estado='pendiente', last_try_at=NULL, last_error=NULL WHERE id=?", (id,))
    conn.commit(); conn.close()
    _send_due()
    flash("Reintento ejecutado.", "success")
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
        flash("Elemento no encontrado.", "warning")
        return redirect(url_for("recordatorios.dashboard"))
    tel = row["telefono"]; msg = row["mensaje"] or ""
    if not tel:
        flash("Teléfono inválido.", "warning")
        return redirect(url_for("recordatorios.dashboard"))

    try:
        # usa el driver adjunto para abrir el chat en esa misma pestaña
        if not SENDER: raise RuntimeError("Selenium no inicializado")
        with _sender_lock:
            SENDER._ensure_driver()
            SENDER._driver.switch_to.window(SENDER.wa_handle)
            url = f"https://web.whatsapp.com/send?phone={tel}&text={quote(msg)}&app_absent=0"
            SENDER._driver.get(url)
        flash("Chat abierto en la ventana adjunta (envío manual).", "info")
    except Exception as e:
        flash(f"No se pudo abrir en la ventana adjunta: {_short(str(e))}", "danger")
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
    cur.execute("""
        INSERT INTO reminder_queue (tipo, cliente_id, telefono, mensaje, programado_en)
        VALUES ('manual', NULL, ?, ?, ?)
    """, (tel, msg, when))
    conn.commit(); conn.close()
    flash("Programado.", "success")
    return redirect(url_for("recordatorios.dashboard"))
