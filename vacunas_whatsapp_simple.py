# -*- coding: utf-8 -*-
import argparse
import os
import re
import sqlite3
import time
from datetime import date, datetime, timedelta

import pywhatkit  # pip install pywhatkit

CLINIC_NAME = "Veterinaria Margay"


# ----------------- Helpers básicos -----------------
def _log(msg, verbose):
    if verbose:
        print(msg)


def get_conn(db_path: str):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"No se encontró la base de datos en: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_recordatorios_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vacuna_recordatorios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacuna_id INTEGER UNIQUE,
            fecha_envio TEXT NOT NULL,
            ok INTEGER DEFAULT 1
        )
    """)
    conn.commit()


def normalize_plain(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    return s


def uy_to_e164_plus(phone_raw: str) -> str | None:
    """
    Devuelve +598XXXXXXXX para pywhatkit (o None si no hay).
    """
    if not phone_raw:
        return None
    digits = re.sub(r"\D", "", phone_raw)
    if not digits:
        return None
    if digits.startswith("598"):
        return "+" + digits
    if digits.startswith("0"):
        return "+598" + digits.lstrip("0")
    if len(digits) >= 8:
        return "+598" + digits
    return None


def has_future_vaccine_appt(conn, animal_id: int, today_iso: str) -> bool:
    rows = conn.execute("""
        SELECT m.nombre
          FROM agenda a
          JOIN motivos m ON m.id = a.motivo_id
         WHERE a.animal_id = ?
           AND a.fecha >= ?
           AND a.atendida = 0
    """, (animal_id, today_iso)).fetchall()
    for r in rows:
        n = normalize_plain(r["nombre"])
        if n in ("vacuna", "vacunacion"):
            return True
    return False


def already_notified(conn, vacuna_id: int) -> bool:
    r = conn.execute("SELECT 1 FROM vacuna_recordatorios WHERE vacuna_id=?", (vacuna_id,)).fetchone()
    return r is not None


def build_msg(cliente_nombre: str, animal_nombre: str, fecha_venc: str) -> str:
    try:
        fecha_fmt = datetime.strptime(fecha_venc, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        fecha_fmt = fecha_venc
    return (
        f"Hola {cliente_nombre}, te escribe {CLINIC_NAME} 🐾\n\n"
        f"Vemos que la vacuna de {animal_nombre} vence el {fecha_fmt}.\n"
        f"¿Querés coordinar la renovación?\n"
        f"Respondé este WhatsApp y te agendamos. ¡Gracias!"
    )


def collect_candidates(conn, days: int, verbose=False):
    hoy = date.today()
    hasta = hoy + timedelta(days=days)

    rows = conn.execute("""
        SELECT
            v.id                 AS vacuna_id,
            v.animal_id          AS animal_id,
            v.fecha_vacuna       AS fecha_vacuna,
            v.fecha_vencimiento  AS fecha_vencimiento,
            an.nombre            AS animal_nombre,
            cl.id                AS cliente_id,
            cl.nombre            AS cliente_nombre,
            cl.telefono          AS cliente_telefono
        FROM vacunas v
        JOIN animales an ON an.id = v.animal_id
        JOIN clientes cl ON cl.id = an.cliente_id
        WHERE date(v.fecha_vencimiento) >= date(?)
          AND date(v.fecha_vencimiento) <= date(?)
        ORDER BY v.fecha_vencimiento, cl.nombre COLLATE NOCASE
    """, (hoy.isoformat(), hasta.isoformat())).fetchall()

    out = []
    for r in rows:
        if already_notified(conn, r["vacuna_id"]):
            _log(f"[skip] vacuna_id={r['vacuna_id']}: ya recordada.", verbose)
            continue
        if has_future_vaccine_appt(conn, r["animal_id"], hoy.isoformat()):
            _log(f"[skip] vacuna_id={r['vacuna_id']}: ya tiene cita futura de vacuna.", verbose)
            continue
        out.append(r)
    return out


# ----------------- Envío con pywhatkit -----------------
def send_whatsapp(phone_plus: str, text: str, verbose=False) -> bool:
    """
    Usa pywhatkit para abrir WhatsApp Web en el navegador POR DEFECTO y enviar automáticamente.
    Requiere que ya estés logueado en ese navegador.
    Devuelve True si no lanzó excepciones.
    """
    try:
        _log(f"[verbose] Enviando a {phone_plus}: {text[:60]!r}...", verbose)
        # Espera interna: pywhatkit abre la pestaña, espera que cargue y manda Enter.
        # Ajustá wait_time si tu PC o internet tarda más en cargar WhatsApp Web.
        pywhatkit.sendwhatmsg_instantly(
            phone_no=phone_plus,
            message=text,
            wait_time=20,   # segundos de espera antes de enviar
            tab_close=True,
            close_time=3
        )
        # Pequeño respiro para no encimar pestañas/envíos
        time.sleep(2)
        return True
    except Exception as e:
        _log(f"[error] Fallo enviando a {phone_plus}: {e}", verbose)
        return False


def main():
    ap = argparse.ArgumentParser(description="Avisos automáticos por WhatsApp Web para vacunas próximas a vencer")
    ap.add_argument("--db", required=True, help="Ruta a veterinaria.db")
    ap.add_argument("--days", type=int, default=10, help="Días hacia adelante (default 10)")
    ap.add_argument("--dry-run", action="store_true", help="No enviar, solo listar")
    ap.add_argument("--max", type=int, default=0, help="Máximo de envíos (0 = sin límite)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    conn = get_conn(args.db)
    ensure_recordatorios_table(conn)

    hoy = date.today()
    if args.verbose:
        print(f"[verbose] Usando DB: {args.db}")
        print(f"[verbose] Hoy: {hoy.isoformat()}  |  Ventana: {args.days} días → {(hoy + timedelta(days=args.days)).isoformat()}")

    cand = collect_candidates(conn, args.days, verbose=args.verbose)

    if args.dry_run:
        if not cand:
            print("No hay vacunas por vencer en la ventana indicada.")
        else:
            print("Se notificaría a:")
            for r in cand:
                tel = uy_to_e164_plus(r["cliente_telefono"] or "")
                print(f"  • {r['cliente_nombre']} | {r['animal_nombre']} | vence {r['fecha_vencimiento']} | tel: {tel or '—'}")
        return

    if not cand:
        print("No hay vacunas por vencer en la ventana indicada.")
        return

    enviados = 0
    for r in cand:
        if args.max and enviados >= args.max:
            break

        phone_plus = uy_to_e164_plus(r["cliente_telefono"] or "")
        if not phone_plus:
            _log(f"[skip] vacuna_id={r['vacuna_id']}: sin teléfono válido.", args.verbose)
            continue

        msg = build_msg(r["cliente_nombre"], r["animal_nombre"], r["fecha_vencimiento"])

        ok = send_whatsapp(phone_plus, msg, verbose=args.verbose)
        if ok:
            conn.execute(
                "INSERT OR IGNORE INTO vacuna_recordatorios (vacuna_id, fecha_envio, ok) VALUES (?, ?, 1)",
                (r["vacuna_id"], datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )
            conn.commit()
            enviados += 1
            print(f"[OK] {r['cliente_nombre']} — {r['animal_nombre']} (vence {r['fecha_vencimiento']})")
        else:
            print(f"[FALLO] {r['cliente_nombre']} — {r['animal_nombre']} (no se marcará como enviado)")

    print(f"Listo. Envíos exitosos: {enviados}")


if __name__ == "__main__":
    main()
