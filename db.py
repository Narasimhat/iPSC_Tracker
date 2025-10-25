import os
import shutil
import sqlite3
from contextlib import closing
from datetime import datetime, date
from typing import Dict, Any, List, Optional, Tuple


DB_PATH = os.path.join(os.path.dirname(__file__), "ipsc_tracker.db")
IMAGES_DIR = os.path.join(os.path.dirname(__file__), "images")


def ensure_dirs() -> None:
    os.makedirs(IMAGES_DIR, exist_ok=True)


def get_conn(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Improve reliability for concurrent reads
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
    except Exception:
        pass
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    with closing(conn.cursor()) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                display_name TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                cell_line TEXT,
                event_type TEXT,
                passage INTEGER,
                vessel TEXT,
                location TEXT,
                medium TEXT,
                cell_type TEXT,
                notes TEXT,
                operator TEXT,
                thaw_id TEXT,
                cryo_vial_position TEXT,
                image_path TEXT,
                assigned_to TEXT,
                next_action_date TEXT,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        # Reference tables for dropdowns
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cell_lines (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS event_types (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS vessels (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS locations (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cell_types (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS culture_media (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_thaw_id ON logs (thaw_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs (created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_created_by ON logs (created_by)")
        # Migrations: add columns if missing
        cur.execute("PRAGMA table_info(logs)")
        cols = {row[1] for row in cur.fetchall()}
        if "cell_type" not in cols:
            cur.execute("ALTER TABLE logs ADD COLUMN cell_type TEXT")
        if "assigned_to" not in cols:
            cur.execute("ALTER TABLE logs ADD COLUMN assigned_to TEXT")
        if "next_action_date" not in cols:
            cur.execute("ALTER TABLE logs ADD COLUMN next_action_date TEXT")
        if "volume" not in cols:
            cur.execute("ALTER TABLE logs ADD COLUMN volume REAL")
        conn.commit()

    # Seed default event types if empty
    with closing(conn.cursor()) as cur:
        cur.execute("SELECT COUNT(*) FROM event_types")
        count = cur.fetchone()[0]
        if count == 0:
            defaults = [
                "Observation",
                "Media Change",
                "Split",
                "Thawing",
                "Cryopreservation",
                "Other",
            ]
            now = datetime.utcnow().isoformat()
            cur.executemany(
                "INSERT OR IGNORE INTO event_types (name, created_at) VALUES (?, ?)",
                [(d, now) for d in defaults],
            )
            conn.commit()

    # Seed default cell types if empty
    with closing(conn.cursor()) as cur:
        cur.execute("SELECT COUNT(*) FROM cell_types")
        ct_count = cur.fetchone()[0]
        if ct_count == 0:
            now = datetime.utcnow().isoformat()
            cell_type_defaults = ["iPSC", "NPC", "Cardiomyocyte"]
            cur.executemany(
                "INSERT OR IGNORE INTO cell_types (name, created_at) VALUES (?, ?)",
                [(d, now) for d in cell_type_defaults],
            )
            conn.commit()

    # Seed default culture media if empty
    with closing(conn.cursor()) as cur:
        cur.execute("SELECT COUNT(*) FROM culture_media")
        cm_count = cur.fetchone()[0]
        if cm_count == 0:
            now = datetime.utcnow().isoformat()
            media_defaults = ["StemFlex", "mTeSR1", "E8"]
            cur.executemany(
                "INSERT OR IGNORE INTO culture_media (name, created_at) VALUES (?, ?)",
                [(d, now) for d in media_defaults],
            )
            conn.commit()


def get_or_create_user(conn: sqlite3.Connection, username: str, display_name: Optional[str] = None) -> Dict[str, Any]:
    username = username.strip()
    if not username:
        raise ValueError("Username required")
    with closing(conn.cursor()) as cur:
        cur.execute("SELECT * FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
        if row:
            return dict(row)
        cur.execute(
            "INSERT INTO users (username, display_name, created_at) VALUES (?, ?, ?)",
            (username, display_name or username, datetime.utcnow().isoformat()),
        )
        conn.commit()
        return {
            "id": cur.lastrowid,
            "username": username,
            "display_name": display_name or username,
            "created_at": datetime.utcnow().isoformat(),
        }


def delete_user(conn: sqlite3.Connection, username: str) -> None:
    username = username.strip()
    if not username:
        return
    with closing(conn.cursor()) as cur:
        cur.execute("DELETE FROM users WHERE username = ?", (username,))
        conn.commit()


def generate_thaw_id_for_date(conn: sqlite3.Connection, d: date) -> str:
    day = d.strftime("%Y%m%d")
    with closing(conn.cursor()) as cur:
        cur.execute(
            "SELECT COUNT(*) FROM logs WHERE event_type = 'Thawing' AND date = ?",
            (d.isoformat(),),
        )
        count = cur.fetchone()[0] or 0
    return f"TH-{day}-{count + 1:03d}"


def insert_log(conn: sqlite3.Connection, payload: Dict[str, Any]) -> int:
    cols = [
        "date",
        "cell_line",
        "event_type",
        "passage",
        "vessel",
        "location",
        "medium",
    "cell_type",
        "notes",
        "operator",
        "thaw_id",
        "cryo_vial_position",
        "image_path",
    "assigned_to",
    "next_action_date",
    "volume",
        "created_by",
        "created_at",
    ]
    values = [payload.get(c) for c in cols]
    with closing(conn.cursor()) as cur:
        cur.execute(
            f"INSERT INTO logs ({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))})",
            values,
        )
        conn.commit()
        return cur.lastrowid


def query_logs(
    conn: sqlite3.Connection,
    *,
    user: Optional[str] = None,
    event_type: Optional[str] = None,
    thaw_id: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    cell_line_contains: Optional[str] = None,
) -> List[Dict[str, Any]]:
    where: List[str] = []
    params: List[Any] = []
    if user:
        where.append("created_by = ?")
        params.append(user)
    if event_type and event_type != "(any)":
        where.append("event_type = ?")
        params.append(event_type)
    if thaw_id:
        where.append("thaw_id = ?")
        params.append(thaw_id)
    if start_date:
        where.append("date >= ?")
        params.append(start_date.isoformat())
    if end_date:
        where.append("date <= ?")
        params.append(end_date.isoformat())
    if cell_line_contains:
        where.append("LOWER(cell_line) LIKE ?")
        params.append(f"%{cell_line_contains.lower()}%")

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    sql = "SELECT * FROM logs" + where_sql + " ORDER BY date ASC, created_at ASC"
    with closing(conn.cursor()) as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def list_distinct_thaw_ids(conn: sqlite3.Connection) -> List[str]:
    with closing(conn.cursor()) as cur:
        cur.execute("SELECT DISTINCT thaw_id FROM logs WHERE thaw_id IS NOT NULL AND thaw_id <> '' ORDER BY thaw_id")
        rows = cur.fetchall()
    return [r[0] for r in rows]


def _ref_table_for(kind: str) -> str:
    mapping = {
        "cell_line": "cell_lines",
        "event_type": "event_types",
        "vessel": "vessels",
        "location": "locations",
    "cell_type": "cell_types",
    "culture_medium": "culture_media",
    }
    if kind not in mapping:
        raise ValueError("Unsupported ref kind")
    return mapping[kind]


def get_ref_values(conn: sqlite3.Connection, kind: str) -> List[str]:
    table = _ref_table_for(kind)
    with closing(conn.cursor()) as cur:
        cur.execute(f"SELECT name FROM {table} ORDER BY name COLLATE NOCASE ASC")
        rows = cur.fetchall()
    return [r[0] for r in rows]


def add_ref_value(conn: sqlite3.Connection, kind: str, name: str) -> None:
    table = _ref_table_for(kind)
    with closing(conn.cursor()) as cur:
        cur.execute(
            f"INSERT OR IGNORE INTO {table} (name, created_at) VALUES (?, ?)",
            (name.strip(), datetime.utcnow().isoformat()),
        )
        conn.commit()


def delete_ref_value(conn: sqlite3.Connection, kind: str, name: str) -> None:
    table = _ref_table_for(kind)
    with closing(conn.cursor()) as cur:
        cur.execute(f"DELETE FROM {table} WHERE name = ?", (name.strip(),))
        conn.commit()


def rename_ref_value(conn: sqlite3.Connection, kind: str, old_name: str, new_name: str) -> None:
    table = _ref_table_for(kind)
    with closing(conn.cursor()) as cur:
        cur.execute(
            f"UPDATE {table} SET name = ? WHERE name = ?",
            (new_name.strip(), old_name.strip()),
        )
        conn.commit()


def backup_now(dest_root: Optional[str] = None) -> str:
    """Create a timestamped backup of the DB and images directory.

    Returns the backup folder path.
    """
    root = dest_root or os.path.join(os.path.dirname(__file__), "backups")
    os.makedirs(root, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(root, f"backup_{ts}")
    os.makedirs(out_dir, exist_ok=True)
    # Copy DB
    if os.path.exists(DB_PATH):
        shutil.copy2(DB_PATH, os.path.join(out_dir, os.path.basename(DB_PATH)))
    # Copy images (shallow copy maintaining structure)
    if os.path.isdir(IMAGES_DIR):
        img_out = os.path.join(out_dir, "images")
        os.makedirs(img_out, exist_ok=True)
        for root_dir, dirs, files in os.walk(IMAGES_DIR):
            rel = os.path.relpath(root_dir, IMAGES_DIR)
            target_dir = os.path.join(img_out, rel if rel != "." else "")
            os.makedirs(target_dir, exist_ok=True)
            for f in files:
                src = os.path.join(root_dir, f)
                dst = os.path.join(target_dir, f)
                try:
                    shutil.copy2(src, dst)
                except Exception:
                    # Skip unreadable files
                    pass
    return out_dir


def list_distinct_values(
    conn: sqlite3.Connection,
    column: str,
    *,
    cell_line: Optional[str] = None,
    limit: int = 10,
) -> List[str]:
    if column not in {
        "cell_line",
        "event_type",
        "vessel",
        "location",
        "medium",
        "cell_type",
        "operator",
        "assigned_to",
    }:
        raise ValueError("Unsupported column for distinct values")
    where = "WHERE {col} IS NOT NULL AND {col} <> ''".format(col=column)
    params: List[Any] = []
    if cell_line and column != "cell_line":
        where += " AND cell_line = ?"
        params.append(cell_line)
    sql = f"SELECT {column}, COUNT(*) as cnt FROM logs {where} GROUP BY {column} ORDER BY cnt DESC LIMIT {limit}"
    with closing(conn.cursor()) as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def get_last_log_for_cell_line(conn: sqlite3.Connection, cell_line: str) -> Optional[Dict[str, Any]]:
    with closing(conn.cursor()) as cur:
        cur.execute(
            "SELECT * FROM logs WHERE cell_line = ? ORDER BY date DESC, created_at DESC LIMIT 1",
            (cell_line,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_last_log_for_line_event(conn: sqlite3.Connection, cell_line: str, event_type: str) -> Optional[Dict[str, Any]]:
    with closing(conn.cursor()) as cur:
        cur.execute(
            """
            SELECT * FROM logs
            WHERE cell_line = ? AND event_type = ?
            ORDER BY date DESC, created_at DESC
            LIMIT 1
            """,
            (cell_line, event_type),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_recent_logs_for_cell_line(
    conn: sqlite3.Connection,
    cell_line: str,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    with closing(conn.cursor()) as cur:
        cur.execute(
            """
            SELECT * FROM logs
            WHERE cell_line = ?
            ORDER BY date DESC, created_at DESC
            LIMIT ?
            """,
            (cell_line, limit),
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def predict_next_passage(conn: sqlite3.Connection, cell_line: str) -> Optional[int]:
    last = get_last_log_for_cell_line(conn, cell_line)
    if not last:
        return None
    try:
        p = int(last.get("passage") or 0)
        return p + 1 if p > 0 else None
    except Exception:
        return None


def top_values(
    conn: sqlite3.Connection,
    column: str,
    *,
    cell_line: Optional[str] = None,
    limit: int = 3,
) -> List[str]:
    vals = list_distinct_values(conn, column, cell_line=cell_line, limit=limit)
    return vals


def suggest_next_event(conn: sqlite3.Connection, cell_line: str) -> Optional[str]:
    # Heuristic: look at last event; suggest likely follow-up
    last = get_last_log_for_cell_line(conn, cell_line)
    if not last:
        return None
    last_evt = (last.get("event_type") or "").lower()
    mapping = {
        "thawing": "Observation",
        "observation": "Media Change",
        "media change": "Observation",
        "split": "Observation",
        "cryopreservation": "Observation",
    }
    return mapping.get(last_evt)
