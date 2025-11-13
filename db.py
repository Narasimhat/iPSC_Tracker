import os
import json
import shutil
from contextlib import closing
from datetime import date, datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Sequence

import snowflake.connector
from snowflake.connector import DictCursor

DATA_ROOT = os.environ.get("DATA_ROOT", os.path.dirname(__file__))
IMAGES_DIR = os.path.join(DATA_ROOT, "images")

_SNOWFLAKE_KEYS = ("account", "user", "password", "warehouse", "database", "schema", "role")
_WEEKEND_SCHEDULE_FLAGS: Optional[Dict[str, bool]] = None


def ensure_dirs() -> None:
    os.makedirs(IMAGES_DIR, exist_ok=True)


@lru_cache(maxsize=1)
def _snowflake_config() -> Dict[str, str]:
    cfg: Dict[str, str] = {}
    for key in _SNOWFLAKE_KEYS:
        env_key = f"SNOWFLAKE_{key.upper()}"
        if os.environ.get(env_key):
            cfg[key] = os.environ[env_key]
    missing = [k for k in _SNOWFLAKE_KEYS if k not in cfg]
    if missing:
        try:
            import streamlit as st  # type: ignore

            snow_cfg = st.secrets.get("snowflake", {})  # type: ignore[attr-defined]
            for key in missing:
                if key in snow_cfg:
                    cfg[key] = snow_cfg[key]
        except Exception:
            pass
    missing = [k for k in ("account", "user", "password", "warehouse", "database", "schema") if k not in cfg]
    if missing:
        raise RuntimeError(
            "Snowflake configuration missing values: "
            + ", ".join(missing)
            + ". Set SNOWFLAKE_* environment variables or streamlit secrets."
        )
    return cfg


def get_conn():
    cfg = _snowflake_config()
    conn = snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
        role=cfg.get("role"),
        autocommit=True,
        client_session_keep_alive=True,
    )
    return conn


def _dict_cursor(conn):
    return conn.cursor(DictCursor)


def _execute(conn, sql: str, params: Optional[Sequence[Any]] = None) -> None:
    with closing(conn.cursor()) as cur:
        cur.execute(sql, params or ())


def _fetchall_dicts(cur) -> List[Dict[str, Any]]:
    rows = cur.fetchall()
    result: List[Dict[str, Any]] = []
    for row in rows:
        result.append({k.lower(): row[k] for k in row})
    return result


def _table_has_column(conn, table: str, column: str) -> bool:
    with closing(conn.cursor()) as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
              AND column_name = %s
            """,
            (table.upper(), column.upper()),
        )
        return cur.fetchone() is not None


def _weekend_schedule_column_flags(conn) -> Dict[str, bool]:
    global _WEEKEND_SCHEDULE_FLAGS
    if _WEEKEND_SCHEDULE_FLAGS is None:
        _WEEKEND_SCHEDULE_FLAGS = {
            "start_date": _table_has_column(conn, "WEEKEND_SCHEDULE", "START_DATE"),
            "end_date": _table_has_column(conn, "WEEKEND_SCHEDULE", "END_DATE"),
            "assignee": _table_has_column(conn, "WEEKEND_SCHEDULE", "ASSIGNEE"),
        }
    return _WEEKEND_SCHEDULE_FLAGS


def init_db(conn) -> None:
    with closing(conn.cursor()) as cur:
        try:
            cur.execute("DROP VIEW IF EXISTS logs")
        except snowflake.connector.errors.ProgrammingError:
            # If a table already exists with this name, ignore.
            pass
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER AUTOINCREMENT,
                date DATE NOT NULL,
                cell_line VARCHAR,
                event_type VARCHAR,
                passage NUMBER,
                vessel VARCHAR,
                location VARCHAR,
                medium VARCHAR,
                cell_type VARCHAR,
                notes VARCHAR,
                operator VARCHAR,
                thaw_id VARCHAR,
                cryo_vial_position VARCHAR,
                image_path VARCHAR,
                assigned_to VARCHAR,
                next_action_date DATE,
                created_by VARCHAR NOT NULL,
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                volume FLOAT,
                cryo_storage_position VARCHAR,
                PRIMARY KEY (id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                username VARCHAR PRIMARY KEY,
                display_name VARCHAR,
                color_hex VARCHAR,
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS weekend_schedule (
                date DATE PRIMARY KEY,
                assigned_to VARCHAR,
                notes VARCHAR,
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        for table_name in (
            "cell_lines",
            "event_types",
            "vessels",
            "locations",
            "cell_types",
            "culture_media",
        ):
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    name VARCHAR PRIMARY KEY,
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS entry_templates (
                name VARCHAR PRIMARY KEY,
                payload VARIANT,
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    # Ensure legacy Snowflake bootstrap columns exist (from earlier script)
    with closing(conn.cursor()) as cur:
        if not _table_has_column(conn, "USERS", "USERNAME"):
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username VARCHAR")
            cur.execute("UPDATE users SET username = COALESCE(username, name)")
        if not _table_has_column(conn, "USERS", "DISPLAY_NAME"):
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name VARCHAR")
            cur.execute("UPDATE users SET display_name = COALESCE(display_name, initials, username)")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS color_hex VARCHAR")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP_NTZ")
        cur.execute("UPDATE users SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP())")

        # Weekend schedule from earlier script stored start/end; keep but ensure date column exists
        cur.execute("ALTER TABLE weekend_schedule ADD COLUMN IF NOT EXISTS date DATE")
        cur.execute("ALTER TABLE weekend_schedule ADD COLUMN IF NOT EXISTS assigned_to VARCHAR")
        cur.execute("ALTER TABLE weekend_schedule ADD COLUMN IF NOT EXISTS notes VARCHAR")
        cur.execute("ALTER TABLE weekend_schedule ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP_NTZ")

        cur.execute("ALTER TABLE logs ADD COLUMN IF NOT EXISTS cryo_storage_position VARCHAR")
        cur.execute("ALTER TABLE logs ADD COLUMN IF NOT EXISTS image_path VARCHAR")

    # Seed reference defaults
    now = datetime.utcnow()
    for value in ["Observation", "Media Change", "Split", "Thawing", "Cryopreservation", "Other"]:
        _insert_reference_value(conn, "event_types", value, now)
    for value in ["iPSC", "NPC", "Cardiomyocyte"]:
        _insert_reference_value(conn, "cell_types", value, now)
    for value in ["StemFlex", "mTeSR1", "E8"]:
        _insert_reference_value(conn, "culture_media", value, now)


def _insert_reference_value(conn, table: str, value: str, ts: datetime) -> None:
    with closing(conn.cursor()) as cur:
        cur.execute(
            f"""
            INSERT INTO {table} (name, created_at)
            SELECT %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM {table} WHERE name = %s
            )
            """,
            (value, ts, value),
        )


def get_or_create_user(conn, username: str, display_name: Optional[str] = None, color_hex: Optional[str] = None) -> Dict[str, Any]:
    username = (username or "").strip()
    if not username:
        raise ValueError("Username required")
    has_name_col = _table_has_column(conn, "USERS", "NAME")
    has_initials_col = _table_has_column(conn, "USERS", "INITIALS")
    has_is_active_col = _table_has_column(conn, "USERS", "IS_ACTIVE")
    with closing(_dict_cursor(conn)) as cur:
        cur.execute(
            "SELECT username, display_name, color_hex, created_at FROM users WHERE username = %s",
            (username,),
        )
        row = cur.fetchone()
        if row:
            if color_hex and (row.get("COLOR_HEX") or row.get("color_hex")) != color_hex:
                _execute(
                    conn,
                    "UPDATE users SET color_hex = %s WHERE username = %s",
                    (color_hex, username),
                )
                row["COLOR_HEX"] = color_hex
            return {
                "username": row.get("USERNAME") or row.get("username"),
                "display_name": row.get("DISPLAY_NAME") or row.get("display_name"),
                "color_hex": row.get("COLOR_HEX") or row.get("color_hex"),
                "created_at": row.get("CREATED_AT") or row.get("created_at"),
            }
    created_at = datetime.utcnow()
    columns = ["username", "display_name", "color_hex", "created_at"]
    values: List[Any] = [username, display_name or username, color_hex, created_at]
    if has_name_col:
        columns.append("name")
        values.append(display_name or username)
    if has_initials_col:
        columns.append("initials")
        values.append((display_name or username)[:3])
    if has_is_active_col:
        columns.append("is_active")
        values.append(True)
    placeholders = ", ".join(["%s"] * len(columns))
    _execute(
        conn,
        f"INSERT INTO users ({', '.join(columns)}) VALUES ({placeholders})",
        tuple(values),
    )
    return {
        "username": username,
        "display_name": display_name or username,
        "color_hex": color_hex,
        "created_at": created_at,
    }


def delete_user(conn, username: str) -> None:
    username = (username or "").strip()
    if not username:
        return
    _execute(conn, "DELETE FROM users WHERE username = %s", (username,))


def update_user_color(conn, username: str, color_hex: Optional[str]) -> None:
    username = (username or "").strip()
    if not username:
        return
    _execute(conn, "UPDATE users SET color_hex = %s WHERE username = %s", (color_hex, username))


def list_usernames(conn) -> List[str]:
    with closing(conn.cursor()) as cur:
        cur.execute("SELECT username FROM users WHERE username IS NOT NULL ORDER BY username")
        return [row[0] for row in cur.fetchall()]


def list_users_with_colors(conn) -> List[Dict[str, Any]]:
    with closing(_dict_cursor(conn)) as cur:
        cur.execute("SELECT username, display_name, color_hex FROM users ORDER BY username")
        return _fetchall_dicts(cur)


def _tokenize_name(text: Optional[str], max_len: Optional[int], fallback: str) -> str:
    token = "".join(ch for ch in (text or "") if ch.isalnum()).upper()
    if not token:
        return fallback
    if max_len:
        return token[:max_len]
    return token


def _operator_initials(name: Optional[str]) -> str:
    if not name:
        return "OP"
    parts = [p for p in name.replace("-", " ").split() if p]
    if not parts:
        return "OP"
    return "".join(p[0] for p in parts[:2]).upper() or "OP"


def generate_thaw_id(conn, cell_line: Optional[str], operator: Optional[str], d: date) -> str:
    day = d.strftime("%Y%m%d")
    cell_token = _tokenize_name(cell_line, None, "CELL")
    op_token = _operator_initials(operator)
    prefix = f"TH-{day}-{cell_token}-{op_token}"
    with closing(conn.cursor()) as cur:
        cur.execute("SELECT COUNT(*) FROM logs WHERE thaw_id LIKE %s", (f"{prefix}%",))
        count = cur.fetchone()[0] or 0
    return f"{prefix}-{count + 1:02d}"


def insert_log(conn, payload: Dict[str, Any]) -> int:
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
        "cryo_storage_position",
        "created_by",
        "created_at",
    ]
    values = [payload.get(c) for c in cols]
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO logs ({', '.join(cols)}) VALUES ({placeholders})"
    with closing(conn.cursor()) as cur:
        cur.execute(sql, values)
    return 0


def query_logs(
    conn,
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
        where.append("created_by = %s")
        params.append(user)
    if event_type and event_type != "(any)":
        where.append("event_type = %s")
        params.append(event_type)
    if thaw_id:
        where.append("thaw_id = %s")
        params.append(thaw_id)
    if start_date:
        where.append("date >= %s")
        params.append(start_date)
    if end_date:
        where.append("date <= %s")
        params.append(end_date)
    if cell_line_contains:
        where.append("LOWER(cell_line) LIKE %s")
        params.append(f"%{cell_line_contains.lower()}%")
    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
    with closing(_dict_cursor(conn)) as cur:
        cur.execute(f"SELECT * FROM logs{where_sql} ORDER BY date ASC, created_at ASC", tuple(params))
        return _fetchall_dicts(cur)


def list_distinct_thaw_ids(conn) -> List[str]:
    with closing(conn.cursor()) as cur:
        cur.execute("SELECT DISTINCT thaw_id FROM logs WHERE thaw_id IS NOT NULL AND thaw_id <> '' ORDER BY thaw_id")
        return [row[0] for row in cur.fetchall()]


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


def get_ref_values(conn, kind: str) -> List[str]:
    table = _ref_table_for(kind)
    with closing(conn.cursor()) as cur:
        cur.execute(f"SELECT name FROM {table} ORDER BY name")
        return [row[0] for row in cur.fetchall()]


def add_ref_value(conn, kind: str, name: str) -> None:
    table = _ref_table_for(kind)
    now = datetime.utcnow()
    _execute(
        conn,
        f"""
        INSERT INTO {table} (name, created_at)
        SELECT %s, %s
        WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE name = %s)
        """,
        (name.strip(), now, name.strip()),
    )


def delete_ref_value(conn, kind: str, name: str) -> None:
    table = _ref_table_for(kind)
    _execute(conn, f"DELETE FROM {table} WHERE name = %s", (name.strip(),))


def list_entry_templates(conn) -> List[Dict[str, Any]]:
    with closing(_dict_cursor(conn)) as cur:
        cur.execute("SELECT name, payload, created_at FROM entry_templates ORDER BY name")
        return _fetchall_dicts(cur)


def save_entry_template(conn, name: str, payload: Dict[str, Any]) -> None:
    normalized = name.strip()
    if not normalized:
        raise ValueError("Template name required")
    payload_json = json.dumps(payload or {})
    with closing(conn.cursor()) as cur:
        cur.execute(
            """
            MERGE INTO entry_templates AS tgt
            USING (SELECT %s AS name, PARSE_JSON(%s) AS payload) AS src
            ON tgt.name = src.name
            WHEN MATCHED THEN UPDATE SET payload = src.payload, created_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (name, payload, created_at)
            VALUES (src.name, src.payload, CURRENT_TIMESTAMP())
            """,
            (normalized, payload_json),
        )


def delete_entry_template(conn, name: str) -> None:
    _execute(conn, "DELETE FROM entry_templates WHERE name = %s", (name.strip(),))


def rename_ref_value(conn, kind: str, old_name: str, new_name: str) -> None:
    table = _ref_table_for(kind)
    _execute(conn, f"UPDATE {table} SET name = %s WHERE name = %s", (new_name.strip(), old_name.strip()))


def backup_now(dest_root: Optional[str] = None) -> str:
    root = dest_root or os.path.join(os.path.dirname(__file__), "backups")
    os.makedirs(root, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(root, f"backup_{ts}")
    os.makedirs(out_dir, exist_ok=True)
    db_path = os.path.join(DATA_ROOT, "ipsc_tracker.db")
    if os.path.exists(db_path):
        shutil.copy2(db_path, os.path.join(out_dir, os.path.basename(db_path)))
    if os.path.isdir(IMAGES_DIR):
        img_out = os.path.join(out_dir, "images")
        os.makedirs(img_out, exist_ok=True)
        for root_dir, _, files in os.walk(IMAGES_DIR):
            rel = os.path.relpath(root_dir, IMAGES_DIR)
            target_dir = os.path.join(img_out, rel if rel != "." else "")
            os.makedirs(target_dir, exist_ok=True)
            for f in files:
                src = os.path.join(root_dir, f)
                dst = os.path.join(target_dir, f)
                try:
                    shutil.copy2(src, dst)
                except Exception:
                    pass
    return out_dir


def list_distinct_values(
    conn,
    column: str,
    *,
    cell_line: Optional[str] = None,
    limit: int = 10,
) -> List[str]:
    allowed = {
        "cell_line",
        "event_type",
        "vessel",
        "location",
        "medium",
        "cell_type",
        "operator",
        "assigned_to",
    }
    if column not in allowed:
        raise ValueError("Unsupported column for distinct values")
    where = f"WHERE {column} IS NOT NULL AND {column} <> ''"
    params: List[Any] = []
    if cell_line and column != "cell_line":
        where += " AND cell_line = %s"
        params.append(cell_line)
    sql = f"""
        SELECT {column}, COUNT(*) as cnt
        FROM logs
        {where}
        GROUP BY {column}
        ORDER BY cnt DESC
        LIMIT {limit}
    """
    with closing(conn.cursor()) as cur:
        cur.execute(sql, tuple(params))
        return [row[0] for row in cur.fetchall()]


def _fetch_single_row(conn, sql: str, params: Sequence[Any]) -> Optional[Dict[str, Any]]:
    with closing(_dict_cursor(conn)) as cur:
        cur.execute(sql, params)
        rows = _fetchall_dicts(cur)
        return rows[0] if rows else None


def get_last_log_for_cell_line(conn, cell_line: str) -> Optional[Dict[str, Any]]:
    return _fetch_single_row(
        conn,
        """
        SELECT * FROM logs
        WHERE cell_line = %s
        ORDER BY date DESC, created_at DESC
        LIMIT 1
        """,
        (cell_line,),
    )


def get_last_log_for_line_event(conn, cell_line: str, event_type: str) -> Optional[Dict[str, Any]]:
    return _fetch_single_row(
        conn,
        """
        SELECT * FROM logs
        WHERE cell_line = %s AND event_type = %s
        ORDER BY date DESC, created_at DESC
        LIMIT 1
        """,
        (cell_line, event_type),
    )


def get_recent_logs_for_cell_line(conn, cell_line: str, limit: int = 10) -> List[Dict[str, Any]]:
    with closing(_dict_cursor(conn)) as cur:
        cur.execute(
            """
            SELECT * FROM logs
            WHERE cell_line = %s
            ORDER BY date DESC, created_at DESC
            LIMIT %s
            """,
            (cell_line, limit),
        )
        return _fetchall_dicts(cur)


def get_latest_log_for_thaw(conn, thaw_id: str) -> Optional[Dict[str, Any]]:
    if not thaw_id:
        return None
    return _fetch_single_row(
        conn,
        """
        SELECT *
        FROM logs
        WHERE thaw_id = %s
        ORDER BY date DESC, created_at DESC
        LIMIT 1
        """,
        (thaw_id,),
    )


def predict_next_passage(conn, cell_line: str) -> Optional[int]:
    last = get_last_log_for_cell_line(conn, cell_line)
    if not last:
        return None
    try:
        passage = int(last.get("passage") or 0)
        return passage + 1 if passage > 0 else None
    except Exception:
        return None


def get_last_thaw_id(conn, cell_line: str) -> Optional[str]:
    row = _fetch_single_row(
        conn,
        """
        SELECT thaw_id
        FROM logs
        WHERE cell_line = %s AND event_type = 'Thawing' AND thaw_id IS NOT NULL
        ORDER BY date DESC, created_at DESC
        LIMIT 1
        """,
        (cell_line,),
    )
    thaw_id = row.get("thaw_id") if row else None
    if thaw_id:
        return thaw_id
    return None


def top_values(conn, column: str, *, cell_line: Optional[str] = None, limit: int = 3) -> List[str]:
    return list_distinct_values(conn, column, cell_line=cell_line, limit=limit)


def suggest_next_event(conn, cell_line: str) -> Optional[str]:
    last = get_last_log_for_cell_line(conn, cell_line)
    if not last:
        return None
    mapping = {
        "thawing": "Observation",
        "observation": "Media Change",
        "media change": "Observation",
        "split": "Observation",
        "cryopreservation": "Observation",
    }
    last_evt = (last.get("event_type") or "").lower()
    return mapping.get(last_evt)


def get_weekend_schedule(conn) -> List[Dict[str, Any]]:
    with closing(_dict_cursor(conn)) as cur:
        cur.execute(
            "SELECT date, assigned_to, notes, updated_at FROM weekend_schedule ORDER BY date DESC"
        )
        return _fetchall_dicts(cur)


def upsert_weekend_assignment(conn, dates: List[str], assigned_to: Optional[str], notes: Optional[str]) -> None:
    flags = _weekend_schedule_column_flags(conn)
    for date_str in dates:
        with closing(conn.cursor()) as cur:
            update_sets = ["assigned_to = %s", "notes = %s", "updated_at = CURRENT_TIMESTAMP()"]
            update_params: List[Any] = [assigned_to, notes]
            if flags.get("assignee"):
                update_sets.insert(1, "assignee = %s")
                update_params.insert(1, assigned_to or "")
            cur.execute(
                f"""
                UPDATE weekend_schedule
                SET {', '.join(update_sets)}
                WHERE date = %s
                """,
                (*update_params, date_str),
            )
            if cur.rowcount == 0:
                insert_cols = ["date"]
                insert_vals: List[Any] = [date_str]
                if flags.get("start_date"):
                    insert_cols.append("start_date")
                    insert_vals.append(date_str)
                if flags.get("end_date"):
                    insert_cols.append("end_date")
                    insert_vals.append(date_str)
                insert_cols.extend(["assigned_to", "notes"])
                insert_vals.extend([assigned_to, notes])
                if flags.get("assignee"):
                    insert_cols.append("assignee")
                    insert_vals.append(assigned_to or "")
                cols_sql = ", ".join(insert_cols + ["updated_at"])
                placeholders = ", ".join(["%s"] * len(insert_vals) + ["CURRENT_TIMESTAMP()"])
                cur.execute(
                    f"""
                    INSERT INTO weekend_schedule ({cols_sql})
                    VALUES ({placeholders})
                    """,
                    tuple(insert_vals),
                )


def delete_weekend_assignment(conn, date_str: str) -> None:
    _execute(conn, "DELETE FROM weekend_schedule WHERE date = %s", (date_str,))


def get_weekend_assignment_for_date(conn, target_date: date) -> Optional[str]:
    row = _fetch_single_row(
        conn,
        "SELECT assigned_to FROM weekend_schedule WHERE date = %s",
        (target_date,),
    )
    assigned = row.get("assigned_to") if row else None
    return assigned or None


def update_log_fields(conn, log_id: int, updates: Dict[str, Any]) -> None:
    if not updates:
        return
    fields = [f"{column} = %s" for column in updates.keys()]
    values = list(updates.values()) + [log_id]
    sql = f"UPDATE logs SET {', '.join(fields)} WHERE id = %s"
    _execute(conn, sql, values)


def bulk_update_logs(conn, changes: Iterable[Dict[str, Any]]) -> None:
    for payload in changes:
        log_id = payload.get("id")
        updates = {k: v for k, v in payload.items() if k != "id"}
        if log_id is None or not updates:
            continue
        update_log_fields(conn, int(log_id), updates)
