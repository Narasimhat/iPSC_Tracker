import os
import io
import statistics
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
import json
import pandas as pd
import streamlit as st
from PIL import Image

from db import (
    ensure_dirs,
    get_conn,
    init_db,
    insert_log,
    generate_thaw_id,
    query_logs,
    list_distinct_thaw_ids,
    list_distinct_values,
    get_last_log_for_cell_line,
    predict_next_passage,
    top_values,
    suggest_next_event,
    get_ref_values,
    add_ref_value,
    delete_ref_value,
    rename_ref_value,
    backup_now,
    get_last_log_for_line_event,
    get_recent_logs_for_cell_line,
    get_latest_log_for_thaw,
    get_last_thaw_id,
    get_weekend_schedule,
    upsert_weekend_assignment,
    delete_weekend_assignment,
    get_or_create_user,
    delete_user,
    update_user_color,
    list_usernames,
    list_users_with_colors,
    update_log_fields,
    bulk_update_logs,
    IMAGES_DIR,
    list_entry_templates,
    save_entry_template,
    delete_entry_template,
)

st.set_page_config(page_title="iPSC Culture Tracker", layout="wide")

st.title("🧬 iPSC Culture Tracker")
st.write("LIMS-style multi-user cell culture tracker with thaw-linked histories.")

st.markdown(
    """
    <style>
        div[data-testid="stForm"] {
            background: #ffffff;
            padding: 1.5rem 1.75rem;
            border-radius: 18px;
            border: 1px solid #e3e8f2;
            box-shadow: 0 10px 30px rgba(15, 30, 67, 0.07);
            margin-bottom: 2rem;
            max-width: 1100px;
            margin-left: auto;
            margin-right: auto;
        }
        div[data-testid="stForm"] h4 {
            margin-top: 1.2rem;
            color: #1f2a44;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 1rem;
        }
        .stTabs [data-baseweb="tab"] {
            padding: 0.5rem 1rem;
            border-radius: 999px;
            background: #eef1f8;
            color: #1f2a44;
            font-weight: 500;
        }
        .stTabs [aria-selected="true"] {
            background: #2d5bff !important;
            color: #fff !important;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# Initialize database and storage
@st.cache_resource(show_spinner=False)
def _get_connection():
    connection = get_conn()
    init_db(connection)
    ensure_dirs()
    return connection


conn = _get_connection()


@st.cache_data(ttl=60, show_spinner=False)
def _load_logs_cached(
    event_type: Optional[str] = None,
    thaw_id: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    cell_line_contains: Optional[str] = None,
) -> pd.DataFrame:
    rows = query_logs(
        conn,
        event_type=event_type,
        thaw_id=thaw_id,
        start_date=start_date,
        end_date=end_date,
        cell_line_contains=cell_line_contains,
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def _cached_ref_values(kind: str) -> List[str]:
    try:
        return get_ref_values(conn, kind)
    except Exception:
        return []


@st.cache_data(ttl=600, show_spinner=False)
def _cached_usernames() -> List[str]:
    try:
        return list_usernames(conn)
    except Exception:
        return []


@st.cache_data(ttl=600, show_spinner=False)
def _cached_user_rows() -> List[Dict[str, Optional[str]]]:
    try:
        return list_users_with_colors(conn)
    except Exception:
        return []


@st.cache_data(ttl=600, show_spinner=False)
def _cached_thaw_ids() -> List[str]:
    try:
        return list_distinct_thaw_ids(conn)
    except Exception:
        return []


@st.cache_data(ttl=300, show_spinner=False)
def _cached_weekend_rows() -> List[Dict[str, Optional[str]]]:
    try:
        return get_weekend_schedule(conn)
    except Exception:
        return []


def get_logs_df(
    *,
    event_type: Optional[str] = None,
    thaw_id: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    cell_line_contains: Optional[str] = None,
) -> pd.DataFrame:
    df = _load_logs_cached(
        event_type=event_type,
        thaw_id=thaw_id,
        start_date=start_date,
        end_date=end_date,
        cell_line_contains=cell_line_contains,
    )
    return df.copy()


def get_ref_values_cached(kind: str) -> List[str]:
    return list(_cached_ref_values(kind))


def get_usernames_cached() -> List[str]:
    return list(_cached_usernames())


def get_users_with_colors_cached() -> List[Dict[str, Optional[str]]]:
    return list(_cached_user_rows())


def get_thaw_ids_cached() -> List[str]:
    return list(_cached_thaw_ids())


def get_weekend_rows_cached() -> List[Dict[str, Optional[str]]]:
    return list(_cached_weekend_rows())


def _trigger_rerun() -> None:
    rerun_fn = getattr(st, "experimental_rerun", None) or getattr(st, "rerun", None)
    if rerun_fn:
        rerun_fn()


def get_cached_weekend_assignment(target_date: Optional[date]) -> Optional[str]:
    if not target_date:
        return None
    assignment = None
    for row in _cached_weekend_rows():
        row_date = row.get("date")
        if isinstance(row_date, str):
            try:
                row_date = datetime.fromisoformat(row_date).date()
            except ValueError:
                continue
        elif isinstance(row_date, datetime):
            row_date = row_date.date()
        if row_date == target_date:
            assignment = row.get("assigned_to")
            break
    if isinstance(assignment, str):
        assignment = assignment.strip() or None
    return assignment


def invalidate_logs_cache() -> None:
    _load_logs_cached.clear()
    _cached_thaw_ids.clear()


def invalidate_reference_cache() -> None:
    _cached_ref_values.clear()


def invalidate_user_cache() -> None:
    _cached_usernames.clear()
    _cached_user_rows.clear()


def invalidate_weekend_cache() -> None:
    _cached_weekend_rows.clear()

# Current user context (for 'Assigned to me' filters)
_usernames_all = get_usernames_cached()
COLOR_PALETTE = [
    "#4a90e2",
    "#7ed321",
    "#f5a623",
    "#d0021b",
    "#9013fe",
    "#50e3c2",
    "#b8e986",
    "#f8e71c",
    "#8b572a",
    "#417505",
]
DEFAULT_USER_COLOR = "#4a90e2"
ACTION_LABELS = [
    "Media Change",
    "Split",
    "Freeze",
    "Thaw",
    "Observation",
    "Cryopreservation",
    "Harvest",
    "QC Review",
    "Other",
]

try:
    _rows_colors = get_users_with_colors_cached()
    _user_colors = {}
    auto_idx = 0
    auto_color_assigned = False
    for row in _rows_colors:
        username = (row.get("username") or "").strip()
        color_hex = (row.get("color_hex") or "").strip()
        if color_hex:
            _user_colors[username] = color_hex
        else:
            auto_color = COLOR_PALETTE[auto_idx % len(COLOR_PALETTE)]
            auto_idx += 1
            update_user_color(conn, username, auto_color)
            auto_color_assigned = True
            _user_colors[username] = auto_color
    if auto_color_assigned:
        invalidate_user_cache()
except Exception:
    _user_colors = {}


def _normalize_user(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    text = str(name).strip()
    return text or None


def _with_alpha(color: str, alpha: float = 0.25) -> str:
    base = (color or DEFAULT_USER_COLOR).lstrip("#")
    if len(base) >= 6:
        try:
            r = int(base[0:2], 16)
            g = int(base[2:4], 16)
            b = int(base[4:6], 16)
            return f"rgba({r},{g},{b},{alpha})"
        except ValueError:
            pass
    return color or DEFAULT_USER_COLOR


def _color_for_user(username: Optional[str]) -> str:
    norm = _normalize_user(username)
    if not norm or norm == "(unassigned)":
        return "#dcdcdc"
    stored = _user_colors.get(norm)
    if stored and stored.strip():
        return stored.strip()
    return DEFAULT_USER_COLOR


def _apply_form_prefill(payload: Dict[str, Any]) -> None:
    if not payload:
        return
    field_map = {
        "cell_line": ["cell_line_select"],
        "event_type": ["event_type_select", "event_type_select_fallback"],
        "vessel": ["vessel_select", "vessel_text_input"],
        "location": ["location_select", "location_text_input"],
        "medium": ["medium_select", "medium_text_input"],
        "cell_type": ["cell_type_select", "cell_type_text_input"],
        "cryo_vial_position": ["cryo_vial_position_input"],
        "notes": ["notes_input"],
        "operator": ["operator_select"],
        "assigned_to": ["assigned_select"],
        "action_label": ["action_label_select"],
    }
    for source, keys in field_map.items():
        value = payload.get(source)
        if value in (None, ""):
            if source == "action_label":
                st.session_state["action_label_select"] = "(none)"
            continue
        for key in keys:
            st.session_state[key] = value
    if payload.get("passage"):
        try:
            st.session_state["passage_input"] = int(payload["passage"])
        except (TypeError, ValueError):
            pass
    volume_val = payload.get("volume")
    if volume_val not in (None, ""):
        try:
            st.session_state["volume_input"] = float(volume_val)
        except (TypeError, ValueError):
            pass
    nad_val = payload.get("next_action_date")
    if nad_val:
        st.session_state["next_action_date_input"] = pd.to_datetime(nad_val).date()
    notes = payload.get("notes")
    if notes not in (None, ""):
        st.session_state["notes_input"] = notes
    st.session_state["form_prefill_payload"] = payload


def _queue_form_prefill(payload: Dict[str, Any], meta: Optional[Dict[str, Any]] = None) -> None:
    st.session_state["pending_form_prefill"] = payload
    st.session_state["pending_form_prefill_meta"] = meta or {}


def _consume_pending_form_prefill() -> Optional[Dict[str, Any]]:
    payload = st.session_state.pop("pending_form_prefill", None)
    meta = st.session_state.pop("pending_form_prefill_meta", None)
    if payload:
        _apply_form_prefill(payload)
        if meta is not None:
            st.session_state["active_form_prefill_meta"] = meta
        return meta
    return None


def _clear_active_form_prefill(kind: Optional[str] = None) -> None:
    meta = st.session_state.get("active_form_prefill_meta")
    if meta and (kind is None or meta.get("kind") == kind):
        st.session_state.pop("active_form_prefill_meta", None)
    st.session_state.pop("form_prefill_payload", None)
    st.session_state.pop("pending_form_prefill", None)
    st.session_state.pop("pending_form_prefill_meta", None)


def _prefill_payload_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if not row:
        return payload
    for field in [
        "cell_line",
        "event_type",
        "passage",
        "vessel",
        "location",
        "medium",
        "cell_type",
        "volume",
        "notes",
        "operator",
        "assigned_to",
        "cryo_vial_position",
        "thaw_id",
        "action_label",
    ]:
        value = row.get(field)
        if value not in (None, ""):
            payload[field] = value
    nad = row.get("next_action_date")
    if nad:
        try:
            payload["next_action_date"] = pd.to_datetime(nad).date().isoformat()
        except Exception:
            payload["next_action_date"] = nad
    return payload


def _build_template_payload(
    *,
    cell_line: Optional[str],
    event_type: Optional[str],
    passage: Optional[int],
    vessel: Optional[str],
    location: Optional[str],
    medium: Optional[str],
    cell_type: Optional[str],
    volume: Optional[float],
    notes: Optional[str],
    operator: Optional[str],
    assigned_to: Optional[str],
    cryo_vial_position: Optional[str],
    next_action_date: Optional[date],
    action_label: Optional[str],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for key, value in [
        ("cell_line", cell_line),
        ("event_type", event_type),
        ("passage", passage),
        ("vessel", vessel),
        ("location", location),
        ("medium", medium),
        ("cell_type", cell_type),
        ("volume", volume),
        ("notes", notes),
        ("operator", operator),
        ("assigned_to", assigned_to),
        ("cryo_vial_position", cryo_vial_position),
        ("action_label", action_label),
    ]:
        if value not in (None, ""):
            payload[key] = value
    if next_action_date:
        payload["next_action_date"] = next_action_date.isoformat()
    return payload


def _as_payload_dict(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def _text_input_stateful(label: str, *, key: str, default: str = "", **kwargs):
    if key in st.session_state:
        return st.text_input(label, key=key, **kwargs)
    return st.text_input(label, value=default, key=key, **kwargs)
my_name = st.selectbox("My name", options=["(none)"] + _usernames_all if _usernames_all else ["(none)"], index=0, help="Used for 'Assigned to me' filters")
normalized_my_name = None if my_name == "(none)" else my_name
if st.session_state.get("my_name") != normalized_my_name:
    st.session_state["my_name"] = normalized_my_name
    st.session_state["operator_select"] = normalized_my_name or st.session_state.get("operator_select")
    if normalized_my_name:
        st.session_state["assigned_select"] = normalized_my_name

tab_add, tab_history, tab_thaw, tab_dashboard, tab_run, tab_scheduler, tab_settings = st.tabs([
    "Add Entry",
    "History",
    "Thaw Timeline",
    "Dashboard",
    "Weekend Run Sheet",
    "Weekend Scheduler",
    "Settings",
])

# ----------------------- Add Entry Tab -----------------------
with tab_add:
    st.subheader("📋 Add New Log Entry")
    WORKFLOW_TEMPLATES = {
        "Custom entry": {
            "description": "Start from scratch and optionally reuse a previous record.",
        },
        "Thaw & expand": {
            "event_type": "Thawing",
            "next_action_days": 2,
            "suggested_values": {
                "vessel": "T25 flask",
                "location": "Incubator A",
                "medium": "StemFlex",
                "cell_type": "iPSC",
            },
            "note_hint": "Record vial ID, split ratio, viability, and confluence checks.",
        },
        "Routine observation": {
            "event_type": "Observation",
            "next_action_days": 1,
            "note_hint": "Capture confluence %, morphology, and any action items.",
        },
        "Media refresh": {
            "event_type": "Media Change",
            "next_action_days": 2,
            "suggested_values": {
                "medium": "StemFlex",
            },
            "note_hint": "Log media lot #, supplements, and observed health before/after.",
        },
    }
    EVENT_FOLLOWUP_DEFAULTS = {
        "Observation": 1,
        "Media Change": 2,
        "Split": 1,
        "Thawing": 2,
        "Cryopreservation": 7,
    }
    template_choice = "Custom entry"
    template_cfg = WORKFLOW_TEMPLATES.get(template_choice, {})

    with st.form("add_entry_form", clear_on_submit=False):
        template_suggests = template_cfg.get("suggested_values", {})
        recent_history = []
        prev = None
        prefill_meta = _consume_pending_form_prefill() or st.session_state.get("active_form_prefill_meta")

        if prefill_meta and prefill_meta.get("label"):
            source_kind = prefill_meta.get("kind", "source")
            if source_kind == "template":
                st.caption(f"Template '{prefill_meta.get('label')}' applied to this form.")
            elif source_kind == "recent":
                st.caption(f"Copied values from log #{prefill_meta.get('label')}.")
            elif source_kind == "thaw":
                st.caption(f"Fields prefilled from thaw {prefill_meta.get('label')}.")

        id_col1, id_col2, id_col3, id_col4 = st.columns([1.7, 1.3, 1.1, 0.9])
        with id_col1:
            cl_values = get_ref_values_cached("cell_line")
            if cl_values:
                cell_line = st.selectbox("Cell Line ID *", options=cl_values, key="cell_line_select")
            else:
                cell_line = st.text_input("Cell Line ID *", value="", placeholder="e.g., BIHi005-A-24")
        with id_col2:
            evt_values = get_ref_values_cached("event_type")
            templ_evt = template_cfg.get("event_type") or ""
            if evt_values:
                idx_evt = evt_values.index(templ_evt) if templ_evt in evt_values else 0
                event_type = st.selectbox("Event Type *", options=evt_values, index=idx_evt, key="event_type_select")
            else:
                fallback_evts = ["Observation", "Media Change", "Split", "Thawing", "Cryopreservation", "Other"]
                idx_evt = fallback_evts.index(templ_evt) if templ_evt in fallback_evts else 0
                event_type = st.selectbox("Event Type *", options=fallback_evts, index=idx_evt, key="event_type_select_fallback")
        recent_history = get_recent_logs_for_cell_line(conn, cell_line, limit=20) if cell_line else []
        with id_col3:
            cryo_vial_position = template_suggests.get("cryo_vial_position", "")
            if not cryo_vial_position and recent_history:
                cryo_vial_position = recent_history[0].get("cryo_vial_position") or ""
            cryo_vial_position = _text_input_stateful(
                "Cryovial Position",
                default=cryo_vial_position,
                placeholder="e.g., Box A2, Row 3 Col 5",
                key="cryo_vial_position_input",
            )
        with id_col4:
            default_volume = 0.0
            if prev and prev.get("volume") is not None:
                try:
                    default_volume = float(prev.get("volume"))
                except Exception:
                    default_volume = 0.0
            volume = st.number_input("Volume (mL)", min_value=0.0, step=0.5, value=default_volume, key="volume_input")
        form_ready = bool(cell_line and event_type)

        prev = None

        split_auto = None
        row1_col1, row1_col2, row1_col3, row1_col4, row1_col5 = st.columns([1, 1, 1, 1, 1])
        with row1_col1:
            default_passage = 1
            if cell_line:
                pred = predict_next_passage(conn, cell_line)
                if pred:
                    default_passage = pred
                else:
                    last_for_line = get_last_log_for_cell_line(conn, cell_line)
                    if last_for_line and last_for_line.get("passage"):
                        try:
                            default_passage = int(last_for_line.get("passage"))
                        except Exception:
                            default_passage = 1
                if event_type == "Split":
                    last_for_line = get_last_log_for_cell_line(conn, cell_line)
                    if last_for_line and last_for_line.get("passage"):
                        try:
                            split_auto = int(last_for_line.get("passage")) + 1
                            default_passage = split_auto
                        except Exception:
                            split_auto = None
            passage_no = st.number_input("Passage No.", min_value=1, step=1, value=default_passage, key="passage_input")
            if split_auto:
                st.caption(f"Split detected → Passage auto-set to {split_auto}.")
        with row1_col2:
            vessel_refs = get_ref_values_cached("vessel")
            vessel_default = prev.get("vessel") if prev and prev.get("vessel") else template_suggests.get("vessel", "")
            if vessel_refs:
                v_idx = vessel_refs.index(vessel_default) if vessel_default in vessel_refs else 0
                vessel = st.selectbox("Vessel", options=vessel_refs, index=v_idx, key="vessel_select")
            else:
                vessel = _text_input_stateful(
                    "Vessel", default=vessel_default, placeholder="e.g., T25, 6-well plate", key="vessel_text_input"
                )
        with row1_col3:
            location_refs = get_ref_values_cached("location")
            default_location = prev.get("location") if prev and prev.get("location") else template_suggests.get("location", "")
            if location_refs:
                loc_idx = location_refs.index(default_location) if default_location in location_refs else 0
                location = st.selectbox("Location", options=location_refs, index=loc_idx, key="location_select")
            else:
                location = _text_input_stateful(
                    "Location",
                    default=default_location,
                    placeholder="e.g., Incubator A, Shelf 2",
                    key="location_text_input",
                )
        with row1_col4:
            _med_sugs = top_values(conn, "medium", cell_line=cell_line) if cell_line else top_values(conn, "medium")
            cm_refs = get_ref_values_cached("culture_medium")
            default_med = prev.get("medium") if prev and prev.get("medium") else template_suggests.get("medium", "")
            if cm_refs:
                med_idx = cm_refs.index(default_med) if default_med in cm_refs else 0
                medium = st.selectbox("Culture Medium", options=cm_refs, index=med_idx, key="medium_select")
            else:
                medium = _text_input_stateful(
                    "Culture Medium", default=default_med, placeholder="e.g., StemFlex", key="medium_text_input"
                )
            if _med_sugs:
                st.caption("Popular: " + ", ".join([str(x) for x in _med_sugs]))
        with row1_col5:
            _ct_sugs = top_values(conn, "cell_type", cell_line=cell_line) if cell_line else top_values(conn, "cell_type")
            ct_refs = get_ref_values_cached("cell_type")
            default_ct = prev.get("cell_type") if prev and prev.get("cell_type") else template_suggests.get("cell_type", "")
            if ct_refs:
                ct_idx = ct_refs.index(default_ct) if default_ct in ct_refs else 0
                cell_type = st.selectbox("Cell Type", options=ct_refs, index=ct_idx, key="cell_type_select")
            else:
                cell_type = _text_input_stateful(
                    "Cell Type",
                    default=default_ct,
                    placeholder="e.g., iPSC, NPC, cardiomyocyte",
                    key="cell_type_text_input",
                )
            if _ct_sugs:
                st.caption("Frequent: " + ", ".join([str(x) for x in _ct_sugs]))
        if cell_line and recent_history:
            prev_volumes = []
            for entry in recent_history:
                try:
                    if entry.get("volume") is not None:
                        prev_volumes.append(float(entry.get("volume")))
                except Exception:
                    continue
            if prev_volumes:
                median_vol = statistics.median(prev_volumes)
                if median_vol > 0 and abs(volume - median_vol) > median_vol:
                    st.warning(f"Volume differs from recent median ({median_vol:.1f} mL). Double-check before saving.")

        notes_placeholder = template_cfg.get("note_hint", "Observations, QC checks, follow-ups…")
        notes = st.text_area("Notes", placeholder=notes_placeholder, height=80, key="notes_input")

        st.divider()
        sched_col1, sched_col2, sched_col3, sched_col4, sched_col5 = st.columns([1, 1, 1, 1, 1])
        with sched_col1:
            usernames = get_usernames_cached()
            if usernames:
                op_index = 0
                if st.session_state.get("my_name") and st.session_state["my_name"] in usernames:
                    op_index = usernames.index(st.session_state["my_name"])
                operator = st.selectbox("Operator *", options=usernames, index=op_index, key="operator_select")
            else:
                st.info("No operators yet. Add some under Settings → Operators.")
                operator = _text_input_stateful("Operator *", default="", placeholder="Your name", key="operator_text_input")
        with sched_col2:
            log_date = st.date_input("Date *", value=date.today())
        default_nad = None
        if template_cfg.get("next_action_days") is not None:
            default_nad = date.today() + timedelta(days=template_cfg["next_action_days"])
        elif EVENT_FOLLOWUP_DEFAULTS.get(event_type) is not None:
            default_nad = date.today() + timedelta(days=EVENT_FOLLOWUP_DEFAULTS[event_type])

        with sched_col3:
            next_action_date = st.date_input("Next Action Date", value=default_nad, key="next_action_date_input")
        with sched_col4:
            combined = st.columns([2, 3])
            with combined[0]:
                all_users = get_usernames_cached()
                assigned_options = ["(unassigned)"] + all_users if all_users else ["(unassigned)"]
                weekend_autofill = None
                if next_action_date:
                    weekend_autofill = get_cached_weekend_assignment(next_action_date)
                assign_index = 0
                if weekend_autofill and weekend_autofill in assigned_options:
                    assign_index = assigned_options.index(weekend_autofill)
                elif st.session_state.get("my_name") and st.session_state["my_name"] in assigned_options:
                    assign_index = assigned_options.index(st.session_state["my_name"])
                assigned_to = st.selectbox("Assigned To", options=assigned_options, index=assign_index, key="assigned_select")
                if weekend_autofill:
                    st.caption(f"Weekend duty auto-selected: {weekend_autofill}")
            with combined[1]:
                action_label_choice = st.selectbox(
                    "Action Label",
                    options=["(none)"] + ACTION_LABELS,
                    index=0,
                    key="action_label_select",
                    help="Categorize the follow-up action.",
                )
        with sched_col5:
            st.empty()
        thaw_preview = ""
        linked_thaw_id = ""
        latest_thaw_for_line = get_last_thaw_id(conn, cell_line) if cell_line else None
        if event_type == "Thawing":
            _clear_active_form_prefill(kind="thaw")
            if cell_line and operator:
                thaw_preview = generate_thaw_id(conn, cell_line, operator, log_date)
                thaw_label = thaw_preview
            else:
                thaw_label = "Select Cell Line + Operator"
            st.text_input("Thaw ID", value=thaw_label, disabled=True, help="Auto-generated when saving.")
        else:
            thaw_ids = get_thaw_ids_cached()
            options = ["(none)"] + thaw_ids if thaw_ids else ["(none)"]
            idx = 0
            if latest_thaw_for_line and latest_thaw_for_line in thaw_ids:
                idx = options.index(latest_thaw_for_line)
            linked_thaw_id = st.selectbox(
                "Link Thaw ID",
                options=options,
                index=idx,
                help="Associate with an existing thaw event (required for follow-ups).",
                key="linked_thaw_select",
            )
            active_meta = st.session_state.get("active_form_prefill_meta") or {}
            active_thaw_id = active_meta.get("label") if active_meta.get("kind") == "thaw" else None
            if linked_thaw_id and linked_thaw_id not in ("(none)",):
                if active_thaw_id == linked_thaw_id:
                    st.caption(f"Fields prefilled from thaw {linked_thaw_id}.")
                else:
                    latest_record = get_latest_log_for_thaw(conn, linked_thaw_id)
                    if latest_record:
                        _queue_form_prefill(latest_record, meta={"kind": "thaw", "label": linked_thaw_id})
                        _trigger_rerun()
                    else:
                        st.info("No prior entries found for this Thaw ID to copy.")
            else:
                _clear_active_form_prefill(kind="thaw")

        submitted = st.form_submit_button("Save Entry", disabled=not form_ready)
        if submitted:
            missing_labels = []
            def _is_blank(val):
                return val is None or (isinstance(val, str) and not val.strip())
            if _is_blank(cell_line):
                missing_labels.append("Cell Line")
            if _is_blank(vessel):
                missing_labels.append("Vessel")
            if _is_blank(location):
                missing_labels.append("Location")
            if _is_blank(medium):
                missing_labels.append("Culture Medium")
            if _is_blank(cell_type):
                missing_labels.append("Cell Type")
            if event_type == "Thawing" and _is_blank(cryo_vial_position):
                missing_labels.append("Cryo Vial Position")
            if event_type != "Thawing":
                if not linked_thaw_id or linked_thaw_id == "(none)":
                    missing_labels.append("Linked Thaw ID")
            if _is_blank(operator):
                missing_labels.append("Operator")
            if missing_labels:
                st.error(f"Please fill required fields: {', '.join(missing_labels)}.")
                st.stop()
            if next_action_date and next_action_date < date.today():
                st.error("Next Action Date cannot be in the past.")
                st.stop()
            thaw_id_val = ""
            if event_type == "Thawing":
                thaw_id_val = generate_thaw_id(conn, cell_line, operator, log_date)
            else:
                thaw_id_val = linked_thaw_id if linked_thaw_id and linked_thaw_id != "(none)" else ""

            final_passage = int(passage_no) if passage_no else None
            if event_type == "Split" and split_auto:
                final_passage = split_auto

            resolved_assignee = None if assigned_to in (None, "(unassigned)") else assigned_to
            auto_assignee_note = None
            if (not resolved_assignee) and next_action_date:
                weekend_owner = get_cached_weekend_assignment(next_action_date)
                if weekend_owner:
                    resolved_assignee = weekend_owner
                    auto_assignee_note = weekend_owner

            resolved_action_label = None if action_label_choice in (None, "(none)") else action_label_choice

            payload = {
                "date": log_date.isoformat(),
                "cell_line": cell_line,
                "event_type": event_type,
                "action_label": resolved_action_label,
                "passage": final_passage,
                "vessel": vessel,
                "location": location,
                "medium": medium,
                "cell_type": cell_type,
                "volume": float(volume) if volume is not None else None,
                "notes": notes,
                "operator": operator,
                "thaw_id": thaw_id_val,
                "cryo_vial_position": cryo_vial_position,
                "image_path": None,
                "assigned_to": resolved_assignee,
                "next_action_date": next_action_date.isoformat() if next_action_date else None,
                "created_by": operator,
                "created_at": datetime.utcnow().isoformat(),
            }
            insert_log(conn, payload)
            invalidate_logs_cache()
            st.success("✅ Log entry saved to database!")
            if auto_assignee_note:
                st.info(f"Assigned to weekend duty: {auto_assignee_note}")

    reuse_history = recent_history if 'recent_history' in locals() else []
    template_rows = list_entry_templates(conn)
    template_map = {row.get("name"): _as_payload_dict(row.get("payload")) for row in template_rows if row.get("name")}
    template_names = sorted(template_map.keys())
    with st.expander("Reuse previous entry or templates", expanded=False):
        col_recent, col_templates = st.columns(2)
        with col_recent:
            st.markdown("**Copy a recent entry**")
            if reuse_history:
                recent_map: Dict[str, Dict[str, Any]] = {}
                option_keys: List[str] = []
                for entry in reuse_history:
                    entry_id = str(entry.get("id") or f"row-{len(option_keys)}")
                    recent_map[entry_id] = entry
                    option_keys.append(entry_id)
                selected_recent = st.selectbox(
                    "Recent entries for this cell line",
                    options=["(none)"] + option_keys,
                    format_func=lambda opt: "Choose entry"
                    if opt == "(none)"
                    else " · ".join(
                        [
                            f"#{recent_map[opt].get('id', '?')}",
                            str(recent_map[opt].get("date") or "?"),
                            str(recent_map[opt].get("event_type") or "?"),
                        ]
                    ),
                    key="prefill_recent_select",
                )
                if st.button(
                    "Copy selected entry",
                    key="prefill_recent_btn",
                    disabled=selected_recent == "(none)",
                ):
                    payload = _prefill_payload_from_row(recent_map[selected_recent])
                    _queue_form_prefill(payload, meta={"kind": "recent", "label": selected_recent})
                    _trigger_rerun()
            else:
                st.info("Select a cell line with prior logs to enable copying.")
        with col_templates:
            st.markdown("**Templates**")
            template_choice = st.selectbox(
                "Load template",
                options=["(none)"] + template_names,
                key="template_load_select",
            )
            if st.button(
                "Load template values",
                key="template_load_btn",
                disabled=template_choice == "(none)",
            ):
                payload = _as_payload_dict(template_map.get(template_choice))
                if payload:
                    _queue_form_prefill(payload, meta={"kind": "template", "label": template_choice})
                    _trigger_rerun()
                else:
                    st.warning("Template payload unavailable.")

            template_name_input = st.text_input("Template name", key="template_name_input")
            if st.button("Save current form as template", key="template_save_btn"):
                if not template_name_input or not template_name_input.strip():
                    st.warning("Enter a template name.")
                else:
                    template_payload = _build_template_payload(
                        cell_line=cell_line,
                        event_type=event_type,
                        passage=int(passage_no) if passage_no else None,
                        vessel=vessel,
                        location=location,
                        medium=medium,
                        cell_type=cell_type,
                        volume=float(volume) if volume is not None else None,
                        notes=notes,
                        operator=operator,
                        assigned_to=None if assigned_to in (None, "(unassigned)") else assigned_to,
                        cryo_vial_position=cryo_vial_position,
                        next_action_date=next_action_date,
                        action_label=resolved_action_label,
                    )
                    save_entry_template(conn, template_name_input.strip(), template_payload)
                    st.success("Template saved.")
                    _trigger_rerun()

            if template_names:
                delete_choice = st.selectbox(
                    "Delete template",
                    options=["(none)"] + template_names,
                    key="template_delete_select",
                )
                if st.button(
                    "Delete selected template",
                    key="template_delete_btn",
                    disabled=delete_choice == "(none)",
                ):
                    delete_entry_template(conn, delete_choice)
                    st.success("Template deleted.")
                    _trigger_rerun()
            else:
                st.caption("No templates saved yet.")

with tab_history:
    st.subheader("📜 Culture History")
    fcol1, fcol2 = st.columns([2, 1])
    with fcol1:
        f_cell = st.text_input("Cell line contains", "")
    with fcol2:
        f_assigned = st.text_input("Assigned To contains", "")
    fcol3, fcol4, fcol5 = st.columns(3)
    with fcol3:
        f_event = st.selectbox("Event Type", ["(any)", "Observation", "Media Change", "Split", "Thawing", "Cryopreservation", "Other"])
    with fcol4:
        operator_opts = ["(any)"] + _usernames_all if _usernames_all else ["(any)"]
        f_operator = st.selectbox("Operator", operator_opts)
    with fcol5:
        date_filter = st.selectbox("Date range", ["All", "Today", "Last 7 days", "Last 30 days"])
    only_mine = st.checkbox("Assigned to me only", value=False)

    event_filter = None if f_event == "(any)" else f_event
    today_value = date.today()
    start_range = None
    end_range = None
    if date_filter == "Today":
        start_range = today_value
        end_range = today_value
    elif date_filter == "Last 7 days":
        start_range = today_value - timedelta(days=6)
        end_range = today_value
    elif date_filter == "Last 30 days":
        start_range = today_value - timedelta(days=29)
        end_range = today_value

    df = get_logs_df(
        event_type=event_filter,
        start_date=start_range,
        end_date=end_range,
        cell_line_contains=f_cell or None,
    )

    if not df.empty:
        if f_assigned:
            df = df[df.get("assigned_to", "").astype(str).str.contains(f_assigned, case=False, na=False)]
        if f_operator != "(any)" and "operator" in df.columns:
            df = df[df["operator"] == f_operator]
        if only_mine and st.session_state.get("my_name"):
            df = df[df.get("assigned_to", "").astype(str) == st.session_state["my_name"]]
        elif only_mine and not st.session_state.get("my_name"):
            st.info("Set 'My name' at the top to enable 'Assigned to me'.")
        display_cols = [
            "date",
            "cell_line",
            "event_type",
            "action_label",
            "passage",
            "vessel",
            "location",
            "medium",
            "cell_type",
            "volume",
            "notes",
            "operator",
            "thaw_id",
            "cryo_vial_position",
            "assigned_to",
            "next_action_date",
            "created_by",
        ]
        for c in display_cols:
            if c not in df.columns:
                df[c] = ""
        if "created_at" not in df.columns:
            df["created_at"] = ""
        pretty = df.sort_values(by=["date", "created_at"], ascending=False, ignore_index=True)[["id"] + display_cols]
        pretty["date"] = pd.to_datetime(pretty["date"], errors="coerce")
        pretty["next_action_date"] = pd.to_datetime(pretty["next_action_date"], errors="coerce")
        pretty["assigned_color"] = pretty["assigned_to"].apply(_color_for_user)
        history_display = pretty.rename(columns={
            "id": "ID",
            "date": "Date",
            "cell_line": "Cell Line",
            "event_type": "Event Type",
            "action_label": "Action Label",
            "passage": "Passage",
            "vessel": "Vessel",
            "location": "Location",
            "medium": "Culture Medium",
            "cell_type": "Cell Type",
            "volume": "Volume (mL)",
            "notes": "Notes",
            "operator": "Operator",
            "thaw_id": "Thaw ID",
            "cryo_vial_position": "Cryo Vial Position",
            "assigned_to": "Assigned To",
            "next_action_date": "Next Action Date",
            "created_by": "Created By",
            "assigned_color": "Assigned Color",
        })
        history_display["Mark Done"] = history_display["Next Action Date"].isna()
        history_display["Assigned Color"] = history_display["Assigned Color"].astype(str)
        color_series = history_display.set_index("ID")["Assigned Color"]
        history_view = history_display.drop(columns=["Assigned Color"]).set_index("ID")
        styled_history = history_view.style.apply(
            lambda row: [f"background-color: {_with_alpha(color_series.loc[row.name], 0.25)}"] * len(row),
            axis=1,
        )
        st.dataframe(styled_history, use_container_width=True, hide_index=True)

        with st.expander("Edit or mark done"):
            edited_history = st.data_editor(
                history_display.drop(columns=["Assigned Color"]),
                column_config={
                    "ID": st.column_config.Column(disabled=True),
                    "Date": st.column_config.DateColumn(),
                    "Next Action Date": st.column_config.DateColumn(),
                    "Assigned To": st.column_config.SelectboxColumn(options=["(unassigned)"] + _usernames_all if _usernames_all else ["(unassigned)"]),
                    "Action Label": st.column_config.SelectboxColumn(options=["(none)"] + ACTION_LABELS),
                    "Passage": st.column_config.NumberColumn(step=1),
                    "Volume (mL)": st.column_config.NumberColumn(step=0.5),
                    "Mark Done": st.column_config.CheckboxColumn(),
                },
                hide_index=True,
                use_container_width=True,
            )
            if st.button("Save history edits", key="save_history"):
                try:
                    base_lookup = history_display.set_index("ID")
                    pending_updates = []
                    for _, row in edited_history.iterrows():
                        row_id = row["ID"]
                        original = base_lookup.loc[row_id]
                        updates = {}
                        for col, orig_col in [
                            ("Date","date"),
                            ("Cell Line","cell_line"),
                            ("Event Type","event_type"),
                            ("Action Label","action_label"),
                            ("Passage","passage"),
                            ("Vessel","vessel"),
                            ("Location","location"),
                            ("Culture Medium","medium"),
                            ("Cell Type","cell_type"),
                            ("Volume (mL)","volume"),
                            ("Notes","notes"),
                            ("Operator","operator"),
                            ("Thaw ID","thaw_id"),
                            ("Cryo Vial Position","cryo_vial_position"),
                            ("Assigned To","assigned_to"),
                            ("Next Action Date","next_action_date"),
                        ]:
                            val = row[col]
                            if col in ("Assigned To",) and (val in (None,"(unassigned)")):
                                val = None
                            if col == "Action Label" and (val in (None, "(none)")):
                                val = None
                            if col in ("Date","Next Action Date") and pd.notna(val):
                                val = pd.to_datetime(val).date().isoformat()
                            if col in ("Date","Next Action Date") and pd.isna(val):
                                val = None
                            orig_val = original[col]
                            if col in ("Date","Next Action Date") and isinstance(orig_val, pd.Timestamp):
                                orig_val = orig_val.date().isoformat()
                            if orig_val != val:
                                updates[orig_col] = val
                        mark_done_flag = bool(row.get("Mark Done"))
                        orig_next = original["Next Action Date"]
                        if mark_done_flag and pd.notna(orig_next):
                            updates["next_action_date"] = None
                        if updates:
                            pending_updates.append((row_id, updates))
                    for row_id, updates in pending_updates:
                        update_log_fields(conn, int(row_id), updates)
                    if pending_updates:
                        invalidate_logs_cache()
                    st.success("History updated.")
                except Exception as exc:
                    st.error(f"Failed to save history updates: {exc}")
            csv = edited_history.to_csv(index=False).encode('utf-8')
            st.download_button("📂 Download CSV", data=csv, file_name="ipsc_culture_log.csv", mime="text/csv")

        if not history_display.empty:
            summary_df = history_display.copy()
            summary_df["Volume (mL)"] = pd.to_numeric(summary_df["Volume (mL)"], errors="coerce").fillna(0.0)
            media_summary = (
                summary_df.groupby("Culture Medium", as_index=False)["Volume (mL)"]
                .sum()
                .rename(columns={"Culture Medium": "Media", "Volume (mL)": "Total Volume (mL)"})
                .sort_values("Total Volume (mL)", ascending=False)
            )
            st.markdown("#### Media prep summary (history view)")
            if media_summary.empty:
                st.info("No volume data to summarize.")
            else:
                st.dataframe(media_summary, use_container_width=True)

        st.markdown("---")
        with st.expander("📓 Lab book export"):
            export_date = st.date_input("Day to summarize", value=date.today(), key="lab_book_date")
            operator_opts = ["(any)"] + _usernames_all if _usernames_all else ["(any)"]
            export_operator = st.selectbox("Operator filter", operator_opts, key="lab_book_operator")
            export_df = get_logs_df(start_date=export_date, end_date=export_date)
            if export_df.empty:
                st.info("No entries for that date.")
            else:
                if export_operator != "(any)":
                    export_df = export_df[export_df.get("operator") == export_operator]
                if export_df.empty:
                    st.info("No entries match the selected operator.")
                else:
                    export_df["_nad"] = pd.to_datetime(export_df.get("next_action_date"), errors="coerce")
                    done_df = export_df[export_df["_nad"].isna()].copy()
                    if done_df.empty:
                        st.info("Nothing marked done for that day.")
                    else:
                        done_df["_created"] = pd.to_datetime(done_df.get("created_at"), errors="coerce")
                        done_df = done_df.sort_values(["_created", "date"], ascending=True)
                        lines = []
                        for _, row in done_df.iterrows():
                            line = f"- {row.get('cell_line','?')} • {row.get('event_type','?')}"
                            if row.get("passage"):
                                line += f" (P{row.get('passage')})"
                            if row.get("location"):
                                line += f" @ {row.get('location')}"
                            if row.get("medium"):
                                line += f" | {row.get('medium')}"
                            line += f" — by {row.get('operator') or 'unknown'}"
                            if row.get("notes"):
                                line += f" | Notes: {row.get('notes')}"
                            lines.append(line)
                        lab_text = "\n".join(lines)
                        st.caption("Copy the summary below into the lab book:")
                        st.code(lab_text or "(no entries)", language="markdown")
                        st.download_button(
                            "Download lab book text",
                            data=(lab_text or "").encode("utf-8"),
                            file_name=f"lab_book_{export_date.isoformat()}.txt",
                            mime="text/plain",
                        )
    else:
        st.info("No entries yet — add your first log in Add Entry tab.")

with tab_thaw:
    st.subheader("🧊 Thaw Event Timeline")
    thaw_ids_list = get_thaw_ids_cached()
    selected_tid = st.selectbox("Select Thaw ID", options=["(choose)"] + thaw_ids_list if thaw_ids_list else ["(none)"])
    if thaw_ids_list and selected_tid not in ("(choose)", "(none)"):
        timeline = get_logs_df(thaw_id=selected_tid)
        if not timeline.empty:
            timeline = timeline.sort_values(by=["date"]).reset_index(drop=True)
            tcols = [
                "date",
                "cell_line",
                "event_type",
                "passage",
                "vessel",
                "location",
                "medium",
                "cell_type",
                "volume",
                "notes",
                "operator",
                "cryo_vial_position",
                "created_by",
            ]
            for c in tcols:
                if c not in timeline.columns:
                    timeline[c] = ""
            st.dataframe(timeline[tcols].rename(columns={
                "date": "Date",
                "cell_line": "Cell Line",
                "event_type": "Event Type",
                "passage": "Passage",
                "vessel": "Vessel",
                "location": "Location",
                "medium": "Culture Medium",
                "cell_type": "Cell Type",
                "volume": "Volume (mL)",
                "notes": "Notes",
                "operator": "Operator",
                "cryo_vial_position": "Cryovial Position",
                "created_by": "Created By",
            }), width='stretch')
        else:
            st.info("No records for this Thaw ID yet.")

with tab_dashboard:
    st.subheader("📅 Team Dashboard")
    dash_only_mine = st.checkbox("Show only items assigned to me", value=False)
    df_all = get_logs_df()
    if df_all.empty:
        st.info("No entries yet — add a log to unlock the dashboard.")
    else:
        if "assigned_to" not in df_all.columns:
            df_all["assigned_to"] = ""
        if "next_action_date" not in df_all.columns:
            df_all["next_action_date"] = None
        if "action_label" not in df_all.columns:
            df_all["action_label"] = ""
        if "created_at" not in df_all.columns:
            df_all["created_at"] = ""
        today_dt = pd.to_datetime(date.today())
        df_all["_date"] = pd.to_datetime(df_all["date"], errors="coerce")
        df_all["_nad"] = pd.to_datetime(df_all["next_action_date"], errors="coerce")

        logs_today = df_all[df_all["_date"] == today_dt]
        logs_yesterday = df_all[df_all["_date"] == (today_dt - pd.Timedelta(days=1))]
        active_window = today_dt - pd.Timedelta(days=6)
        active_cell_lines = df_all[df_all["_date"] >= active_window]["cell_line"].nunique()
        overdue_total = int(((~df_all["_nad"].isna()) & (df_all["_nad"] < today_dt)).sum())

        stats = st.columns(3)
        stats[0].metric("Logs today", int(len(logs_today)), delta=int(len(logs_today) - len(logs_yesterday)))
        stats[1].metric("Active cell lines (7d)", int(active_cell_lines))
        stats[2].metric("Overdue actions", overdue_total)

        upcoming_weekend = date.today() + timedelta((5 - date.today().weekday()) % 7)
        weekend_dates = [upcoming_weekend, upcoming_weekend + timedelta(days=1)]
        coverage = []
        for d in weekend_dates:
            coverage.append(
                {
                    "date": d,
                    "assignee": get_cached_weekend_assignment(d),
                }
            )
        coverage_lines = "\n".join(
            f"{item['date'].strftime('%a %b %d')}: {item['assignee'] or 'Unassigned'}" for item in coverage
        )
        st.info(f"Upcoming weekend coverage:\n{coverage_lines}", icon="🗓️")

        view_df = df_all.copy()
        if dash_only_mine:
            if st.session_state.get("my_name"):
                view_df = view_df[view_df.get("assigned_to", "").astype(str) == st.session_state["my_name"]]
            else:
                st.info("Set 'My name' at the top to filter to your items.")

        actions_df = view_df[~view_df["_nad"].isna()]
        df_overdue = actions_df[actions_df["_nad"] < today_dt].sort_values("_nad")
        df_upcoming = actions_df[actions_df["_nad"] >= today_dt].sort_values("_nad").head(50)

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Overdue**")
            if df_overdue.empty:
                st.info("No overdue items.")
            else:
                st.dataframe(
                    df_overdue[["cell_line", "event_type", "action_label", "assigned_to", "next_action_date", "notes"]].rename(columns={
                        "cell_line": "Cell Line",
                        "event_type": "Event Type",
                        "action_label": "Action Label",
                        "assigned_to": "Assigned To",
                        "next_action_date": "Next Action Date",
                        "notes": "Notes",
                    }),
                    use_container_width=True,
                )
        with c2:
            st.markdown("**Upcoming**")
            if df_upcoming.empty:
                st.info("No upcoming items.")
            else:
                st.dataframe(
                    df_upcoming[["cell_line", "event_type", "action_label", "assigned_to", "next_action_date", "notes"]].rename(columns={
                        "cell_line": "Cell Line",
                        "event_type": "Event Type",
                        "action_label": "Action Label",
                        "assigned_to": "Assigned To",
                        "next_action_date": "Next Action Date",
                        "notes": "Notes",
                    }),
                    use_container_width=True,
                )

        my_name = st.session_state.get("my_name")
        if my_name:
            my_queue = df_all[(df_all.get("assigned_to", "").astype(str) == my_name) & (~df_all["_nad"].isna())].sort_values("_nad")
            if not my_queue.empty:
                st.markdown("**My queue**")
                st.dataframe(
                    my_queue[["cell_line", "event_type", "action_label", "next_action_date", "notes"]].rename(columns={
                        "cell_line": "Cell Line",
                        "event_type": "Event Type",
                        "action_label": "Action Label",
                        "next_action_date": "Next Action Date",
                        "notes": "Notes",
                    }),
                    use_container_width=True,
                )

        assigned_series = actions_df["assigned_to"]
        unassigned = actions_df[assigned_series.isna() | (assigned_series == "")]
        if not unassigned.empty:
            st.markdown("**Needs owner**")
            st.dataframe(
                unassigned[["cell_line", "event_type", "action_label", "next_action_date", "notes"]].rename(columns={
                    "cell_line": "Cell Line",
                    "event_type": "Event Type",
                    "action_label": "Action Label",
                    "next_action_date": "Next Action Date",
                    "notes": "Notes",
                }),
                use_container_width=True,
            )

        st.markdown("**Recent team activity**")
        recent = df_all.sort_values(by="created_at", ascending=False).head(12).copy()
        if not recent.empty:
            recent["_created_at"] = pd.to_datetime(recent["created_at"], errors="coerce")
            recent["Logged"] = recent["_created_at"].dt.strftime("%Y-%m-%d %H:%M")
            st.dataframe(
                recent[["Logged", "cell_line", "event_type", "action_label", "operator", "assigned_to", "notes"]].rename(columns={
                    "cell_line": "Cell Line",
                    "event_type": "Event Type",
                    "action_label": "Action Label",
                    "operator": "Operator",
                    "assigned_to": "Assigned To",
                    "notes": "Notes",
                }),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No activity yet.")

with tab_run:
    st.subheader("🧪 Weekend Run Sheet")
    df_tasks = get_logs_df()
    if df_tasks.empty:
        st.info("No tasks yet.")
    else:
        ensure_cols = {
            "assigned_to": "",
            "next_action_date": None,
            "action_label": "",
            "medium": "",
            "location": "",
            "event_type": "",
            "cell_type": "",
            "vessel": "",
            "volume": 0.0,
            "notes": "",
        }
        for col_name, default in ensure_cols.items():
            if col_name not in df_tasks.columns:
                df_tasks[col_name] = default
        df_tasks["next_action_date"] = pd.to_datetime(df_tasks["next_action_date"], errors="coerce")
        today_start = pd.Timestamp(date.today())
        tomorrow_start = today_start + pd.Timedelta(days=1)
        day_after_tomorrow = tomorrow_start + pd.Timedelta(days=1)
        df_tasks = df_tasks[
            (~df_tasks["next_action_date"].isna())
            & (df_tasks["next_action_date"] >= today_start)
        ]
        my_user = st.session_state.get("my_name")
        default_assignee = my_user if my_user else "(any)"
        assignee_filter = st.selectbox(
            "Assigned To",
            options=["(any)", "(me)"] + sorted(set(df_tasks["assigned_to"].dropna().unique())),
            index=1 if my_user else 0,
        )
        def matches_assignee(row_name: str) -> bool:
            if assignee_filter == "(any)":
                return True
            if assignee_filter == "(me)":
                if not my_user:
                    st.info("Set 'My name' at the top to use '(me)' filter.")
                    return True
                return str(row_name) == my_user
            return str(row_name) == assignee_filter

        df_tasks = df_tasks[df_tasks["assigned_to"].apply(matches_assignee)]

        media_options = sorted(df_tasks["medium"].dropna().unique()) if "medium" in df_tasks else []
        location_options = sorted(df_tasks["location"].dropna().unique()) if "location" in df_tasks else []
        event_options = sorted(df_tasks["event_type"].dropna().unique()) if "event_type" in df_tasks else []
        media_filter = st.multiselect("Filter by medium", options=media_options)
        location_filter = st.multiselect("Filter by location", options=location_options)
        event_filter = st.multiselect("Filter by event type", options=event_options)
        date_filter = st.selectbox("Show", ["All", "Today", "Tomorrow", "Today + Tomorrow"])
        if media_filter:
            df_tasks = df_tasks[df_tasks["medium"].isin(media_filter)]
        if location_filter:
            df_tasks = df_tasks[df_tasks["location"].isin(location_filter)]
        if event_filter:
            df_tasks = df_tasks[df_tasks["event_type"].isin(event_filter)]
        if date_filter == "Today":
            df_tasks = df_tasks[
                (df_tasks["next_action_date"] >= today_start)
                & (df_tasks["next_action_date"] < tomorrow_start)
            ]
        elif date_filter == "Tomorrow":
            df_tasks = df_tasks[
                (df_tasks["next_action_date"] >= tomorrow_start)
                & (df_tasks["next_action_date"] < day_after_tomorrow)
            ]
        elif date_filter == "Today + Tomorrow":
            df_tasks = df_tasks[
                (df_tasks["next_action_date"] >= today_start)
                & (df_tasks["next_action_date"] < day_after_tomorrow)
            ]

        if df_tasks.empty:
            st.info("No tasks match the selected filters.")
        else:
            df_tasks["days_to_due"] = (df_tasks["next_action_date"] - today_start).dt.days
            df_tasks = df_tasks.sort_values(by="next_action_date")
            df_tasks["Location"] = df_tasks["location"].fillna("")
            df_tasks["Medium"] = df_tasks["medium"].fillna("")
            df_tasks["done"] = df_tasks["next_action_date"] < today_start
            run_cols_display = df_tasks[["id","cell_line","event_type","action_label","done","vessel","Location","Medium","cell_type","volume","assigned_to","next_action_date","notes"]].rename(columns={
                "id":"ID",
                "cell_line":"Cell Line",
                "event_type":"Event",
                "action_label":"Action Label",
                "done":"Mark Done",
                "vessel":"Vessel",
                "cell_type":"Cell Type",
                "volume":"Volume (mL)",
                "assigned_to":"Assigned To",
                "next_action_date":"Next Action",
                "notes":"Notes",
            })
            run_cols_display["Assigned Color"] = run_cols_display["Assigned To"].apply(_color_for_user)
            color_series_run = run_cols_display.set_index("ID")["Assigned Color"]
            run_view = run_cols_display.drop(columns=["Assigned Color"]).set_index("ID")
            styled_run = run_view.style.apply(
                lambda row: [f"background-color: {_with_alpha(color_series_run.loc[row.name], 0.18)};"] * len(row),
                axis=1,
            )
            st.dataframe(styled_run, use_container_width=True)

            with st.expander("Edit run sheet rows"):
                edited = st.data_editor(
                    run_cols_display.drop(columns=["Assigned Color"]),
                    column_config={
                        "ID": st.column_config.Column(disabled=True),
                        "Cell Line": st.column_config.Column(disabled=True),
                        "Event": st.column_config.Column(disabled=True),
                        "Action Label": st.column_config.SelectboxColumn(options=["(none)"] + ACTION_LABELS),
                        "Mark Done": st.column_config.CheckboxColumn(),
                        "Vessel": st.column_config.Column(disabled=False),
                        "Location": st.column_config.Column(disabled=False),
                        "Medium": st.column_config.Column(disabled=False),
                        "Cell Type": st.column_config.Column(disabled=False),
                        "Volume (mL)": st.column_config.Column(disabled=False),
                        "Assigned To": st.column_config.SelectboxColumn(options=["(unassigned)"] + _usernames_all if _usernames_all else ["(unassigned)"]),
                        "Next Action": st.column_config.DateColumn(),
                        "Notes": st.column_config.TextColumn(),
                    },
                    hide_index=True,
                    use_container_width=True,
                )
                st.caption("Edit fields inline and click 'Save updates' to record changes.")
                if st.button("Save updates", key="save_run_sheet"):
                    # Compare edited vs original to detect changes
                    changes = []
                    for _, row in edited.iterrows():
                            orig = df_tasks[df_tasks["id"] == row["ID"]].iloc[0]
                            payload = {}
                            if orig.get("location","") != row["Location"]:
                                payload["location"] = row["Location"]
                            if orig.get("medium","") != row["Medium"]:
                                payload["medium"] = row["Medium"]
                            if orig.get("cell_type","") != row["Cell Type"]:
                                payload["cell_type"] = row["Cell Type"]
                            if (orig.get("volume") or 0) != row["Volume (mL)"]:
                                payload["volume"] = row["Volume (mL)"]
                            if orig.get("assigned_to","") != row["Assigned To"]:
                                payload["assigned_to"] = None if not row["Assigned To"] or row["Assigned To"] == "(unassigned)" else row["Assigned To"]
                            if orig.get("action_label","") != row["Action Label"]:
                                payload["action_label"] = None if row["Action Label"] in (None, "(none)") else row["Action Label"]
                            if row["Mark Done"]:
                                payload["next_action_date"] = None
                            elif str(orig.get("next_action_date")) != str(row["Next Action"]):
                                payload["next_action_date"] = row["Next Action"].isoformat() if pd.notna(row["Next Action"]) else None
                            if orig.get("notes","") != row["Notes"]:
                                payload["notes"] = row["Notes"]
                            if payload:
                                payload["id"] = row["ID"]
                                changes.append(payload)
                    if not changes:
                        st.info("No changes to save.")
                    else:
                        try:
                            bulk_update_logs(conn, changes)
                            invalidate_logs_cache()
                            st.success("Updates saved.")
                        except Exception as exc:
                            st.error(f"Failed to save: {exc}")

                st.markdown("#### Media prep summary (total volume)")
                if edited.empty:
                    st.info("Nothing to summarize.")
                else:
                    summary_df = edited.copy()
                    summary_df["Volume (mL)"] = pd.to_numeric(summary_df["Volume (mL)"], errors="coerce").fillna(0.0)
            media_summary = (
                summary_df.groupby("Medium", as_index=False)["Volume (mL)"]
                .sum()
                .rename(columns={"Medium": "Media", "Volume (mL)": "Total Volume (mL)"})
                .sort_values("Total Volume (mL)", ascending=False)
            )
            st.dataframe(media_summary, use_container_width=True)

with tab_scheduler:
    st.subheader("📆 Weekend Duty Scheduler")
    upcoming_saturday = date.today() + timedelta((5 - date.today().weekday()) % 7)
    sched_col1, sched_col2 = st.columns(2)
    with sched_col1:
        range_start = st.date_input("From (Saturday)", value=upcoming_saturday, key="sched_start")
    with sched_col2:
        range_end = st.date_input("To (Sunday)", value=upcoming_saturday + timedelta(days=1), key="sched_end")
    if range_end < range_start:
        st.warning("End date cannot be before start date.")
    duty_dates = [(range_start + timedelta(days=i)).isoformat() for i in range((range_end - range_start).days + 1)]
    sched_col3, _ = st.columns([1, 1])
    with sched_col3:
        sched_user = st.selectbox(
            "Assign operator",
            options=["(none)"] + _usernames_all if _usernames_all else ["(none)"],
            key="sched_user",
        )
    save_col, delete_col = st.columns(2)
    with save_col:
        if st.button("Save weekend assignment", key="sched_save"):
            if duty_dates:
                upsert_weekend_assignment(
                    conn,
                    duty_dates,
                    None if sched_user in (None, "(none)") else sched_user,
                    None,
                )
                invalidate_weekend_cache()
                st.success(f"Assigned {len(duty_dates)} date(s).")
            else:
                st.error("Pick a weekend date.")
    with delete_col:
        if st.button("Remove assignment", key="sched_delete"):
            if duty_dates:
                for d in duty_dates:
                    delete_weekend_assignment(conn, d)
                invalidate_weekend_cache()
                st.success("Selected weekends cleared.")
            else:
                st.error("Pick a weekend date to remove.")

    schedule_rows = get_weekend_rows_cached()
    if schedule_rows:
        sched_df = pd.DataFrame(schedule_rows)
        sched_df["date"] = pd.to_datetime(sched_df["date"], errors="coerce")
        sched_df = sched_df.dropna(subset=["date"]).sort_values("date")
        sched_df["assigned_to"] = sched_df["assigned_to"].fillna("")

        filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
        with filter_col1:
            default_view_start = date.today() - timedelta(days=7)
            filter_start = st.date_input("View from", value=default_view_start, key="sched_filter_start")
        with filter_col2:
            filter_end = st.date_input("Through", value=date.today() + timedelta(days=28), key="sched_filter_end")
        with filter_col3:
            filter_users = st.multiselect(
                "Operators",
                options=["(unassigned)"] + _usernames_all if _usernames_all else ["(unassigned)"],
                key="sched_filter_users",
            )
        with filter_col4:
            future_only = st.checkbox("Future only", value=False, key="sched_filter_future")

        filtered = sched_df.copy()
        if filter_start:
            start_ts = pd.Timestamp(filter_start)
            filtered = filtered[filtered["date"] >= start_ts]
        if filter_end:
            end_ts = pd.Timestamp(filter_end)
            filtered = filtered[filtered["date"] <= end_ts]
        if filter_users:
            normalized = filtered["assigned_to"].replace({"": "(unassigned)"})
            filtered = filtered[normalized.isin(filter_users)]
        if future_only:
            today_ts = pd.Timestamp(date.today())
            filtered = filtered[filtered["date"] >= today_ts]

        if filtered.empty:
            st.info("No assignments match the selected filters.")
        else:
            base_lookup = {row["date"].date(): row for _, row in sched_df.iterrows()}
            editor_df = filtered.copy()
            editor_df["Weekend"] = pd.to_datetime(editor_df["date"])
            editor_df["Assigned To"] = editor_df["assigned_to"].replace({"": "(unassigned)"})
            editor_df["Updated"] = pd.to_datetime(editor_df["updated_at"], errors="coerce")
            editor_df["Remove"] = False
            editor_view = editor_df[["Weekend", "Assigned To", "Updated", "Remove"]]

            st.markdown("#### Coverage calendar")
            edited_sched = st.data_editor(
                editor_view,
                column_config={
                    "Weekend": st.column_config.DateColumn(disabled=True),
                    "Assigned To": st.column_config.SelectboxColumn(
                        options=["(unassigned)"] + _usernames_all if _usernames_all else ["(unassigned)"]
                    ),
                    "Updated": st.column_config.DatetimeColumn(disabled=True),
                    "Remove": st.column_config.CheckboxColumn(),
                },
                hide_index=True,
                key="sched_editor",
                use_container_width=True,
            )
            st.caption("Update assignments inline, mark rows for removal, then save.")
            if st.button("Save scheduler changes", key="sched_editor_save"):
                try:
                    changed = False
                    for _, row in edited_sched.iterrows():
                        raw_date = row["Weekend"]
                        if pd.isna(raw_date):
                            continue
                        day = raw_date.date() if isinstance(raw_date, pd.Timestamp) else raw_date
                        date_key = day.isoformat()
                        if row.get("Remove"):
                            delete_weekend_assignment(conn, date_key)
                            changed = True
                            continue
                        new_assignee = row["Assigned To"]
                        normalized_assignee = None if new_assignee in (None, "", "(unassigned)") else new_assignee
                        orig = base_lookup.get(day, {})
                        if orig.get("assigned_to") != normalized_assignee:
                            upsert_weekend_assignment(
                                conn,
                                [date_key],
                                normalized_assignee,
                                orig.get("notes"),
                            )
                            changed = True
                    if changed:
                        invalidate_weekend_cache()
                    st.success("Scheduler updated.")
                except Exception as exc:
                    st.error(f"Failed to save scheduler changes: {exc}")
    else:
        st.info("No weekend assignments yet — add one above.")

with tab_settings:
    st.subheader("⚙️ Settings (Reference Lists)")
    st.caption("Manage dropdown values and backups.")
    manage_kind_label = st.selectbox("Manage list", options=["Cell Lines","Event Types","Vessels","Locations","Cell Types","Culture Media","Operators"], index=0)
    _kind_map = {
        "Cell Lines": "cell_line",
        "Event Types": "event_type",
        "Vessels": "vessel",
        "Locations": "location",
        "Cell Types": "cell_type",
        "Culture Media": "culture_medium",
    }
    manage_kind = _kind_map.get(manage_kind_label)

    if manage_kind_label != "Operators":
        existing_vals = get_ref_values_cached(manage_kind)
        st.write(f"Current {manage_kind_label} ({len(existing_vals)}):")
        if existing_vals:
            st.dataframe(pd.DataFrame({"Name": existing_vals}), width='stretch')
        else:
            st.info("No values yet.")

        st.markdown("---")
        st.markdown("### Add New")
        new_val = st.text_input("New name", key=f"new_{manage_kind_label}")
        if st.button("Add", key=f"btn_add_{manage_kind_label}"):
            if not new_val or not new_val.strip():
                st.warning("Enter a name to add.")
            else:
                add_ref_value(conn, manage_kind, new_val.strip())
                invalidate_reference_cache()
                st.success("Added.")
                st.rerun()

        st.markdown("### Rename")
        existing_vals = get_ref_values_cached(manage_kind)
        if existing_vals:
            old_val = st.selectbox("Select existing", options=existing_vals, key=f"rename_src_{manage_kind_label}")
            new_name = st.text_input("New name", key=f"rename_dst_{manage_kind_label}")
            if st.button("Rename", key=f"btn_rename_{manage_kind_label}"):
                if not new_name or not new_name.strip():
                    st.warning("Enter a new name.")
                else:
                    rename_ref_value(conn, manage_kind, old_val, new_name)
                    invalidate_reference_cache()
                    st.success("Renamed.")
                    st.rerun()
        else:
            st.info("Nothing to rename.")

        st.markdown("### Delete")
        existing_vals = get_ref_values_cached(manage_kind)
        if existing_vals:
            del_val = st.selectbox("Select to delete", options=existing_vals, key=f"del_{manage_kind_label}")
            confirm = st.checkbox("I understand this will remove the value", key=f"confirm_del_{manage_kind_label}")
            if st.button("Delete", key=f"btn_del_{manage_kind_label}"):
                if confirm:
                    delete_ref_value(conn, manage_kind, del_val)
                    invalidate_reference_cache()
                    st.success("Deleted.")
                    st.rerun()
                else:
                    st.warning("Please confirm before deleting.")
        else:
            st.info("Nothing to delete.")
    else:
        # Operators management
        try:
            ops_raw = get_users_with_colors_cached()
            ops = [
                (
                    row.get("username"),
                    row.get("display_name") or row.get("username"),
                    row.get("color_hex") or "",
                )
                for row in ops_raw
                if row.get("username")
            ]
        except Exception:
            ops = []
        st.write(f"Current Operators ({len(ops)}):")
        if ops:
            df_ops = pd.DataFrame(ops, columns=["Username", "Display Name", "Color"])
            st.dataframe(df_ops, width='stretch')
        else:
            st.info("No operators yet.")

        st.markdown("---")
        st.markdown("### Add Operator")
        new_username = st.text_input("Username", key="new_operator_username")
        new_display = st.text_input("Display name (optional)", key="new_operator_display")
        new_color = st.color_picker("Color code", value="#4a90e2", key="new_operator_color")
        if st.button("Add Operator", key="btn_add_operator"):
            if not new_username or not new_username.strip():
                st.warning("Enter a username.")
            else:
                get_or_create_user(
                    conn,
                    new_username.strip(),
                    new_display.strip() if new_display else None,
                    new_color,
                )
                invalidate_user_cache()
                st.success("Operator added.")
                st.rerun()

        if ops:
            st.markdown("### Update Operator Color")
            color_map = {row[0]: (row[2] or "#4a90e2") for row in ops}
            color_user = st.selectbox("Operator", options=list(color_map.keys()), key="color_operator_select")
            current_color = color_map.get(color_user, "#4a90e2")
            updated_color = st.color_picker("Color", value=current_color, key="color_operator_value")
            if st.button("Save color", key="btn_update_color"):
                update_user_color(conn, color_user, updated_color)
                invalidate_user_cache()
                st.success("Color updated.")
                st.rerun()

        st.markdown("### Delete Operator")
        try:
            ops2 = get_usernames_cached()
        except Exception:
            ops2 = []
        if ops2:
            del_op = st.selectbox("Select operator to delete", options=ops2, key="del_operator")
            confirm_op = st.checkbox("I understand this will remove the operator", key="confirm_del_operator")
            if st.button("Delete Operator", key="btn_del_operator"):
                if confirm_op:
                    delete_user(conn, del_op)
                    invalidate_user_cache()
                    st.success("Operator deleted.")
                    st.rerun()
                else:
                    st.warning("Please confirm before deleting.")
        else:
            st.info("No operators to delete.")

    st.markdown("---")
    st.markdown("### Backup")
    if st.button("Backup database and images"):
        out_dir = backup_now()
        st.success(f"Backup created: {out_dir}")
