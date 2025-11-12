import os
import io
import statistics
from datetime import date, datetime, timedelta
from typing import Optional
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
    get_last_thaw_id,
    get_weekend_schedule,
    upsert_weekend_assignment,
    delete_weekend_assignment,
    get_weekend_assignment_for_date,
    get_or_create_user,
    delete_user,
    update_user_color,
    list_usernames,
    list_users_with_colors,
    update_log_fields,
    bulk_update_logs,
    IMAGES_DIR,
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
conn = get_conn()
init_db(conn)
ensure_dirs()

# Current user context (for 'Assigned to me' filters)
try:
    _usernames_all = list_usernames(conn)
except Exception:
    _usernames_all = []
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

try:
    _rows_colors = list_users_with_colors(conn)
    _user_colors = {}
    auto_idx = 0
    for row in _rows_colors:
        username = (row.get("username") or "").strip()
        color_hex = (row.get("color_hex") or "").strip()
        if color_hex:
            _user_colors[username] = color_hex
        else:
            auto_color = COLOR_PALETTE[auto_idx % len(COLOR_PALETTE)]
            auto_idx += 1
            update_user_color(conn, username, auto_color)
            _user_colors[username] = auto_color
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
my_name = st.selectbox("My name", options=["(none)"] + _usernames_all if _usernames_all else ["(none)"], index=0, help="Used for 'Assigned to me' filters")
st.session_state["my_name"] = None if my_name == "(none)" else my_name

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

        id_col1, id_col2, id_col3, id_col4 = st.columns([1.7, 1.3, 1.1, 0.9])
        with id_col1:
            cl_values = get_ref_values(conn, "cell_line")
            if cl_values:
                cell_line = st.selectbox("Cell Line ID *", options=cl_values, key="cell_line_select")
            else:
                cell_line = st.text_input("Cell Line ID *", value="", placeholder="e.g., BIHi005-A-24")
        with id_col2:
            evt_values = get_ref_values(conn, "event_type")
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
            cryo_vial_position = st.text_input(
                "Cryovial Position",
                value=cryo_vial_position,
                placeholder="e.g., Box A2, Row 3 Col 5",
            )
        with id_col4:
            default_volume = 0.0
            if prev and prev.get("volume") is not None:
                try:
                    default_volume = float(prev.get("volume"))
                except Exception:
                    default_volume = 0.0
            volume = st.number_input("Volume (mL)", min_value=0.0, step=0.5, value=default_volume)
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
            passage_no = st.number_input("Passage No.", min_value=1, step=1, value=default_passage)
            if split_auto:
                st.caption(f"Split detected → Passage auto-set to {split_auto}.")
        with row1_col2:
            vessel_refs = get_ref_values(conn, "vessel")
            vessel_default = prev.get("vessel") if prev and prev.get("vessel") else template_suggests.get("vessel", "")
            if vessel_refs:
                v_idx = vessel_refs.index(vessel_default) if vessel_default in vessel_refs else 0
                vessel = st.selectbox("Vessel", options=vessel_refs, index=v_idx)
            else:
                vessel = st.text_input("Vessel", value=vessel_default, placeholder="e.g., T25, 6-well plate")
        with row1_col3:
            location_refs = get_ref_values(conn, "location")
            default_location = prev.get("location") if prev and prev.get("location") else template_suggests.get("location", "")
            if location_refs:
                loc_idx = location_refs.index(default_location) if default_location in location_refs else 0
                location = st.selectbox("Location", options=location_refs, index=loc_idx)
            else:
                location = st.text_input("Location", value=default_location, placeholder="e.g., Incubator A, Shelf 2")
        with row1_col4:
            _med_sugs = top_values(conn, "medium", cell_line=cell_line) if cell_line else top_values(conn, "medium")
            cm_refs = get_ref_values(conn, "culture_medium")
            default_med = prev.get("medium") if prev and prev.get("medium") else template_suggests.get("medium", "")
            if cm_refs:
                med_idx = cm_refs.index(default_med) if default_med in cm_refs else 0
                medium = st.selectbox("Culture Medium", options=cm_refs, index=med_idx)
            else:
                medium = st.text_input("Culture Medium", value=default_med, placeholder="e.g., StemFlex")
            if _med_sugs:
                st.caption("Popular: " + ", ".join([str(x) for x in _med_sugs]))
        with row1_col5:
            _ct_sugs = top_values(conn, "cell_type", cell_line=cell_line) if cell_line else top_values(conn, "cell_type")
            ct_refs = get_ref_values(conn, "cell_type")
            default_ct = prev.get("cell_type") if prev and prev.get("cell_type") else template_suggests.get("cell_type", "")
            if ct_refs:
                ct_idx = ct_refs.index(default_ct) if default_ct in ct_refs else 0
                cell_type = st.selectbox("Cell Type", options=ct_refs, index=ct_idx)
            else:
                cell_type = st.text_input("Cell Type", value=default_ct, placeholder="e.g., iPSC, NPC, cardiomyocyte")
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
        notes = st.text_area("Notes", placeholder=notes_placeholder, height=80)

        st.divider()
        sched_col1, sched_col2, sched_col3, sched_col4, sched_col5 = st.columns([1, 1, 1, 1, 1])
        with sched_col1:
            try:
                usernames = list_usernames(conn)
            except Exception:
                usernames = []
            if usernames:
                op_index = 0
                if st.session_state.get("my_name") and st.session_state["my_name"] in usernames:
                    op_index = usernames.index(st.session_state["my_name"])
                operator = st.selectbox("Operator *", options=usernames, index=op_index)
            else:
                st.info("No operators yet. Add some under Settings → Operators.")
                operator = st.text_input("Operator *", placeholder="Your name")
        with sched_col2:
            log_date = st.date_input("Date *", value=date.today())
        default_nad = None
        if template_cfg.get("next_action_days") is not None:
            default_nad = date.today() + timedelta(days=template_cfg["next_action_days"])
        elif EVENT_FOLLOWUP_DEFAULTS.get(event_type) is not None:
            default_nad = date.today() + timedelta(days=EVENT_FOLLOWUP_DEFAULTS[event_type])

        with sched_col3:
            next_action_date = st.date_input("Next Action Date", value=default_nad)
        with sched_col4:
            all_users = []
            try:
                all_users = list_usernames(conn)
            except Exception:
                all_users = []
            assigned_options = ["(unassigned)"] + all_users if all_users else ["(unassigned)"]
            weekend_autofill = None
            if next_action_date:
                weekend_autofill = get_weekend_assignment_for_date(conn, next_action_date)
            assign_index = 0
            if weekend_autofill and weekend_autofill in assigned_options:
                assign_index = assigned_options.index(weekend_autofill)
            elif st.session_state.get("my_name") and st.session_state["my_name"] in assigned_options:
                assign_index = assigned_options.index(st.session_state["my_name"])
            assigned_to = st.selectbox("Assigned To", options=assigned_options, index=assign_index)
            if weekend_autofill:
                st.caption(f"Weekend duty auto-selected: {weekend_autofill}")
        with sched_col5:
            st.empty()
        thaw_preview = ""
        linked_thaw_id = ""
        latest_thaw_for_line = get_last_thaw_id(conn, cell_line) if cell_line else None
        if event_type == "Thawing":
            if cell_line and operator:
                thaw_preview = generate_thaw_id(conn, cell_line, operator, log_date)
                thaw_label = thaw_preview
            else:
                thaw_label = "Select Cell Line + Operator"
            st.text_input("Thaw ID", value=thaw_label, disabled=True, help="Auto-generated when saving.")
        else:
            thaw_ids = list_distinct_thaw_ids(conn)
            options = ["(none)"] + thaw_ids if thaw_ids else ["(none)"]
            idx = 0
            if latest_thaw_for_line and latest_thaw_for_line in thaw_ids:
                idx = options.index(latest_thaw_for_line)
            linked_thaw_id = st.selectbox(
                "Link Thaw ID",
                options=options,
                index=idx,
                help="Associate with an existing thaw event (required for follow-ups).",
            )

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
                weekend_owner = get_weekend_assignment_for_date(conn, next_action_date)
                if weekend_owner:
                    resolved_assignee = weekend_owner
                    auto_assignee_note = weekend_owner

            payload = {
                "date": log_date.isoformat(),
                "cell_line": cell_line,
                "event_type": event_type,
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
            st.success("✅ Log entry saved to database!")
            if auto_assignee_note:
                st.info(f"Assigned to weekend duty: {auto_assignee_note}")

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

    logs = query_logs(
        conn,
        user=None,
        event_type=f_event,
        thaw_id=None,
        start_date=None,
        end_date=None,
        cell_line_contains=f_cell or None,
    )

    if logs:
        df = pd.DataFrame(logs)
        if f_assigned:
            df = df[df.get("assigned_to", "").astype(str).str.contains(f_assigned, case=False, na=False)]
        if f_operator != "(any)" and "operator" in df.columns:
            df = df[df["operator"] == f_operator]
        if only_mine and st.session_state.get("my_name"):
            df = df[df.get("assigned_to", "").astype(str) == st.session_state["my_name"]]
        elif only_mine and not st.session_state.get("my_name"):
            st.info("Set 'My name' at the top to enable 'Assigned to me'.")
        if date_filter != "All":
            df["_date"] = pd.to_datetime(df["date"], errors="coerce")
            today_dt = pd.to_datetime(date.today())
            if date_filter == "Today":
                df = df[df["_date"] == today_dt]
            elif date_filter == "Last 7 days":
                df = df[df["_date"] >= today_dt - pd.Timedelta(days=6)]
            elif date_filter == "Last 30 days":
                df = df[df["_date"] >= today_dt - pd.Timedelta(days=29)]
            df = df.drop(columns=["_date"])
        display_cols = [
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
            export_logs = query_logs(conn, start_date=export_date, end_date=export_date)
            if not export_logs:
                st.info("No entries for that date.")
            else:
                export_df = pd.DataFrame(export_logs)
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
    thaw_ids_list = list_distinct_thaw_ids(conn)
    selected_tid = st.selectbox("Select Thaw ID", options=["(choose)"] + thaw_ids_list if thaw_ids_list else ["(none)"])
    if thaw_ids_list and selected_tid not in ("(choose)", "(none)"):
        timeline = pd.DataFrame(query_logs(conn, thaw_id=selected_tid))
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
    all_logs = query_logs(conn)
    df_all = pd.DataFrame(all_logs) if all_logs else pd.DataFrame([])
    if df_all.empty:
        st.info("No entries yet — add a log to unlock the dashboard.")
    else:
        if "assigned_to" not in df_all.columns:
            df_all["assigned_to"] = ""
        if "next_action_date" not in df_all.columns:
            df_all["next_action_date"] = None
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
                    "assignee": get_weekend_assignment_for_date(conn, d),
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
                    df_overdue[["cell_line", "event_type", "assigned_to", "next_action_date", "notes"]].rename(columns={
                        "cell_line": "Cell Line",
                        "event_type": "Event Type",
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
                    df_upcoming[["cell_line", "event_type", "assigned_to", "next_action_date", "notes"]].rename(columns={
                        "cell_line": "Cell Line",
                        "event_type": "Event Type",
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
                    my_queue[["cell_line", "event_type", "next_action_date", "notes"]].rename(columns={
                        "cell_line": "Cell Line",
                        "event_type": "Event Type",
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
                unassigned[["cell_line", "event_type", "next_action_date", "notes"]].rename(columns={
                    "cell_line": "Cell Line",
                    "event_type": "Event Type",
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
                recent[["Logged", "cell_line", "event_type", "operator", "assigned_to", "notes"]].rename(columns={
                    "cell_line": "Cell Line",
                    "event_type": "Event Type",
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
    all_logs = query_logs(conn)
    df_tasks = pd.DataFrame(all_logs) if all_logs else pd.DataFrame([])
    if df_tasks.empty:
        st.info("No tasks yet.")
    else:
        if "assigned_to" not in df_tasks.columns:
            df_tasks["assigned_to"] = ""
        if "next_action_date" not in df_tasks.columns:
            df_tasks["next_action_date"] = None
        df_tasks["next_action_date"] = pd.to_datetime(df_tasks["next_action_date"], errors="coerce")
        today_dt = pd.to_datetime(date.today())
        df_tasks = df_tasks[
            (~df_tasks["next_action_date"].isna())
            & (df_tasks["next_action_date"].dt.date >= today_dt.date())
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

        media_filter = st.multiselect(
            "Filter by medium",
            options=sorted(df_tasks["medium"].dropna().unique()),
        )
        location_filter = st.multiselect(
            "Filter by location",
            options=sorted(df_tasks["location"].dropna().unique()),
        )
        event_filter = st.multiselect(
            "Filter by event type",
            options=sorted(df_tasks["event_type"].dropna().unique()),
        )
        date_filter = st.selectbox("Show", ["All", "Today", "Tomorrow", "Today + Tomorrow"])
        if media_filter:
            df_tasks = df_tasks[df_tasks["medium"].isin(media_filter)]
        if location_filter:
            df_tasks = df_tasks[df_tasks["location"].isin(location_filter)]
        if event_filter:
            df_tasks = df_tasks[df_tasks["event_type"].isin(event_filter)]
        if date_filter == "Today":
            df_tasks = df_tasks[df_tasks["next_action_date"].dt.date == today_dt.date()]
        elif date_filter == "Tomorrow":
            df_tasks = df_tasks[df_tasks["next_action_date"].dt.date == (today_dt + pd.Timedelta(days=1)).date()]
        elif date_filter == "Today + Tomorrow":
            df_tasks = df_tasks[df_tasks["next_action_date"].dt.date.isin({today_dt.date(), (today_dt + pd.Timedelta(days=1)).date()})]

        if df_tasks.empty:
            st.info("No tasks match the selected filters.")
        else:
            df_tasks["days_to_due"] = (df_tasks["next_action_date"] - today_dt).dt.days
            df_tasks = df_tasks.sort_values(by="next_action_date")
            df_tasks["Location"] = df_tasks["location"].fillna("")
            df_tasks["Medium"] = df_tasks["medium"].fillna("")
            df_tasks["done"] = df_tasks["next_action_date"].dt.date < today_dt.date()
            run_cols_display = df_tasks[["id","cell_line","event_type","done","vessel","Location","Medium","cell_type","volume","assigned_to","next_action_date","notes"]].rename(columns={
                "id":"ID",
                "cell_line":"Cell Line",
                "event_type":"Event",
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
                st.success(f"Assigned {len(duty_dates)} date(s).")
            else:
                st.error("Pick a weekend date.")
    with delete_col:
        if st.button("Remove assignment", key="sched_delete"):
            if duty_dates:
                for d in duty_dates:
                    delete_weekend_assignment(conn, d)
                st.success("Selected weekends cleared.")
            else:
                st.error("Pick a weekend date to remove.")

    schedule_rows = get_weekend_schedule(conn)
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
            filtered = filtered[filtered["date"].dt.date >= filter_start]
        if filter_end:
            filtered = filtered[filtered["date"].dt.date <= filter_end]
        if filter_users:
            normalized = filtered["assigned_to"].replace({"": "(unassigned)"})
            filtered = filtered[normalized.isin(filter_users)]
        if future_only:
            filtered = filtered[filtered["date"].dt.date >= date.today()]

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
                    for _, row in edited_sched.iterrows():
                        raw_date = row["Weekend"]
                        if pd.isna(raw_date):
                            continue
                        day = raw_date.date() if isinstance(raw_date, pd.Timestamp) else raw_date
                        date_key = day.isoformat()
                        if row.get("Remove"):
                            delete_weekend_assignment(conn, date_key)
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
        existing_vals = get_ref_values(conn, manage_kind)
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
                st.success("Added.")
                st.rerun()

        st.markdown("### Rename")
        existing_vals = get_ref_values(conn, manage_kind)
        if existing_vals:
            old_val = st.selectbox("Select existing", options=existing_vals, key=f"rename_src_{manage_kind_label}")
            new_name = st.text_input("New name", key=f"rename_dst_{manage_kind_label}")
            if st.button("Rename", key=f"btn_rename_{manage_kind_label}"):
                if not new_name or not new_name.strip():
                    st.warning("Enter a new name.")
                else:
                    rename_ref_value(conn, manage_kind, old_val, new_name)
                    st.success("Renamed.")
                    st.rerun()
        else:
            st.info("Nothing to rename.")

        st.markdown("### Delete")
        existing_vals = get_ref_values(conn, manage_kind)
        if existing_vals:
            del_val = st.selectbox("Select to delete", options=existing_vals, key=f"del_{manage_kind_label}")
            confirm = st.checkbox("I understand this will remove the value", key=f"confirm_del_{manage_kind_label}")
            if st.button("Delete", key=f"btn_del_{manage_kind_label}"):
                if confirm:
                    delete_ref_value(conn, manage_kind, del_val)
                    st.success("Deleted.")
                    st.rerun()
                else:
                    st.warning("Please confirm before deleting.")
        else:
            st.info("Nothing to delete.")
    else:
        # Operators management
        try:
            ops_raw = list_users_with_colors(conn)
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
                st.success("Color updated.")
                st.rerun()

        st.markdown("### Delete Operator")
        try:
            ops2 = list_usernames(conn)
        except Exception:
            ops2 = []
        if ops2:
            del_op = st.selectbox("Select operator to delete", options=ops2, key="del_operator")
            confirm_op = st.checkbox("I understand this will remove the operator", key="confirm_del_operator")
            if st.button("Delete Operator", key="btn_del_operator"):
                if confirm_op:
                    delete_user(conn, del_op)
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
