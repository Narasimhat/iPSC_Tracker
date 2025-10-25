import os
import io
from datetime import date, datetime
import pandas as pd
import streamlit as st
from PIL import Image

from db import (
    ensure_dirs,
    get_conn,
    init_db,
    insert_log,
    generate_thaw_id_for_date,
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
    get_or_create_user,
    delete_user,
    IMAGES_DIR,
)

st.set_page_config(page_title="iPSC Culture Tracker", layout="wide")

st.title("🧬 iPSC Culture Tracker")
st.write("LIMS-style multi-user cell culture tracker with thaw-linked histories.")

# Initialize database and storage
conn = get_conn()
init_db(conn)
ensure_dirs()

# Current user context (for 'Assigned to me' filters)
try:
    _rows_users = conn.execute("SELECT username FROM users ORDER BY username").fetchall()
    _usernames_all = [r[0] for r in _rows_users]
except Exception:
    _usernames_all = []
my_name = st.selectbox("My name", options=["(none)"] + _usernames_all if _usernames_all else ["(none)"], index=0, help="Used for 'Assigned to me' filters")
st.session_state["my_name"] = None if my_name == "(none)" else my_name

# Initialize session state
if "pending_thaw_id" not in st.session_state:
    st.session_state["pending_thaw_id"] = ""

tab_add, tab_history, tab_thaw, tab_dashboard, tab_settings = st.tabs([
    "Add Entry",
    "History",
    "Thaw Timeline",
    "Dashboard",
    "Settings",
])

# ----------------------- Add Entry Tab -----------------------
with tab_add:
    st.subheader("📋 Add New Log Entry")

    with st.form("add_entry_form", clear_on_submit=False):
        cl_values = get_ref_values(conn, "cell_line")
        cell_line = st.selectbox("Cell Line ID *", options=cl_values) if cl_values else st.text_input("Cell Line ID *", placeholder="e.g., BIHi005-A-24")

        evt_values = get_ref_values(conn, "event_type")
        event_type = st.selectbox("Event Type *", options=evt_values if evt_values else [
            "Observation", "Media Change", "Split", "Thawing", "Cryopreservation", "Other"
        ])
        # Copy from a previous entry for this cell line
        prev = None
        copy_col1, copy_col2 = st.columns([1, 2])
        with copy_col1:
            enable_copy = st.checkbox("Copy previous entry", value=False)
        with copy_col2:
            prev_choice = None
            if enable_copy and cell_line:
                recent = get_recent_logs_for_cell_line(conn, cell_line, limit=20)
                if recent:
                    display = [f"{r.get('date','')} • {r.get('event_type','')} • P{r.get('passage') or ''} • {r.get('vessel') or ''}" for r in recent]
                    idx = st.selectbox("Choose an entry to copy", options=list(range(len(display))), format_func=lambda i: display[i])
                    prev = recent[idx]
                else:
                    st.info("No previous entries for this Cell Line yet.")

        # Option to reuse previous values for same task
        reuse_prev = st.checkbox("Reuse previous values for this Cell Line + Event", value=False)
        if reuse_prev and not prev and cell_line and event_type:
            prev = get_last_log_for_line_event(conn, cell_line, event_type)

        col_a, col_b, col_c = st.columns(3)
        with col_a:
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
            passage_no = st.number_input("Passage No.", min_value=1, step=1, value=default_passage)
        with col_b:
            vessel_refs = get_ref_values(conn, "vessel")
            if vessel_refs:
                v_index = 0
                if prev and prev.get("vessel") in vessel_refs:
                    v_index = vessel_refs.index(prev.get("vessel"))
                vessel = st.selectbox("Vessel", options=vessel_refs, index=v_index)
            else:
                vessel_default = prev.get("vessel") if prev and prev.get("vessel") else ""
                vessel = st.text_input("Vessel", placeholder="e.g., T25, 6-well plate", value=vessel_default)
        with col_c:
            location_refs = get_ref_values(conn, "location")
            if location_refs:
                l_index = 0
                if prev and prev.get("location") in location_refs:
                    l_index = location_refs.index(prev.get("location"))
                location = st.selectbox("Location", options=location_refs, index=l_index)
            else:
                loc_default = prev.get("location") if prev and prev.get("location") else ""
                location = st.text_input("Location", placeholder="e.g., Incubator A, Shelf 2", value=loc_default)

        # Culture Medium (single input with suggestions)
        _med_sugs = top_values(conn, "medium", cell_line=cell_line) if cell_line else top_values(conn, "medium")
        cm_refs = get_ref_values(conn, "culture_medium")
        if cm_refs:
            m_index = 0
            if prev and prev.get("medium") in cm_refs:
                m_index = cm_refs.index(prev.get("medium"))
            medium = st.selectbox("Culture Medium", options=cm_refs, index=m_index)
        else:
            med_default = prev.get("medium") if prev and prev.get("medium") else ""
            medium = st.text_input("Culture Medium", placeholder="e.g., StemFlex", value=med_default)
        if _med_sugs:
            st.caption("Suggestions: " + ", ".join([str(x) for x in _med_sugs]))

        # Cell Type (single input with suggestions)
        _ct_sugs = top_values(conn, "cell_type", cell_line=cell_line) if cell_line else top_values(conn, "cell_type")
        ct_refs = get_ref_values(conn, "cell_type")
        if ct_refs:
            ct_index = 0
            if prev and prev.get("cell_type") in ct_refs:
                ct_index = ct_refs.index(prev.get("cell_type"))
            cell_type = st.selectbox("Cell Type", options=ct_refs, index=ct_index)
        else:
            ct_default = prev.get("cell_type") if prev and prev.get("cell_type") else ""
            cell_type = st.text_input("Cell Type", placeholder="e.g., iPSC, NPC, cardiomyocyte", value=ct_default)
        if _ct_sugs:
            st.caption("Suggestions: " + ", ".join([str(x) for x in _ct_sugs]))

        # Volume in mL
        default_volume = 0.0
        if prev and prev.get("volume") is not None:
            try:
                default_volume = float(prev.get("volume"))
            except Exception:
                default_volume = 0.0
        volume = st.number_input("Volume (mL)", min_value=0.0, step=0.5, value=default_volume)

        notes = st.text_area("Notes / Observations")

        user_rows = conn.execute("SELECT username FROM users ORDER BY username").fetchall()
        usernames = [r[0] for r in user_rows]
        if usernames:
            operator = st.selectbox("Operator *", options=usernames)
        else:
            st.info("No operators yet. Add some under Settings → Operators.")
            operator = st.text_input("Operator *", placeholder="Your name")
        log_date = st.date_input("Date *", value=date.today())

        all_users = []
        try:
            with get_conn() as _c:
                rows = _c.execute("SELECT username FROM users ORDER BY username").fetchall()
                all_users = [r[0] for r in rows]
        except Exception:
            all_users = []

        assigned_to = st.selectbox("Assigned To", options=["(unassigned)"] + all_users if all_users else ["(unassigned)"])
        next_action_date = st.date_input("Next Action Date", value=None)

        uploaded_img = st.file_uploader("Add colony image (optional)", type=["png", "jpg", "jpeg"])

        cryo_vial_position = ""
        linked_thaw_id = ""

        if event_type == "Thawing":
            if not st.session_state["pending_thaw_id"]:
                st.session_state["pending_thaw_id"] = generate_thaw_id_for_date(conn, log_date)
            col_t0, col_t2 = st.columns(2)
            with col_t0:
                st.text_input("Thaw ID (auto)", value=st.session_state["pending_thaw_id"], disabled=True)
            with col_t2:
                cryo_vial_position = st.text_input("Cryo Vial Position", placeholder="e.g., Box A2, Row 3 Col 5")
        else:
            thaw_ids = list_distinct_thaw_ids(conn)
            linked_thaw_id = st.selectbox(
                "Link to Thaw ID",
                options=["(none)"] + thaw_ids if thaw_ids else ["(none)"],
                index=0,
                help="Associate this entry with an existing thaw event",
            )

        if cell_line:
            hint_event = suggest_next_event(conn, cell_line)
            if hint_event:
                st.caption(f"Suggestion: Next likely event for {cell_line} is '{hint_event}'.")

        submitted = st.form_submit_button("Save Entry")
        if submitted:
            if not operator:
                st.error("Please provide an Operator.")
                st.stop()
            img_bytes = uploaded_img.getvalue() if uploaded_img else None
            thaw_id_val = ""
            if event_type == "Thawing":
                thaw_id_val = st.session_state["pending_thaw_id"] or generate_thaw_id_for_date(conn, log_date)
                st.session_state["pending_thaw_id"] = ""
            else:
                thaw_id_val = linked_thaw_id if linked_thaw_id and linked_thaw_id != "(none)" else ""

            image_path = None
            if img_bytes:
                ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
                ext = os.path.splitext(uploaded_img.name)[1] if uploaded_img and uploaded_img.name else ".jpg"
                fname = f"{ts}_{(thaw_id_val or 'noThaw').replace('-', '')}{ext}"
                fpath = os.path.join(IMAGES_DIR, fname)
                with open(fpath, "wb") as f:
                    f.write(img_bytes)
                image_path = fpath

            payload = {
                "date": log_date.isoformat(),
                "cell_line": cell_line,
                "event_type": event_type,
                "passage": int(passage_no) if passage_no else None,
                "vessel": vessel,
                "location": location,
                "medium": medium,
                "cell_type": cell_type,
                "volume": float(volume) if volume is not None else None,
                "notes": notes,
                "operator": operator,
                "thaw_id": thaw_id_val,
                "cryo_vial_position": cryo_vial_position,
                "image_path": image_path,
                "assigned_to": None if assigned_to in (None, "(unassigned)") else assigned_to,
                "next_action_date": next_action_date.isoformat() if next_action_date else None,
                "created_by": operator,
                "created_at": datetime.utcnow().isoformat(),
            }
            insert_log(conn, payload)
            st.success("✅ Log entry saved to database!")

with tab_history:
    st.subheader("📜 Culture History")
    fcol1, fcol2, fcol3 = st.columns([2, 1, 1])
    with fcol1:
        f_cell = st.text_input("Cell line contains", "")
    with fcol2:
        f_event = st.selectbox("Event Type", ["(any)", "Observation", "Media Change", "Split", "Thawing", "Cryopreservation", "Other"]) 
    with fcol3:
        f_assigned = st.text_input("Assigned To contains", "")
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
        if only_mine and st.session_state.get("my_name"):
            df = df[df.get("assigned_to", "").astype(str) == st.session_state["my_name"]]
        elif only_mine and not st.session_state.get("my_name"):
            st.info("Set 'My name' at the top to enable 'Assigned to me'.")
        display_cols = [
            "date", "cell_line", "event_type", "passage", "vessel", "location", "medium", "cell_type", "volume", "notes", "operator", "thaw_id", "cryo_vial_position", "assigned_to", "next_action_date", "created_by"
        ]
        for c in display_cols:
            if c not in df.columns:
                df[c] = ""
        pretty = df[display_cols].rename(columns={
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
        })
        st.dataframe(pretty, width='stretch')

        csv = pretty.to_csv(index=False).encode('utf-8')
        st.download_button("📂 Download CSV", data=csv, file_name="ipsc_culture_log.csv", mime="text/csv")
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
                "date", "cell_line", "event_type", "passage", "vessel", "location", "medium", "cell_type", "volume", "notes", "operator", "created_by"
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
                "created_by": "Created By",
            }), width='stretch')
        else:
            st.info("No records for this Thaw ID yet.")

with tab_dashboard:
    st.subheader("📅 Upcoming & Overdue")
    dash_only_mine = st.checkbox("Show only items assigned to me", value=False)
    # Basic upcoming/overdue view using Next Action Date
    all_logs = query_logs(conn)
    df_all = pd.DataFrame(all_logs) if all_logs else pd.DataFrame([])
    if not df_all.empty and "next_action_date" in df_all.columns:
        today = pd.to_datetime(date.today())
        df_all["_nad"] = pd.to_datetime(df_all["next_action_date"], errors="coerce")
        if dash_only_mine and st.session_state.get("my_name"):
            df_all = df_all[df_all.get("assigned_to", "").astype(str) == st.session_state["my_name"]]
        elif dash_only_mine and not st.session_state.get("my_name"):
            st.info("Set 'My name' at the top to filter to your items.")
        df_overdue = df_all[(~df_all["_nad"].isna()) & (df_all["_nad"] < today)]
        df_upcoming = df_all[(~df_all["_nad"].isna()) & (df_all["_nad"] >= today)].sort_values("_nad").head(50)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Overdue**")
            if df_overdue.empty:
                st.info("No overdue items.")
            else:
                st.dataframe(df_overdue[["cell_line","event_type","assigned_to","next_action_date","notes"]].rename(columns={
                    "cell_line":"Cell Line","event_type":"Event Type","assigned_to":"Assigned To","next_action_date":"Next Action Date","notes":"Notes"
                }), width='stretch')
        with c2:
            st.markdown("**Upcoming**")
            if df_upcoming.empty:
                st.info("No upcoming items.")
            else:
                st.dataframe(df_upcoming[["cell_line","event_type","assigned_to","next_action_date","notes"]].rename(columns={
                    "cell_line":"Cell Line","event_type":"Event Type","assigned_to":"Assigned To","next_action_date":"Next Action Date","notes":"Notes"
                }), width='stretch')
    else:
        st.info("No Next Action Dates yet.")

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
            _urows = conn.execute("SELECT username, COALESCE(display_name, username) FROM users ORDER BY username").fetchall()
            ops = [(r[0], r[1]) for r in _urows]
        except Exception:
            ops = []
        st.write(f"Current Operators ({len(ops)}):")
        if ops:
            st.dataframe(pd.DataFrame(ops, columns=["Username","Display Name"]), width='stretch')
        else:
            st.info("No operators yet.")

        st.markdown("---")
        st.markdown("### Add Operator")
        new_username = st.text_input("Username", key="new_operator_username")
        new_display = st.text_input("Display name (optional)", key="new_operator_display")
        if st.button("Add Operator", key="btn_add_operator"):
            if not new_username or not new_username.strip():
                st.warning("Enter a username.")
            else:
                get_or_create_user(conn, new_username.strip(), new_display.strip() if new_display else None)
                st.success("Operator added.")
                st.rerun()

        st.markdown("### Delete Operator")
        try:
            _urows2 = conn.execute("SELECT username FROM users ORDER BY username").fetchall()
            ops2 = [r[0] for r in _urows2]
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