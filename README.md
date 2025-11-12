# iPSC Culture Tracker (Streamlit)

Team-ready LIMS-style app to log iPSC culture events with images, track thaw-linked timelines, assign next actions, and export CSV. Culture logs, reference data, and weekend schedules now live in Snowflake; uploaded images still reside on disk (or your Streamlit Cloud scratch space).

## Features
- Add Entry: form with Cell Line, Event, Passage, Vessel, Location, Culture Medium, Cell Type, Volume (mL), Notes, Operator, optional image; auto Thaw IDs for Thawing events; link other events to an existing Thaw ID
- Reuse/Copy speed-ups: reuse previous values for same Cell Line + Event; copy a recent entry for the Cell Line and tweak
- History: filter, view, and download CSV; shows Medium, Cell Type, Volume
- Thaw Timeline: chronological view for a selected Thaw ID
- Dashboard: Upcoming/Overdue using Next Action Date; “Assigned to me” filter
- Settings: manage reference lists (Cell Lines, Event Types, Vessels, Locations, Cell Types, Culture Media), Operators (add/delete), and create backups

## Snowflake configuration

The app expects Snowflake credentials via environment variables or Streamlit secrets. Minimum keys: `account`, `user`, `password`, `warehouse`, `database`, `schema` (optional: `role`). Examples:

**Environment variables**
```
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
export SNOWFLAKE_USER=ntelugu
export SNOWFLAKE_PASSWORD=********
export SNOWFLAKE_WAREHOUSE=IPSC_TRACKER_WH
export SNOWFLAKE_DATABASE=IPSC_TRACKER_DB
export SNOWFLAKE_SCHEMA=PUBLIC
```

**.streamlit/secrets.toml**
```
[snowflake]
account = "xy12345.us-east-1"
user = "ntelugu"
password = "********"
warehouse = "IPSC_TRACKER_WH"
database = "IPSC_TRACKER_DB"
schema = "PUBLIC"
```

`db.py` calls `init_db()` at startup and will create/adjust the Snowflake tables if they do not exist.

## Quick start (local)
1) Create and activate a virtual environment
   - macOS/Linux (venv)
     - python3 -m venv .venv
     - source .venv/bin/activate
2) Install dependencies
   - pip install -r requirements.txt
3) Configure Snowflake credentials (env vars or `.streamlit/secrets.toml`)
4) Run the app
   - streamlit run app.py
   - If Safari cannot open the app, try http://localhost:8501 or run with: python -m streamlit run app.py --server.address=localhost --server.port=8501

Data location
- Structured data: Snowflake (configured above)
- SQLite file `ipsc_tracker.db`: only used for legacy backups (still ignored by Git)
- Uploaded images: images/ (ignored by Git)
- Backups: backups/ (ignored by Git)

## Share via GitHub

This repo is set up with a .gitignore to avoid committing your local database, images, and backups.

Create a new GitHub repo and push:
1) Initialize Git and commit locally
   - git init
   - git add .
   - git commit -m "Initial commit: iPSC Culture Tracker"
2) Create a GitHub repo (on github.com) and copy the remote URL, e.g. https://github.com/<you>/iPSC_Tracker.git
3) Add remote and push
   - git branch -M main
   - git remote add origin https://github.com/<you>/iPSC_Tracker.git
   - git push -u origin main

Invite collaborators
- On GitHub, go to Settings → Collaborators → Add people → grant Write access

## Collaborator setup
1) Clone the repo
   - git clone https://github.com/<you>/iPSC_Tracker.git
   - cd iPSC_Tracker
2) Create a virtual env and install deps
   - python3 -m venv .venv
   - source .venv/bin/activate
   - pip install -r requirements.txt
3) Add `SNOWFLAKE_*` env vars (or `.streamlit/secrets.toml`)
4) Run
   - streamlit run app.py

Notes
- Everyone reads/writes the same Snowflake tables, so credentials control who can see/edit culture history.
- Images/backups remain local; to share them, use Settings → Backup and send the bundle (images + optional sqlite snapshot) to your teammate.

## Deploy for team

Whether you deploy on Streamlit Cloud, Render, or another platform, supply the same Snowflake credentials as secrets/environment variables. The existing `render.yaml` still works for Streamlit + disk-backed images; just add:

```
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DATABASE=...
SNOWFLAKE_SCHEMA=...
```

Images remain on the server disk (`DATA_ROOT`), but the culture history lives in Snowflake so redeploys no longer wipe data.
