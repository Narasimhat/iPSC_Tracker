# iPSC Culture Tracker (Streamlit)

Team-ready LIMS-style app to log iPSC culture events with images, track thaw-linked timelines, assign next actions, and export CSV. Uses SQLite for persistence and stores uploaded images on disk.

## Features
- Add Entry: form with Cell Line, Event, Passage, Vessel, Location, Culture Medium, Cell Type, Volume (mL), Notes, Operator, optional image; auto Thaw IDs for Thawing events; link other events to an existing Thaw ID
- Reuse/Copy speed-ups: reuse previous values for same Cell Line + Event; copy a recent entry for the Cell Line and tweak
- History: filter, view, and download CSV; shows Medium, Cell Type, Volume
- Thaw Timeline: chronological view for a selected Thaw ID
- Dashboard: Upcoming/Overdue using Next Action Date; “Assigned to me” filter
- Settings: manage reference lists (Cell Lines, Event Types, Vessels, Locations, Cell Types, Culture Media), Operators (add/delete), and create backups

## Quick start (local)
1) Create and activate a virtual environment
   - macOS/Linux (venv)
     - python3 -m venv .venv
     - source .venv/bin/activate
2) Install dependencies
   - pip install -r requirements.txt
3) Run the app
   - streamlit run app.py
   - If Safari cannot open the app, try http://localhost:8501 or run with: python -m streamlit run app.py --server.address=localhost --server.port=8501

Data location
- SQLite database: ipsc_tracker.db (ignored by Git by default)
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
3) Run
   - streamlit run app.py

Notes
- Each collaborator gets their own local SQLite DB and images unless you explicitly share those files outside Git.
- To share a snapshot of your data, use Settings → Backup and send the folder to a teammate. They can replace their ipsc_tracker.db and images/ with your backup copies.

## Deploy for team (Render, persistent disk)

This keeps your SQLite database and uploaded images on a server disk so data persists across restarts.

1) Repo contains `render.yaml`. Go to https://dashboard.render.com → New → Blueprint → Connect this repo
2) Confirm the service settings:
   - Build: `pip install -r requirements.txt`
   - Start: `streamlit run app.py --server.address 0.0.0.0 --server.port $PORT`
   - Env vars: `PYTHON_VERSION=3.11`, `DATA_ROOT=/var/data`
   - Disk: name `data`, mount `/var/data`, size ~2GB (adjust as needed)
3) Deploy. Render will give you a public URL to share with your team.

Limitations
- SQLite is fine for small teams. For higher concurrency, consider migrating to Postgres (e.g., Supabase) and object storage for images.
- To migrate later, we can refactor `db.py` to use Postgres and move images to a bucket while keeping the UI unchanged.
