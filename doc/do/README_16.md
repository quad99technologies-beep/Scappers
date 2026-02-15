# Testing / One-off Scripts

Scripts here are for **testing**, **debugging**, or **one-time fixes**. They are not part of the main pipeline.

- **Root** – DB checks, PCID reload, statistics, migrations, GUI apply
- **Argentina/** – Loop/reset fixes, set latest run for resume
- **Russia/** – Progress logs, schema migrations

Run from repo root, e.g. `python testing/check_db.py` or `python testing/Argentina/reset_loop_count.py`.
