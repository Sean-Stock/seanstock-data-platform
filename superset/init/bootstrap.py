#!/usr/bin/env python3
"""
Idempotent Superset bootstrap:
  1. Registers the read-only DuckDB database connection pointing to /data/views.duckdb
  2. Registers all Gold views as Superset physical datasets ready for charting
  3. Grants Gamma read access to the warehouse so Embedded Guest Tokens work cleanly
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from superset.app import create_app

GOLD_VIEWS = [
    "gold_national_intensity",
    "gold_regional_mix",
    "gold_national_generation",
    "gold_northwest_weather_energy",
    "gold_pipeline_runs",
]


def main() -> None:
    app = create_app()
    with app.app_context():
        from superset import db, security_manager
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database

        data_dir = Path(os.environ.get("DATA_DIR", "/data"))
        db_file = data_dir / "views.duckdb"
        sqlalchemy_uri = f"duckdb:///{db_file}"
        extra = json.dumps(
            {
                "engine_params": {
                    "connect_args": {"read_only": True},
                },
                "allows_virtual_table_explore": True,
            }
        )

        database = db.session.query(Database).filter_by(database_name="UK Grid Warehouse (DuckDB)").first()
        if not database:
            database = Database(
                database_name="UK Grid Warehouse (DuckDB)",
                sqlalchemy_uri=sqlalchemy_uri,
                extra=extra,
                expose_in_sqllab=True,
                allow_ctas=False,
                allow_cvas=False,
                allow_dml=False,
            )
            db.session.add(database)
            db.session.commit()
            print(f"[bootstrap] Created database connection: {sqlalchemy_uri}")
        else:
            database.sqlalchemy_uri = sqlalchemy_uri
            database.extra = extra
            db.session.commit()
            print(f"[bootstrap] Updated database connection: {sqlalchemy_uri}")

        if db_file.exists():
            for view_name in GOLD_VIEWS:
                tbl = (
                    db.session.query(SqlaTable)
                    .filter_by(table_name=view_name, database_id=database.id)
                    .first()
                )
                if not tbl:
                    tbl = SqlaTable(table_name=view_name, database=database)
                    db.session.add(tbl)
                    db.session.commit()
                    print(f"[bootstrap] Registered dataset: {view_name}")
                try:
                    tbl.fetch_metadata()
                    db.session.commit()
                except Exception as exc:
                    print(f"[bootstrap] Warning refreshing metadata for {view_name}: {exc}")
        else:
            print(f"[bootstrap] {db_file} does not exist yet; run Meltano CronJob then re-run bootstrap.")

        # Ensure Gamma role can query the DuckDB database (required for Embedded Guest Tokens)
        gamma_role = security_manager.find_role("Gamma")
        db_perm = security_manager.find_permission_view_menu("database_access", database.perm)
        if gamma_role and db_perm and db_perm not in gamma_role.permissions:
            gamma_role.permissions.append(db_perm)
            db.session.commit()
            print("[bootstrap] Granted Gamma role read access to UK Grid Warehouse (DuckDB)")


if __name__ == "__main__":
    main()
