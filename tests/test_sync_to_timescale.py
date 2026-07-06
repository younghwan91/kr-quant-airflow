"""Regression test for sync_to_timescale.py's sqlite -> TimescaleDB upsert path.

SUPPLY_DEMAND_COLS must stay in sync with kr_quant/storage.py's actual sqlite
column names (code/date/close/flu_rt/acc_trde_qty/individual/foreign_/
institution/...). This test locks that shape in against a fixture db.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from sync_to_timescale import SUPPLY_DEMAND_COLS, sync_table  # noqa: E402


def _make_sqlite(tmp_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(tmp_path / "fixture.db")
    ddl_cols = ",\n".join(
        f"{c} TEXT" if c in ("code", "date") else f"{c} INTEGER" for c in SUPPLY_DEMAND_COLS
    )
    con.execute(f"CREATE TABLE supply_demand ({ddl_cols})")
    row = ["005930", "20260701"] + [0] * (len(SUPPLY_DEMAND_COLS) - 2)
    con.execute(f"INSERT INTO supply_demand VALUES ({','.join('?' * len(SUPPLY_DEMAND_COLS))})", row)
    con.commit()
    return con


def test_sync_table_upserts_cleanly(tmp_path: Path) -> None:
    """A fixture matching kr-quant's actual sqlite schema must sync without error.

    execute_values itself is psycopg2's own (already well-tested) code, so it's
    patched out here — this test only needs to prove sync_table's column
    selection matches the real sqlite schema.
    """
    sq = _make_sqlite(tmp_path)
    pg = MagicMock()

    with patch("sync_to_timescale.psycopg2.extras.execute_values") as execute_values:
        n = sync_table(sq, pg, "supply_demand", SUPPLY_DEMAND_COLS, "19000101")

    assert n == 1
    execute_values.assert_called_once()
    pg.commit.assert_called_once()
