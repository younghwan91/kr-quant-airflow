"""Schema-drift regression test for sync_to_timescale.py.

2026-07-06: a local sqlite `supply_demand` table created under an old
storage.py schema (columns `individual`/`foreign_`/`institution`) silently
stopped collecting for ~3 weeks once storage.py renamed those columns to
`ind_invsr`/`frgnr_invsr`/`orgn` — the collector's broad except swallowed the
write failure. This test locks in the correct behavior at the sync layer:
a legacy-schema db must fail loudly (not silently), a migrated one must not.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from sync_to_timescale import SUPPLY_DEMAND_COLS, sync_table  # noqa: E402

_NEW_TO_LEGACY = {"ind_invsr": "individual", "frgnr_invsr": "foreign_", "orgn": "institution"}


def _make_sqlite(tmp_path: Path, legacy: bool) -> sqlite3.Connection:
    # SUPPLY_DEMAND_COLS is already the *current* (new) schema; for the legacy
    # fixture we rename the 3 drifted columns back to their old names.
    cols = [_NEW_TO_LEGACY.get(c, c) if legacy else c for c in SUPPLY_DEMAND_COLS]
    con = sqlite3.connect(tmp_path / "fixture.db")
    ddl_cols = ",\n".join(f"{c} TEXT" if c in ("code", "date") else f"{c} INTEGER" for c in cols)
    con.execute(f"CREATE TABLE supply_demand ({ddl_cols})")
    row = ["005930", "20260701"] + [0] * (len(cols) - 2)
    con.execute(f"INSERT INTO supply_demand VALUES ({','.join('?' * len(cols))})", row)
    con.commit()
    return con


def test_legacy_schema_fails_loudly(tmp_path: Path) -> None:
    """Old individual/foreign_/institution columns must raise, not silently no-op."""
    sq = _make_sqlite(tmp_path, legacy=True)
    pg = MagicMock()

    with pytest.raises(sqlite3.OperationalError, match="ind_invsr"):
        sync_table(sq, pg, "supply_demand", SUPPLY_DEMAND_COLS, "19000101")

    pg.commit.assert_not_called()


def test_migrated_schema_upserts_cleanly(tmp_path: Path) -> None:
    """New ind_invsr/frgnr_invsr/orgn columns must sync without error.

    execute_values itself is psycopg2's own (already well-tested) code, so it's
    patched out here — this test only needs to prove sync_table's sqlite-side
    column selection succeeds against a migrated schema, unlike the legacy one.
    """
    sq = _make_sqlite(tmp_path, legacy=False)
    pg = MagicMock()

    with patch("sync_to_timescale.psycopg2.extras.execute_values") as execute_values:
        n = sync_table(sq, pg, "supply_demand", SUPPLY_DEMAND_COLS, "19000101")

    assert n == 1
    execute_values.assert_called_once()
    pg.commit.assert_called_once()
