"""Puts the repo root on sys.path so tests can ``import collectors`` — mirrors
how the Airflow container reaches it (``sys.path.insert(0, "/opt/airflow")``
in dags/, cwd=/opt/airflow for collector subprocess calls), without needing
collectors/ installed as a package.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
