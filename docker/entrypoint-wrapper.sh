#!/usr/bin/env bash
set -e

# Collectors are self-contained (./collectors, image deps only) — no editable
# install of kr-quant needed anymore. kr-quant is still mounted read-only at
# /opt/kr-quant for the 2 DAGs that intentionally run its analysis code
# in-place (daily_minervini_scan.py's scanner_final.py, weekly_price_adjust.py's
# kr_quant.price_adjust, both via PYTHONPATH/sys.path, not a package install).

exec /entrypoint "$@"
