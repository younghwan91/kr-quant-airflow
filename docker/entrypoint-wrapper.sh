#!/usr/bin/env bash
set -e

# Collectors are self-contained (./collectors, image deps only) — no editable
# install of kr-quant needed anymore. kr-quant is still mounted read-only at
# /opt/kr-quant for the 1 DAG that intentionally runs its analysis code
# in-place (weekly_price_adjust.py's kr_quant.price_adjust, via
# PYTHONPATH/sys.path, not a package install).

exec /entrypoint "$@"
