#!/usr/bin/env bash
set -e

# kr-quant mounted read-write at /opt/kr-quant (sibling repo on host).
# --no-deps: its deps (kiwoom-client, pandas) are already baked into the
# image; re-resolving them on every container start would just be slow.
pip install --no-deps -e /opt/kr-quant -q

exec /entrypoint "$@"
