#!/usr/bin/env bash
# Waits for today's scheduled DAG runs (daily_collection, daily_short_credit)
# to reach a terminal state, then stops the stack. Replaces the old fixed
# 22:00 KST `docker compose stop` cron entry so the server doesn't sit up
# for hours after collection already finished. Keeps 22:00 KST as a
# fallback deadline in case a run hangs, so behavior never regresses past
# the old fixed-time shutdown.
set -euo pipefail
cd "$(dirname "$0")/.."

POLL_INTERVAL=60
DEADLINE=$(date -d "22:00" +%s)

today_dow=$(date +%u)  # 1=Mon .. 7=Sun
today=$(date +%Y-%m-%d)

log() { echo "[$(date '+%F %T')] $*"; }

# Per-stock tables have their own day-window caps (supply_demand/credit/short
# ~100d, sector_index ~10d) so they never backfill to 2024 like daily_bars
# does — this just reports how many known stocks are missing *today's* row
# in each table, so a repeat of the 2026-07-08 transaction-cascade failure
# (473 stocks silently dropped) shows up in the log instead of going unnoticed.
report_coverage() {
    local user db
    user=$(grep '^TIMESCALE_USER' .env | cut -d= -f2)
    db=$(grep '^TIMESCALE_DB' .env | cut -d= -f2)
    log "=== 커버리지 점검 ($today) ==="
    docker compose exec -T timescaledb psql -U "$user" -d "$db" -c "
SELECT 'daily_bars' AS tbl, COUNT(*) AS missing_today FROM stocks s
    WHERE NOT EXISTS (SELECT 1 FROM daily_bars d WHERE d.code=s.code AND d.date='$today')
UNION ALL
SELECT 'supply_demand', COUNT(*) FROM stocks s
    WHERE NOT EXISTS (SELECT 1 FROM supply_demand d WHERE d.code=s.code AND d.date='$today')
UNION ALL
SELECT 'credit_balance', COUNT(*) FROM stocks s
    WHERE NOT EXISTS (SELECT 1 FROM credit_balance d WHERE d.code=s.code AND d.date='$today')
UNION ALL
SELECT 'short_selling', COUNT(*) FROM stocks s
    WHERE NOT EXISTS (SELECT 1 FROM short_selling d WHERE d.code=s.code AND d.date='$today')
UNION ALL
SELECT 'sector_index', COUNT(*) FROM (SELECT DISTINCT code FROM sector_index) si
    WHERE NOT EXISTS (SELECT 1 FROM sector_index x WHERE x.code=si.code AND x.date='$today')
UNION ALL
SELECT 'shares_outstanding', COUNT(*) FROM stocks s
    WHERE NOT EXISTS (SELECT 1 FROM shares_outstanding_history d WHERE d.code=s.code AND d.date >= current_date - interval '7 days');
" 2>&1 || log "커버리지 점검 실패 (DB 연결 안 됨?)"
}

pending=()
# schedule="5 10 * * *" in daily_collection_catchup.py — runs every day
pending+=("daily_collection_catchup")
# schedule="0 16 * * 1-5" in daily_collection.py
if [ "$today_dow" -ge 1 ] && [ "$today_dow" -le 5 ]; then
    pending+=("daily_collection")
fi
# schedule="0 10 * * 2-6" in daily_short_credit.py
if [ "$today_dow" -ge 2 ] && [ "$today_dow" -le 6 ]; then
    pending+=("daily_short_credit")
fi
# schedule="10 10 * * 1" in weekly_listed_shares.py
if [ "$today_dow" -eq 1 ]; then
    pending+=("weekly_listed_shares")
fi

if [ ${#pending[@]} -eq 0 ]; then
    log "오늘은 예약된 DAG가 없음 — 바로 종료"
    docker compose stop
    exit 0
fi

log "대기 대상 DAG: ${pending[*]}"

is_done() {
    local dag_id="$1"
    docker compose exec -T airflow-scheduler airflow dags list-runs -d "$dag_id" --output json 2>/dev/null \
        | python3 -c "
import json, sys
today = '$today'
try:
    runs = json.load(sys.stdin)
except Exception:
    sys.exit(1)
todays = [
    r for r in runs
    if str(r.get('logical_date', r.get('execution_date', ''))).startswith(today)
    or str(r.get('start_date', '')).startswith(today)
]
if not todays:
    sys.exit(1)
sys.exit(0 if todays[0].get('state') in ('success', 'failed') else 1)
"
}

while :; do
    remaining=("${pending[@]}")
    pending=()
    for dag_id in "${remaining[@]}"; do
        if is_done "$dag_id"; then
            log "$dag_id 완료"
        else
            pending+=("$dag_id")
        fi
    done

    if [ ${#pending[@]} -eq 0 ]; then
        log "모든 DAG 완료"
        report_coverage
        log "컨테이너 종료"
        docker compose stop
        exit 0
    fi

    if [ "$(date +%s)" -ge "$DEADLINE" ]; then
        log "22:00 안전장치 도달, 미완료(${pending[*]})인 채로 종료"
        report_coverage
        docker compose stop
        exit 0
    fi

    sleep "$POLL_INTERVAL"
done
