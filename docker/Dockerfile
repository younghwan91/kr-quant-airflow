# Collectors (./collectors, bind-mounted like dags/scripts) are self-contained
# and no longer need kr-quant installed — see docker/entrypoint-wrapper.sh and
# collectors/storage.py. kr-quant (private repo) is still bind-mounted at
# /opt/kr-quant for 2 DAGs that intentionally run its analysis code in-place
# (daily_minervini_scan.py, weekly_price_adjust.py), reached via
# PYTHONPATH/sys.path rather than a package install.
FROM apache/airflow:2.10.5-python3.11

USER airflow
COPY docker/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY docker/entrypoint-wrapper.sh /entrypoint-wrapper.sh
USER root
RUN chmod +x /entrypoint-wrapper.sh
USER airflow

ENTRYPOINT ["/entrypoint-wrapper.sh"]
