# kr-quant isn't published to PyPI (see kr-quant/README.md), so it's
# installed editable from the sibling repo mounted at /opt/kr-quant — done
# at container start (entrypoint-wrapper.sh), not build time, so a
# `git pull` on the host picks up new collector code without an image rebuild.
FROM apache/airflow:2.10.5-python3.11

USER airflow
COPY docker/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY docker/entrypoint-wrapper.sh /entrypoint-wrapper.sh
USER root
RUN chmod +x /entrypoint-wrapper.sh
USER airflow

ENTRYPOINT ["/entrypoint-wrapper.sh"]
