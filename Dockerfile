FROM quay.io/jupyter/pyspark-notebook:2026-02-23

COPY --chown=jovyan:users requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip setuptools wheel

RUN pip install --no-cache-dir -r /tmp/requirements.txt

EXPOSE 8888 8501

WORKDIR /home/jovyan/work