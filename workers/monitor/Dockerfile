FROM ghcr.io/translatorsri/renci-python-image:3.12.13

LABEL org.opencontainers.image.source https://github.com/BioPack-team/shepherd

ENV PYTHONHASHSEED=0

# Make the ``workers`` package resolvable for ``python -m workers.monitor.worker``
# regardless of the working directory the runtime launches the container with.
# ``python -m`` only adds the *current* working directory to sys.path, so without
# this an environment that overrides the working dir fails with
# "No module named 'workers'".
ENV PYTHONPATH=/app

WORKDIR /app

RUN chmod -R 777 .

COPY ./shepherd_utils ./shepherd_utils
COPY ./pyproject.toml .
RUN pip install .

COPY ./workers/monitor/requirements.txt .
RUN pip install -r requirements.txt

USER nru

# Package the worker so ``workers.monitor.worker:APP`` resolves under uvicorn.
COPY ./workers/__init__.py ./workers/__init__.py
COPY ./workers/monitor ./workers/monitor

# Default alert config; mountable via volume to override without rebuilding.
COPY ./workers/monitor/monitor_alerts.yaml /app/monitor_alerts.yaml

EXPOSE 5440

CMD ["python", "-m", "workers.monitor.worker"]
