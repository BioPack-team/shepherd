FROM ghcr.io/translatorsri/renci-python-image:3.11.5

LABEL org.opencontainers.image.source https://github.com/BioPack-team/shepherd

ENV PYTHONHASHSEED=0

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
