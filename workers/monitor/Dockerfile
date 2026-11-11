# Built on the shared base (shepherd_base/Dockerfile): the Python image,
# runtime settings and Shepherd's dependencies. Layers run from least to most
# often changed, so a Shepherd code change rebuilds only the last few.
FROM shepherd_base

LABEL org.opencontainers.image.source=https://github.com/BioPack-team/shepherd

# The monitor runs as the workers.monitor package from /app.
ENV PYTHONPATH=/app

COPY ./workers/monitor/requirements.txt .
RUN pip install -r requirements.txt

# Shepherd's code; its dependencies are in the base already
COPY ./pyproject.toml .
COPY ./shepherd_utils ./shepherd_utils
RUN pip install --no-deps .

COPY ./workers/__init__.py ./workers/__init__.py
COPY ./workers/monitor ./workers/monitor
COPY ./workers/monitor/monitor_alerts.yaml /app/monitor_alerts.yaml

# Release version (git tag), injected by .github/workflows/release.yml.
# Reported in the worker heartbeat so the monitor can show which image
# version each replica runs. Local builds default to "dev". Declared last so
# a new version doesn't invalidate the layers above.
ARG SHEPHERD_VERSION=dev
ENV SHEPHERD_VERSION=${SHEPHERD_VERSION}

# the non-root user, defined in the RENCI base image
USER nru

EXPOSE 5440
CMD ["python", "-m", "workers.monitor.worker"]
