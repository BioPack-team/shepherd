# Built on the shared base (shepherd_base/Dockerfile): the Python image,
# runtime settings and Shepherd's dependencies. Layers run from least to most
# often changed, so a Shepherd code change rebuilds only the last few.
FROM shepherd_base

LABEL org.opencontainers.image.source=https://github.com/BioPack-team/shepherd

# uvicorn worker-process count (overridable at deploy time). One process
# cannot keep up with concurrent multi-MB ARA callbacks; each worker
# initializes its own tracer provider, lifespan seeding is idempotent, and
# schema upgrades take a Postgres advisory lock, so workers are independent.
ENV WEB_CONCURRENCY=4

# arax-api: what the /arax API needs on top of the base install (see
# pyproject), installed before any code like the base dependencies
RUN python /opt/shepherd/deps.py arax-api > /tmp/requirements.txt \
    && pip install -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# Shepherd's code; its dependencies are in the base already
COPY ./pyproject.toml .
COPY ./shepherd_utils ./shepherd_utils
COPY ./shepherd_server ./shepherd_server
RUN pip install --no-deps -e .

# Release version (git tag), injected by .github/workflows/release.yml.
# Reported in the worker heartbeat so the monitor can show which image
# version each replica runs. Local builds default to "dev". Declared last so
# a new version doesn't invalidate the layers above.
ARG SHEPHERD_VERSION=dev
ENV SHEPHERD_VERSION=${SHEPHERD_VERSION}

# the non-root user, defined in the RENCI base image
USER nru

CMD ["python", "-m", "uvicorn", "--host", "0.0.0.0", "--port", "5439", "shepherd_server.server:APP"]
