# Built on the shared base (shepherd_base/Dockerfile): the Python image,
# runtime settings and Shepherd's dependencies. Layers run from least to most
# often changed, so a Shepherd code change rebuilds only the last few.
FROM shepherd_base

LABEL org.opencontainers.image.source=https://github.com/BioPack-team/shepherd

# The worker is a package (worker, mcq, store, build_duckdb) with relative
# imports, so it runs as a module from /app.
ENV PYTHONPATH=/app

COPY ./workers/aragorn_coalesce/requirements.txt .
RUN pip install -r requirements.txt

# Shepherd's code; its dependencies are in the base already
COPY ./pyproject.toml .
COPY ./shepherd_utils ./shepherd_utils
RUN pip install --no-deps .

COPY ./workers/__init__.py ./workers/__init__.py
COPY ./workers/aragorn_coalesce ./workers/aragorn_coalesce

# Release version (git tag), injected by .github/workflows/release.yml.
# Reported in the worker heartbeat so the monitor can show which image
# version each replica runs. Local builds default to "dev". Declared last so
# a new version doesn't invalidate the layers above.
ARG SHEPHERD_VERSION=dev
ENV SHEPHERD_VERSION=${SHEPHERD_VERSION}

# the non-root user, defined in the RENCI base image
USER nru

CMD ["python", "-m", "workers.aragorn_coalesce.worker"]
