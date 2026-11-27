# Built on the shared base (shepherd_base/Dockerfile): the Python image,
# runtime settings and Shepherd's dependencies. Layers run from least to most
# often changed, so a Shepherd code change rebuilds only the last few.
FROM shepherd_base

LABEL org.opencontainers.image.source=https://github.com/BioPack-team/shepherd

COPY ./workers/arax/requirements.txt .
RUN pip install -r requirements.txt

# Shepherd's code; its dependencies are in the base already
COPY ./pyproject.toml .
COPY ./shepherd_utils ./shepherd_utils
RUN pip install --no-deps .

COPY ./workers/arax .

# A fixed string-hash seed, so ARAX answers the same query the same way in
# every process and after every restart. ARAX orders results with equal raw
# scores by Python set order (ARAX_resultify), which follows the hash seed; the
# ranker then gives tied results positional scores and a result cap keeps the
# first ones. Upstream ARAX's order follows its own server's seed, so this makes
# Shepherd reproducible, not identical to upstream (docs/ARAX_PARITY_RUN.md).
ENV PYTHONHASHSEED=0

# Release version (git tag), injected by .github/workflows/release.yml.
# Reported in the worker heartbeat so the monitor can show which image
# version each replica runs. Local builds default to "dev". Declared last so
# a new version doesn't invalidate the layers above.
ARG SHEPHERD_VERSION=dev
ENV SHEPHERD_VERSION=${SHEPHERD_VERSION}

# the non-root user, defined in the RENCI base image
USER nru

CMD ["python", "worker.py"]
