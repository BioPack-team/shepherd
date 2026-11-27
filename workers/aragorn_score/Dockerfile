# The Rust scorer (workers/aragorn_score/rust; ARAGORN_SCORER chooses it),
# built into a Python wheel in maturin's manylinux image, so the wheel runs on
# any glibc-based Python image.
FROM ghcr.io/pyo3/maturin:v1.7.4 AS rust_scorer
WORKDIR /build
# The version in rust-toolchain.toml, installed explicitly: not every rustup
# installs a toolchain file's version on its own.
RUN rustup toolchain install 1.97.0 --profile minimal
COPY ./workers/aragorn_score/rust .
RUN maturin build --release --compatibility manylinux2014 --out /wheels

# Built on the shared base (shepherd_base/Dockerfile): the Python image,
# runtime settings and Shepherd's dependencies. Layers run from least to most
# often changed, so a Shepherd code change rebuilds only the last few.
FROM shepherd_base

LABEL org.opencontainers.image.source=https://github.com/BioPack-team/shepherd

COPY ./workers/aragorn_score/requirements.txt .
RUN pip install -r requirements.txt

# The aragorn_score_rs module, from the stage above.
COPY --from=rust_scorer /wheels /tmp/wheels
RUN pip install /tmp/wheels/*.whl && rm -rf /tmp/wheels

# Shepherd's code; its dependencies are in the base already
COPY ./pyproject.toml .
COPY ./shepherd_utils ./shepherd_utils
RUN pip install --no-deps .

COPY ./workers/aragorn_score .

# Release version (git tag), injected by .github/workflows/release.yml.
# Reported in the worker heartbeat so the monitor can show which image
# version each replica runs. Local builds default to "dev". Declared last so
# a new version doesn't invalidate the layers above.
ARG SHEPHERD_VERSION=dev
ENV SHEPHERD_VERSION=${SHEPHERD_VERSION}

# the non-root user, defined in the RENCI base image
USER nru

CMD ["python", "worker.py"]
