# Shared base for the Shepherd server and every worker image (#223 C11-5).
#
# Every image used to copy shepherd_utils before `pip install .`, so any code
# change reinstalled all of Shepherd's dependencies in all ~28 images. This
# image holds the Python runtime settings and those dependencies only -- read
# from pyproject.toml, no Shepherd code -- so it rebuilds only when
# pyproject.toml (or the Python image) changes. Each service image then adds,
# in order, its own requirements, Shepherd's code (installed --no-deps) and its
# own files.
#
# Built as the `shepherd_base` named context: compose builds it through
# `additional_contexts`, the release workflow pushes it first and passes it in
# with `build-contexts`. For a plain `docker build`, tag it first:
#   docker build -f shepherd_base/Dockerfile -t shepherd_base .
ARG PYTHON_IMAGE=ghcr.io/translatorsri/renci-python-image:3.12.13
FROM ${PYTHON_IMAGE}

LABEL org.opencontainers.image.source=https://github.com/BioPack-team/shepherd

ENV PYTHONHASHSEED=0 \
    # Dump every thread's traceback on SIGABRT, including in a pool child
    # that hangs during import (see ProcessPoolManager's timeout handling).
    PYTHONFAULTHANDLER=1 \
    # bmt imports bioregistry, which creates a PyStow data dir at import time;
    # nru's home is not writable.
    PYSTOW_HOME=/tmp/pystow \
    # No wheel cache in the image layers.
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
# Writable by nru (the base image's non-root user) and group 0, for the data
# the workers download at runtime (/app/omnicorp_lmdb, /app/arax_dbs, ...).
# It was `chmod -R 777 .`: world-writable, and in some images run after the
# code was copied, which left the code writable too.
RUN chown nru:0 /app && chmod 775 /app

COPY ./shepherd_base/deps.py ./pyproject.toml /opt/shepherd/
RUN python /opt/shepherd/deps.py > /tmp/requirements.txt \
    && pip install -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
