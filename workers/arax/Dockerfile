# Use RENCI python base image
FROM ghcr.io/translatorsri/renci-python-image:3.12.13

# Add image info
LABEL org.opencontainers.image.source https://github.com/BioPack-team/shepherd

# ARAX builds several lists from sets, so its output depends on the hash seed
ENV PYTHONHASHSEED=0
# Dump every thread's traceback on SIGABRT, including in a pool child that hangs
# during import (see ProcessPoolManager's timeout handling)
ENV PYTHONFAULTHANDLER=1

# set up requirements
WORKDIR /app

# make sure all is writeable for the nru USER later on
RUN chmod -R 777 .

# Install requirements
COPY ./shepherd_utils ./shepherd_utils
COPY ./pyproject.toml .
RUN pip install .

COPY ./workers/arax/requirements.txt .
RUN pip install -r requirements.txt

# switch to the non-root user (nru). defined in the base image
# bmt imports bioregistry, which creates a PyStow data dir at import time;
# nru's home is not writable, so point it at /tmp (as score_paths does)
ENV PYSTOW_HOME=/tmp/pystow
USER nru

# Copy in files
COPY ./workers/arax ./

# Set up base for command and any variables
# that shouldn't be modified
# ENTRYPOINT ["uvicorn", "shepherd_server.server:APP"]

# Variables that can be overriden
CMD ["python", "worker.py"]
