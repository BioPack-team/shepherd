# Use RENCI python base image
FROM ghcr.io/translatorsri/renci-python-image:3.12.13

# Add image info
LABEL org.opencontainers.image.source https://github.com/BioPack-team/shepherd

ENV PYTHONHASHSEED=0

# set up requirements
WORKDIR /app

# make sure all is writeable for the nru USER later on
RUN chmod -R 777 .

# Install requirements
COPY ./shepherd_utils ./shepherd_utils
COPY ./pyproject.toml .
RUN pip install .

COPY ./workers/aragorn_lookup/requirements.txt .
RUN pip install -r requirements.txt

# switch to the non-root user (nru). defined in the base image
USER nru

# Copy in files. The worker is a package (worker.py imports its sibling
# query_templates/probe modules), so it keeps its path under ./workers and is
# started with -m rather than being flattened into /app.
COPY ./workers/__init__.py ./workers/__init__.py
COPY ./workers/aragorn_lookup ./workers/aragorn_lookup

# Set up base for command and any variables
# that shouldn't be modified
# ENTRYPOINT ["uvicorn", "shepherd_server.server:APP"]

# Variables that can be overriden
CMD ["python", "-m", "workers.aragorn_lookup.worker"]
