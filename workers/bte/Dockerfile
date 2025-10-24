# Use RENCI python base image
FROM ghcr.io/translatorsri/renci-python-image:3.11.5

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

COPY ./workers/bte/requirements.txt .
RUN pip install -r requirements.txt

# switch to the non-root user (nru). defined in the base image
USER nru

# Copy in files
COPY ./workers/bte ./

# Set up base for command and any variables
# that shouldn't be modified
# ENTRYPOINT ["uvicorn", "shepherd_server.server:APP"]

# Variables that can be overriden
CMD ["python", "worker.py"]
