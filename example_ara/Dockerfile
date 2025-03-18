FROM ubuntu:latest AS utils

COPY ./shepherd_utils ./shepherd_utils

# Use RENCI python base image
FROM ghcr.io/translatorsri/renci-python-image:3.11.5

# Add image info
LABEL org.opencontainers.image.source https://github.com/BioPack-team/shepherd

ENV PYTHONHASHSEED=0
ARG POSTGRES_PASSWORD=supersecretpassw0rd
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD

# set up requirements
WORKDIR /app

# make sure all is writeable for the nru USER later on
RUN chmod -R 777 .

# Install requirements
COPY --from=utils ./shepherd_utils ./shepherd_utils
RUN pip install ./shepherd_utils/

COPY ./example_ara/requirements.txt .
RUN pip install -r requirements.txt

# switch to the non-root user (nru). defined in the base image
USER nru

# Copy in files
COPY ./example_ara ./

# Variables that can be overriden
CMD ["python", "worker.py"]
