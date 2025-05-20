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
ADD shepherd_utils/ ./shepherd_utils
RUN pip install ./shepherd_utils/

ADD shepherd_server/ ./shepherd_server
RUN pip install -e ./shepherd_server


# switch to the non-root user (nru). defined in the base image
USER nru

# Copy in files
# ADD shepherd_server/ ./shepherd_ser

# Set up base for command and any variables
# that shouldn't be modified
# ENTRYPOINT ["uvicorn", "shepherd_server.server:APP"]

# Variables that can be overriden
CMD ["python", "-m", "uvicorn", "--host", "0.0.0.0", "--port", "5439", "shepherd_server.server:APP"]
