# Use RENCI python base image
FROM renciorg/renci-python-image:v3.11.5

# Add image info
LABEL org.opencontainers.image.source https://github.com/BioPack-team/shepherd

ENV PYTHONHASHSEED=0

# set up requirements
WORKDIR /app

# make sure all is writeable for the nru USER later on
RUN chmod -R 777 .

# Install requirements
ADD requirements.txt .
RUN pip install -r requirements.txt

# switch to the non-root user (nru). defined in the base image
# USER nru

# Copy in files
ADD . .

# Set up base for command and any variables
# that shouldn't be modified
ENTRYPOINT ["gunicorn", "shepherd.server:APP", "-k", "uvicorn.workers.UvicornWorker", "--timeout", "0"]

# Variables that can be overriden
CMD [ "--bind", "0.0.0.0:5439", "--workers", "4", "--threads", "3"]
