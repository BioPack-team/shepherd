# Use RENCI python base image
FROM ghcr.io/translatorsri/renci-python-image:3.11.5

# Add image info
LABEL org.opencontainers.image.source https://github.com/BioPack-team/shepherd

ENV PYTHONHASHSEED=0
ENV HF_HOME=/app/hf_cache

# set up requirements
WORKDIR /app

# Install requirements
COPY ./shepherd_utils ./shepherd_utils
COPY ./pyproject.toml .
RUN pip install .

COPY ./workers/score_paths/requirements.txt .
RUN pip install --timeout=3000 --retries=5 -r requirements.txt
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('cambridgeltl/SapBERT-from-PubMedBERT-fulltext')"

# Copy in files and make sure they are readable
COPY ./workers/score_paths .
RUN chmod -R 777 .

# switch to the non-root user (nru). defined in the base image
USER nru

# Variables that can be overriden
CMD ["python", "worker.py"]
