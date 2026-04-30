# Use RENCI python base image
FROM ghcr.io/translatorsri/renci-python-image:3.11.5

# Add image info
LABEL org.opencontainers.image.source https://github.com/BioPack-team/shepherd

ARG DEVICE=gpu

ENV PYTHONHASHSEED=0
ENV HF_HOME=/app/hf_cache

# set up requirements
WORKDIR /app

COPY ./workers/score_paths/requirements-cpu.txt ./workers/score_paths/requirements-gpu.txt ./
RUN pip install --no-cache-dir --timeout=300 --retries=5 -r requirements-${DEVICE}.txt
# pre-download the model
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('cambridgeltl/SapBERT-from-PubMedBERT-fulltext')"

# Install shared utilities
COPY ./shepherd_utils ./shepherd_utils
COPY ./pyproject.toml .
RUN pip install .

# Copy in files and make sure they are readable
COPY ./workers/score_paths .
RUN chmod -R 777 .

# switch to the non-root user (nru). defined in the base image
ENV PYSTOW_HOME=/tmp/pystow
USER nru

# Variables that can be overriden
CMD ["python", "worker.py"]
