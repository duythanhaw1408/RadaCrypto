FROM python:3.12-slim

WORKDIR /app

# Copy project files
COPY pyproject.toml ./
COPY src/ src/
COPY configs/ configs/
COPY fixtures/ fixtures/
COPY scripts/ scripts/
COPY sql/ sql/
COPY .env.example .env

# Install dependencies
RUN python -m pip install --no-cache-dir -e .

# Initialize SQLite
RUN python scripts/init_sqlite_db.py

# Create data directories
RUN mkdir -p data/raw data/state data/thesis data/review data/replay

# Default: run a full cycle (scan → review)
ENV PYTHONPATH=src
CMD ["python", "-m", "cfte.cli.main", "run-scan"]
