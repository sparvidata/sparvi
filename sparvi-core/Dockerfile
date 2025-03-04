FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install sparvi-core
COPY . .
RUN pip install --no-cache-dir -e .

# Create a non-root user
RUN useradd -m sparvi
USER sparvi

ENTRYPOINT ["sparvi"]
CMD ["--help"]