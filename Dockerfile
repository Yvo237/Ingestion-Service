# syntax=docker/dockerfile:1.6

############################
# Stage 1: Builder
############################
FROM python:3.13-slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./

# Installer setuptools en premier pour pkg_resources
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir setuptools>=65.0.0

# Installer les dépendances Python
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements.txt

############################
# Stage 2: Development
############################
FROM python:3.13-slim-bookworm AS development

RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copier les dépendances
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copier le code source
COPY . .

ENV PATH="/usr/local/bin:${PATH}"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8001

# Commande par défaut pour l'API
CMD ["uvicorn", "app.adapters.main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]

############################
# Stage 3: Production
############################
FROM python:3.13-slim-bookworm AS production

RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copier les dépendances du builder
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copier le code source
COPY . .

EXPOSE 8001

CMD ["python", "-m", "app.adapters.main"]
