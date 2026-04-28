#!/usr/bin/env python3

import os
import sys
from dotenv import load_dotenv

# Ajouter le répertoire courant au Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Charger les variables d'environnement
load_dotenv()

from app.celery_app import celery_app

if __name__ == "__main__":
    # Lancer le worker Celery
    celery_app.start([
        "worker",
        "--loglevel=info",
        "--concurrency=2",  # 2 workers en parallèle
        "--prefetch-multiplier=1",
        "--max-tasks-per-child=1000",
        "--time-limit=300",  # 5 minutes max par tâche
        "--soft-time-limit=240",  # 4 minutes soft limit
    ])
