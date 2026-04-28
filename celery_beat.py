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
    # Configurer les tâches périodiques
    from celery.schedules import crontab
    
    celery_app.conf.beat_schedule = {
        'cleanup-old-datasets': {
            'task': 'app.tasks.data_tasks.cleanup_old_datasets',
            'schedule': crontab(hour=2, minute=0),  # Tous les jours à 2h du matin
        },
        'update-quality-scores': {
            'task': 'app.tasks.data_tasks.update_quality_scores',
            'schedule': crontab(hour=3, minute=0),  # Tous les jours à 3h du matin
        },
    }
    
    # Lancer le beat scheduler
    celery_app.start([
        "beat",
        "--loglevel=info",
    ])
