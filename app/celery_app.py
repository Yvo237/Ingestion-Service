from celery import Celery
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration Celery
celery_app = Celery(
    "data_processing",
    broker=os.getenv("CELERY_BROKER_URL", "redis://redis_queue:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis_queue:6379/0"),
    include=["app.tasks.data_tasks", "app.tasks.kaggle_tasks"]
)

# Configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes timeout
    task_soft_time_limit=25 * 60,  # 25 minutes soft timeout
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
)
