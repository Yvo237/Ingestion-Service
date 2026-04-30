from celery import current_task
from app.celery_app import celery_app
import pandas as pd
import json
import io
from typing import Dict, Any
from app.services.data_profiler import DataProfiler
from app.services.data_processor import DataProcessor
from app.infrastructure.database.config import get_db
from app.infrastructure.database.models import DatasetModel, ProcessingLogModel
from sqlalchemy.orm import Session
from datetime import datetime
import logging
import os
import re
import redis
import io
import math
from app.infrastructure.clients.analysis_client import AnalysisClient
from app.utils.helpers import clean_column_name, sanitize_json

logger = logging.getLogger(__name__)

# Configuration Redis pour le broadcast de logs en temps réel
redis_client = redis.from_url(os.getenv("CELERY_BROKER_URL", "redis://redis_queue:6379/0"))

def broadcast_log(record_id: int, message: str, level: str = "INFO", step: str = "system"):
    """Envoie un log en temps réel via Redis Pub/Sub"""
    try:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            "details": {"analysis_type": step, "dataset_id": record_id}
        }
        redis_client.publish(f"logs:{record_id}", json.dumps(log_entry))
    except Exception as e:
        logger.error(f"Failed to broadcast log: {e}")



@celery_app.task
def cleanup_old_datasets():
    """
    Tâche périodique pour nettoyer les anciens datasets rejetés
    """
    try:
        db = next(get_db())
        
        # Supprimer les datasets rejetés de plus de 30 jours
        cutoff_date = datetime.utcnow() - pd.Timedelta(days=30)
        
        old_datasets = db.query(DatasetModel).filter(
            DatasetModel.status == "rejected",
            DatasetModel.created_at < cutoff_date
        ).all()
        
        deleted_count = len(old_datasets)
        
        for dataset in old_datasets:
            db.delete(dataset)
        
        db.commit()
        
        logger.info(f"Cleaned up {deleted_count} old rejected datasets")
        
        return {"deleted_count": deleted_count, "status": "completed"}
        
    except Exception as e:
        logger.error(f"Error in cleanup task: {str(e)}")
        raise


@celery_app.task
def update_quality_scores():
    """
    Tâche périodique pour mettre à jour les scores de qualité
    """
    try:
        logger.info("Quality scores update task completed")
        
        return {"status": "completed", "updated_count": 0}
        
    except Exception as e:
        logger.error(f"Error in quality scores update task: {str(e)}")
        raise


@celery_app.task(bind=True)
def process_and_analyze_task(self, record_id: int, file_path: str, analysis_endpoint: str, params: Dict[str, Any]):
    """
    Pipeline asynchrone LAN v2 : Nettoyage -> Analyse ML -> Stockage -> Notification
    """
    db = next(get_db())
    notification_service = None
    analysis_client = AnalysisClient()
    
    try:
        # 1. Récupérer l'enregistrement
        dataset = db.query(DatasetModel).filter(DatasetModel.id == record_id).first()
        if not dataset:
            logger.error(f"Record {record_id} not found")
            return
        
        dataset.status = "processing"
        db.commit()
        
        self.update_state(state="PROCESSING", meta={"progress": 5, "status": "Initialisation"})
        
        broadcast_log(record_id, "Début du traitement du dataset...", "INFO", "system")
        lineage = [{"step": "start", "message": "Initialisation du pipeline", "timestamp": datetime.utcnow().isoformat()}]
        
        # 2. Charger le CSV
        if not os.path.exists(file_path):
            # Essayer avec l'extension .csv si elle manque
            if not file_path.endswith('.csv'):
                file_path += '.csv'
            
            if not os.path.exists(file_path):
                broadcast_log(record_id, f"Fichier source non trouvé : {file_path}", "ERROR", "io")
                raise FileNotFoundError(f"Fichier non trouvé : {file_path}")
            
        broadcast_log(record_id, "Chargement des données CSV...", "INFO", "io")
        df = pd.read_csv(file_path)
        lineage.append({"step": "loading", "message": f"Chargement réussi ({len(df)} lignes)", "timestamp": datetime.utcnow().isoformat()})
        self.update_state(state="PROCESSING", meta={"progress": 20, "status": "Données chargées"})
        
        # 3. Nettoyage (CPU intensif)
        broadcast_log(record_id, "Nettoyage et profilage des données...", "INFO", "clean")
        processor = DataProcessor()
        profiler = DataProfiler()
        profile_result = profiler.analyze_dataset(df, dataset.name)
        cleaned_df, cleaning_lineage = processor.process_dataset(df, profile_result)
        
        lineage.append({"step": "cleaning", "message": "Nettoyage terminé", "timestamp": datetime.utcnow().isoformat()})
        self.update_state(state="PROCESSING", meta={"progress": 50, "status": "Nettoyage terminé"})
        
        # Convertir en bytes pour l'envoi au service d'analyse
        csv_buffer = io.StringIO()
        cleaned_df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode('utf-8')
        
        # 4. Analyse ML (Appel au microservice Analytics)
        broadcast_log(record_id, f"Envoi au moteur ML ({analysis_endpoint})...", "INFO", "ml")
        
        # Remapper les colonnes dans params
        cleaned_params = params.copy()
        for key in ["x_columns", "feature_columns"]:
            if key in cleaned_params and isinstance(cleaned_params[key], list):
                cleaned_params[key] = [clean_column_name(c) for c in cleaned_params[key]]
        
        for key in ["y_column", "target_column"]:
            if key in cleaned_params and isinstance(cleaned_params[key], str):
                cleaned_params[key] = clean_column_name(cleaned_params[key])
        
        try:
            analysis_results = analysis_client.sync_call_analysis(analysis_endpoint, csv_bytes, cleaned_params)
            broadcast_log(record_id, "Analyse terminée avec succès", "SUCCESS", "ml")
            self.update_state(state="PROCESSING", meta={"progress": 90, "status": "Analyse ML terminée"})
        except Exception as api_err:
            broadcast_log(record_id, f"Échec de l'analyse : {str(api_err)}", "ERROR", "ml")
            raise Exception(f"Erreur lors de l'analyse ML : {str(api_err)}")
        
        # 5. Sauvegarde des fichiers et mise à jour de la base de données
        storage_dir = "/app/storage/processed"
        os.makedirs(storage_dir, exist_ok=True)
        
        cleaned_csv_path = os.path.join(storage_dir, f"cleaned_{record_id}.csv")
        results_json_path = os.path.join(storage_dir, f"results_{record_id}.json")
        
        # Sauvegarder le CSV complet sur disque pour éviter de saturer la DB
        cleaned_df.to_csv(cleaned_csv_path, index=False)
        
        # Sauvegarder les résultats ML complets sur disque
        with open(results_json_path, 'w') as f:
            json.dump(sanitize_json(analysis_results), f)
            
        dataset.status = "analyzed"
        dataset.headers = cleaned_df.columns.tolist()
        dataset.row_count = len(cleaned_df)
        dataset.storage_path = cleaned_csv_path
        dataset.results_path = results_json_path
        
        # Garder seulement un aperçu (100 premières lignes) en DB pour l'UI
        preview_data = cleaned_df.head(100).to_dict(orient='records')
        
        dataset.cleaned_data = sanitize_json(preview_data)
        # On ne garde qu'un résumé ou les métriques clés en DB pour analysis_results
        summary_results = {k: v for k, v in analysis_results.items() if not isinstance(v, (list, dict))}
        dataset.analysis_results = sanitize_json(summary_results)
        
        dataset.quality_score = profile_result.get('quality_score', 0.8)
        dataset.processing_log = sanitize_json(lineage)
        
        db.commit()
        broadcast_log(record_id, "Pipeline terminé, résultats enregistrés", "SUCCESS", "system")
        
        return {"status": "success", "record_id": record_id}
        
    except Exception as e:
        logger.error(f"Error in process_and_analyze_task for record {record_id}: {str(e)}")
        # Rollback important pour débloquer la session SQLAlchemy
        try:
            db.rollback()
        except:
            pass
            
        # Re-fetch dataset to avoid session issues
        try:
            dataset = db.query(DatasetModel).filter(DatasetModel.id == record_id).first()
            if dataset:
                dataset.status = "failed"
                db.commit()
        except Exception as db_err:
            logger.error(f"Failed to update dataset status to failed: {db_err}")
        
        raise e
    finally:
        db.close()
