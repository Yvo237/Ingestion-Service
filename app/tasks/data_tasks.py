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

logger = logging.getLogger(__name__)


@celery_app.task(bind=True)
def process_dataset_task(self, dataset_id: int, csv_content: str, user_id: str, 
                        dataset_name: str, analysis_type: str, analysis_parameters: Dict[str, Any]):
    """
    Tâche Celery pour traiter un dataset de manière asynchrone
    """
    try:
        # Mettre à jour le statut du dataset
        db = next(get_db())
        dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
        
        if not dataset:
            raise ValueError(f"Dataset {dataset_id} not found")
        
        # Mettre à jour le statut en "processing"
        dataset.status = "processing"
        db.commit()
        
        # Mettre à jour la progression de la tâche
        self.update_state(state="PROCESSING", meta={"progress": 10, "status": "Starting processing"})
        
        # Étape 1: Charger et parser le CSV
        df = pd.read_csv(io.StringIO(csv_content))
        
        # Mettre à jour la progression
        self.update_state(state="PROCESSING", meta={"progress": 20, "status": "CSV loaded"})
        
        # Étape 2: Profiling avec data profiler
        profiler = DataProfiler()
        profile_result = profiler.analyze_dataset(df, dataset_name)
        
        # Mettre à jour la progression
        self.update_state(state="PROCESSING", meta={"progress": 40, "status": "Profiling completed"})
        
        # Étape 3: Traitement et nettoyage
        processor = DataProcessor()
        cleaned_df, lineage = processor.process_dataset(df, profile_result)
        
        # Mettre à jour la progression
        self.update_state(state="PROCESSING", meta={"progress": 60, "status": "Data cleaning completed"})
        
        # Étape 4: Calculer les hashes
        file_hash = profiler.generate_file_hash(csv_content)
        content_hash = profiler.generate_content_hash(df)
        
        # Étape 5: Vérification anti-doublons
        is_duplicate = False
        duplicate_reason = None
        
        # Vérifier dans la base locale
        existing_dataset = db.query(DatasetModel).filter(
            DatasetModel.file_hash == file_hash
        ).first()
        
        if existing_dataset and existing_dataset.id != dataset_id:
            is_duplicate = True
            duplicate_reason = "Duplicate in local database"
        
        # Mettre à jour la progression
        self.update_state(state="PROCESSING", meta={"progress": 70, "status": "Duplicate check completed"})
        
        # Étape 6: Préparer les données pour la base
        raw_data_json = df.to_dict(orient='records')
        cleaned_data_json = cleaned_df.to_dict(orient='records')
        
        # Mettre à jour le dataset avec les résultats
        dataset.raw_data = raw_data_json
        dataset.cleaned_data = cleaned_data_json
        dataset.headers = df.columns.tolist()
        dataset.row_count = len(df)
        dataset.file_size = len(csv_content.encode('utf-8'))
        dataset.file_hash = file_hash
        dataset.dataset_metadata = {
            **profile_result['basic_metrics'],
            **profile_result['completeness_metrics'],
            'type_metrics': profile_result['type_metrics'],
            'profiling_summary': {
                'alerts_count': len(profile_result['alerts']),
                'correlations_count': len(profile_result['correlations']),
                'quality_score': profile_result['quality_score']
            }
        }
        dataset.processing_log = lineage
        dataset.quality_score = profile_result['quality_score']
        dataset.analysis_type = analysis_type
        dataset.analysis_parameters = analysis_parameters
        
        # Déterminer le statut final
        if is_duplicate:
            dataset.status = "rejected"
            dataset.kaggle_info = {
                "rejected": True,
                "reason": duplicate_reason,
                "rejected_at": datetime.utcnow().isoformat()
            }
        elif profile_result['quality_score'] >= 8.0:
            dataset.status = "premium"
        elif profile_result['quality_score'] >= 6.0:
            dataset.status = "cleaned"
        else:
            dataset.status = "rejected"
            dataset.kaggle_info = {
                "rejected": True,
                "reason": f"Low quality score: {profile_result['quality_score']}",
                "rejected_at": datetime.utcnow().isoformat()
            }
        
        # Mettre à jour la progression
        self.update_state(state="PROCESSING", meta={"progress": 80, "status": "Status determination completed"})
        
        # Étape 7: Logger les étapes de traitement
        for log_entry in lineage.get('transformations', []):
            processing_log = ProcessingLogModel(
                dataset_id=dataset_id,
                log_level="INFO",
                step=log_entry['step'],
                message=log_entry['action'],
                details=log_entry.get('details', {})
            )
            db.add(processing_log)
        
        # Logger le profilage
        processing_log = ProcessingLogModel(
            dataset_id=dataset_id,
            log_level="INFO",
            step="profiling",
            message="Quality profiling completed",
            details={
                "quality_score": profile_result['quality_score'],
                "alerts_count": len(profile_result['alerts']),
                "completeness_ratio": profile_result['completeness_metrics']['completeness_ratio']
            }
        )
        db.add(processing_log)
        
        # Logger le résultat final
        processing_log = ProcessingLogModel(
            dataset_id=dataset_id,
            log_level="INFO" if not is_duplicate else "WARNING",
            step="finalization",
            message=f"Processing completed - Status: {dataset.status}",
            details={
                "final_quality_score": profile_result['quality_score'],
                "is_duplicate": is_duplicate,
                "duplicate_reason": duplicate_reason,
                "final_shape": cleaned_df.shape
            }
        )
        db.add(processing_log)
        
        # Commit des changements
        db.commit()
        
        # Mettre à jour la progression finale
        result = {
            "progress": 100,
            "status": "completed",
            "dataset_id": dataset_id,
            "final_status": dataset.status,
            "quality_score": profile_result['quality_score'],
            "is_duplicate": is_duplicate,
            "duplicate_reason": duplicate_reason,
            "final_shape": cleaned_df.shape,
            "recommendations": profiler.get_quality_recommendations(profile_result)
        }
        
        self.update_state(state="SUCCESS", meta=result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing dataset {dataset_id}: {str(e)}")
        
        # Mettre à jour le statut en erreur
        try:
            db = next(get_db())
            dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
            if dataset:
                dataset.status = "rejected"
                dataset.kaggle_info = {
                    "rejected": True,
                    "reason": f"Processing error: {str(e)}",
                    "rejected_at": datetime.utcnow().isoformat()
                }
                
                # Logger l'erreur
                processing_log = ProcessingLogModel(
                    dataset_id=dataset_id,
                    log_level="ERROR",
                    step="processing",
                    message=f"Processing failed: {str(e)}",
                    details={"error_type": type(e).__name__}
                )
                db.add(processing_log)
                db.commit()
        except Exception as db_error:
            logger.error(f"Failed to update database with error status: {str(db_error)}")
        
        # Mettre à jour l'état de la tâche
        self.update_state(
            state="FAILURE", 
            meta={
                "error": str(e),
                "dataset_id": dataset_id,
                "status": "failed"
            }
        )
        
        raise


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
    basée sur le feedback des publications Kaggle
    """
    try:
        db = next(get_db())
        
        # Logique pour mettre à jour les scores basée sur les métriques Kaggle
        # Ceci est un placeholder - à implémenter selon les besoins
        
        logger.info("Quality scores update task completed")
        
        return {"status": "completed", "updated_count": 0}
        
    except Exception as e:
        logger.error(f"Error in quality scores update task: {str(e)}")
        raise
