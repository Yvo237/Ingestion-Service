from celery import current_task
from app.celery_app import celery_app
from app.services.kaggle_publisher import KagglePublisher
from app.services.notification_client import NotificationClient
from app.infrastructure.database.config import get_db
from app.infrastructure.database.models import DatasetModel, ProcessingLogModel
from sqlalchemy.orm import Session
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@celery_app.task(bind=True)
def publish_to_kaggle_task(self, dataset_id: int):
    """
    Tâche Celery pour publier un dataset sur Kaggle
    """
    try:
        # Récupérer le dataset
        db = next(get_db())
        dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
        
        if not dataset:
            raise ValueError(f"Dataset {dataset_id} not found")
        
        # Vérifier que le dataset est dans le bon statut
        if dataset.status != "premium":
            raise ValueError(f"Dataset {dataset_id} is not in premium status (current: {dataset.status})")
        
        # Mettre à jour le statut
        dataset.status = "processing"
        db.commit()
        
        # Mettre à jour la progression
        self.update_state(state="PROCESSING", meta={"progress": 10, "status": "Starting Kaggle publication"})
        
        # Préparer les données pour Kaggle
        dataset_data = {
            'id': dataset.id,
            'name': dataset.name,
            'status': dataset.status,
            'quality_score': float(dataset.quality_score) if dataset.quality_score else 0,
            'row_count': dataset.row_count,
            'headers': dataset.headers,
            'file_hash': dataset.file_hash,
            'processing_log': dataset.processing_log,
            'metadata': dataset.metadata,
            'cleaned_data': dataset.cleaned_data
        }
        
        # Initialiser le publisher
        publisher = KagglePublisher()
        
        # Mettre à jour la progression
        self.update_state(state="PROCESSING", meta={"progress": 30, "status": "Kaggle API initialized"})
        
        # Vérifier les conditions de publication
        can_publish, conditions = publisher.can_publish_to_kaggle(dataset_data)
        
        if not can_publish:
            # Marquer comme rejected si les conditions ne sont pas remplies
            dataset.status = "rejected"
            dataset.kaggle_info = {
                "rejected": True,
                "reason": "Does not meet publication requirements",
                "conditions": conditions,
                "rejected_at": datetime.utcnow().isoformat()
            }
            db.commit()
            
            # Logger
            processing_log = ProcessingLogModel(
                dataset_id=dataset_id,
                log_level="WARNING",
                step="kaggle_publication",
                message="Publication rejected - conditions not met",
                details={"conditions": conditions}
            )
            db.add(processing_log)
            db.commit()
            
            # Préparer le résultat pour la notification
            rejection_result = {
                "success": False,
                "error": "Dataset does not meet publication requirements",
                "conditions": conditions,
                "dataset_id": dataset_id
            }
            
            # Envoyer la notification email même en cas d'échec
            try:
                import asyncio
                notification_client = NotificationClient()
                notification_sent = asyncio.run(notification_client.send_kaggle_notification(dataset_data, rejection_result))
                
                if notification_sent:
                    logger.info(f"Email notification sent for rejected dataset {dataset_id}")
                else:
                    logger.warning(f"Failed to send email notification for rejected dataset {dataset_id}")
                    
            except Exception as e:
                logger.error(f"Error sending notification for rejected dataset {dataset_id}: {str(e)}")
                # Ne pas échouer la tâche si l'email échoue
            
            return rejection_result
        
        # Mettre à jour la progression
        self.update_state(state="PROCESSING", meta={"progress": 50, "status": "Conditions verified"})
        
        # Vérifier les doublons Kaggle
        is_duplicate, duplicate_reason = publisher.check_kaggle_duplicates(dataset_data)
        
        if is_duplicate:
            # Marquer comme rejected si doublon trouvé
            dataset.status = "rejected"
            dataset.kaggle_info = {
                "rejected": True,
                "reason": "Duplicate found on Kaggle",
                "duplicate_reason": duplicate_reason,
                "rejected_at": datetime.utcnow().isoformat()
            }
            db.commit()
            
            # Logger
            processing_log = ProcessingLogModel(
                dataset_id=dataset_id,
                log_level="WARNING",
                step="kaggle_publication",
                message="Publication rejected - duplicate found",
                details={"duplicate_reason": duplicate_reason}
            )
            db.add(processing_log)
            db.commit()
            
            # Préparer le résultat pour la notification
            duplicate_result = {
                "success": False,
                "error": "Duplicate dataset found on Kaggle",
                "duplicate_reason": duplicate_reason,
                "dataset_id": dataset_id
            }
            
            # Envoyer la notification email même en cas de doublon
            try:
                import asyncio
                notification_client = NotificationClient()
                notification_sent = asyncio.run(notification_client.send_kaggle_notification(dataset_data, duplicate_result))
                
                if notification_sent:
                    logger.info(f"Email notification sent for duplicate dataset {dataset_id}")
                else:
                    logger.warning(f"Failed to send email notification for duplicate dataset {dataset_id}")
                    
            except Exception as e:
                logger.error(f"Error sending notification for duplicate dataset {dataset_id}: {str(e)}")
                # Ne pas échouer la tâche si l'email échoue
            
            return duplicate_result
        
        # Mettre à jour la progression
        self.update_state(state="PROCESSING", meta={"progress": 70, "status": "Duplicate check completed"})
        
        # Publier sur Kaggle
        result = publisher.publish_to_kaggle(dataset_data)
        
        # Mettre à jour la progression
        self.update_state(state="PROCESSING", meta={"progress": 90, "status": "Kaggle publication completed"})
        
        if result['success']:
            # Mettre à jour le statut du dataset
            dataset.status = "published"
            dataset.kaggle_info = {
                "published": True,
                "action": result.get('action', 'created'),
                "visibility": result.get('visibility', 'private'),
                "dataset_url": result.get('dataset_url'),
                "published_at": result.get('published_at'),
                "quality_score_at_publication": dataset_data['quality_score']
            }
            
            # Envoyer la notification email via le microservice
            try:
                import asyncio
                notification_client = NotificationClient()
                notification_sent = asyncio.run(notification_client.send_kaggle_notification(dataset_data, result))
                
                if notification_sent:
                    logger.info(f"Email notification sent for dataset {dataset_id}")
                else:
                    logger.warning(f"Failed to send email notification for dataset {dataset_id}")
                    
            except Exception as e:
                logger.error(f"Error sending notification for dataset {dataset_id}: {str(e)}")
                # Ne pas échouer la tâche si l'email échoue
            
            # Logger le succès
            processing_log = ProcessingLogModel(
                dataset_id=dataset_id,
                log_level="INFO",
                step="kaggle_publication",
                message="Successfully published to Kaggle",
                details={
                    "action": result.get('action'),
                    "visibility": result.get('visibility'),
                    "dataset_url": result.get('dataset_url'),
                    "quality_score": dataset_data['quality_score']
                }
            )
            db.add(processing_log)
            
            logger.info(f"Dataset {dataset_id} successfully published to Kaggle")
            
        else:
            # Marquer comme rejected en cas d'échec
            dataset.status = "rejected"
            dataset.kaggle_info = {
                "rejected": True,
                "reason": result.get('error', 'Unknown error'),
                "error_type": result.get('error_type'),
                "rejected_at": datetime.utcnow().isoformat()
            }
            
            # Logger l'échec
            processing_log = ProcessingLogModel(
                dataset_id=dataset_id,
                log_level="ERROR",
                step="kaggle_publication",
                message="Failed to publish to Kaggle",
                details={
                    "error": result.get('error'),
                    "error_type": result.get('error_type')
                }
            )
            db.add(processing_log)
            
            logger.error(f"Failed to publish dataset {dataset_id} to Kaggle: {result.get('error')}")
        
        db.commit()
        
        # Mettre à jour la progression finale
        final_result = {
            "progress": 100,
            "status": "completed",
            "dataset_id": dataset_id,
            "success": result['success'],
            "final_status": dataset.status,
            "kaggle_info": dataset.kaggle_info
        }
        
        self.update_state(state="SUCCESS", meta=final_result)
        
        return final_result
        
    except Exception as e:
        logger.error(f"Error in Kaggle publication task for dataset {dataset_id}: {str(e)}")
        
        # Mettre à jour le statut en erreur
        try:
            db = next(get_db())
            dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
            if dataset:
                dataset.status = "rejected"
                dataset.kaggle_info = {
                    "rejected": True,
                    "reason": f"Kaggle publication error: {str(e)}",
                    "error_type": type(e).__name__,
                    "rejected_at": datetime.utcnow().isoformat()
                }
                
                # Logger l'erreur
                processing_log = ProcessingLogModel(
                    dataset_id=dataset_id,
                    log_level="ERROR",
                    step="kaggle_publication",
                    message=f"Kaggle publication failed: {str(e)}",
                    details={"error_type": type(e).__name__}
                )
                db.add(processing_log)
                db.commit()
        except Exception as db_error:
            logger.error(f"Failed to update database with Kaggle publication error: {str(db_error)}")
        
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
def monitor_kaggle_datasets():
    """
    Tâche périodique pour monitorer les datasets publiés sur Kaggle
    """
    try:
        db = next(get_db())
        
        # Récupérer tous les datasets publiés
        published_datasets = db.query(DatasetModel).filter(
            DatasetModel.status == "published"
        ).all()
        
        publisher = KagglePublisher()
        updated_count = 0
        
        for dataset in published_datasets:
            try:
                # Extraire le nom du dataset Kaggle
                kaggle_info = dataset.kaggle_info or {}
                dataset_url = kaggle_info.get('dataset_url')
                
                if dataset_url:
                    # Extraire le réf du dataset depuis l'URL
                    dataset_ref = dataset_url.split('kaggle.com/datasets/')[-1]
                    
                    # Récupérer les infos actuelles
                    kaggle_data = publisher.get_kaggle_dataset_info(dataset_ref)
                    
                    if 'error' not in kaggle_data:
                        # Mettre à jour les métriques
                        updated_kaggle_info = {
                            **kaggle_info,
                            "view_count": kaggle_data.get('view_count'),
                            "download_count": kaggle_data.get('download_count'),
                            "vote_count": kaggle_data.get('vote_count'),
                            "usability_rating": kaggle_data.get('usability_rating'),
                            "last_monitored": datetime.utcnow().isoformat()
                        }
                        
                        dataset.kaggle_info = updated_kaggle_info
                        updated_count += 1
                        
                        # Logger le monitoring
                        processing_log = ProcessingLogModel(
                            dataset_id=dataset.id,
                            log_level="INFO",
                            step="kaggle_monitoring",
                            message="Kaggle metrics updated",
                            details={
                                "view_count": kaggle_data.get('view_count'),
                                "download_count": kaggle_data.get('download_count'),
                                "vote_count": kaggle_data.get('vote_count')
                            }
                        )
                        db.add(processing_log)
                        
            except Exception as e:
                logger.error(f"Error monitoring dataset {dataset.id} on Kaggle: {str(e)}")
                continue
        
        db.commit()
        
        logger.info(f"Kaggle monitoring completed - updated {updated_count} datasets")
        
        return {
            "status": "completed",
            "monitored_count": len(published_datasets),
            "updated_count": updated_count
        }
        
    except Exception as e:
        logger.error(f"Error in Kaggle monitoring task: {str(e)}")
        raise
