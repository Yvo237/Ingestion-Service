from fastapi import FastAPI, Depends, UploadFile, File, Form, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import os
import uuid
import shutil
import redis
import asyncio
from pathlib import Path
from sqlalchemy.orm import Session
import json
from datetime import datetime
from app.infrastructure.database.config import get_db, engine
from app.infrastructure.database.models import Base, DatasetModel
from app.tasks.data_tasks import process_and_analyze_task
from app.adapters.api.endpoints import router

# Créer les tables au démarrage
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Data Collection & Storage Service")

# Ajouter le middleware CORS pour autoriser les requêtes du frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080", "http://127.0.0.1:8080", "https://reporting-service-chi.vercel.app"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Include les API router
app.include_router(router)

@app.post("/upload")
async def upload_dataset(
    user_id: str = Form(...),
    dataset_name: str = Form(...),
    file: UploadFile = File(..., size=500 * 1024 * 1024),  # 500MB limit
    db: Session = Depends(get_db)
):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Seuls les fichiers CSV sont acceptés")
    
    # Sauvegarder le fichier temporairement
    storage_path = Path("storage")
    storage_path.mkdir(exist_ok=True)
    
    file_id = str(uuid.uuid4())
    file_extension = Path(file.filename).suffix
    local_filename = f"{file_id}{file_extension}"
    local_path = storage_path / local_filename
    
    try:
        with local_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la sauvegarde du fichier: {e}")
    
    # Extraire les headers et le nombre de lignes
    import pandas as pd
    try:
        # Lire le fichier complet pour compter les lignes et extraire les headers
        df_full = pd.read_csv(local_path)
        headers = df_full.columns.tolist()
        row_count = len(df_full)
    except Exception as e:
        # En cas d'erreur de parsing CSV, on essaie avec moins de lignes
        try:
            df = pd.read_csv(local_path, nrows=1000)
            headers = df.columns.tolist()
            # Compter les lignes restantes
            with open(local_path, 'r') as f:
                row_count = sum(1 for _ in f) - 1  # -1 pour l'en-tête
        except Exception as e2:
            headers = []
            row_count = 0

    # Créer l'entrée en base avec le statut 'uploaded'
    new_record = DatasetModel(
        user_id=user_id,
        name=dataset_name,
        status="uploaded",
        headers=headers,
        row_count=row_count,
        file_size=os.path.getsize(local_path),
        # On utilise file_hash pour stocker le nom du fichier local
        file_hash=local_filename # On utilise file_hash pour stocker le nom du fichier local
    )
    
    db.add(new_record)
    db.commit()
    db.refresh(new_record)
    
    return {
        "message": "Fichier uploadé avec succès",
        "dataset_id": new_record.id,
        "status": "uploaded",
        "headers": headers
    }

@app.post("/analyze/{dataset_id}")
async def analyze_dataset(
    dataset_id: int,
    analysis_endpoint: str = Form(...), # ex: "regression/linear"
    params: str = Form(...),            # JSON string des colonnes
    db: Session = Depends(get_db)
):
    dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset non trouvé")
        
    if not dataset.file_hash:
        raise HTTPException(status_code=400, detail="Fichier local introuvable pour ce dataset")
        
    try:
        params_dict = json.loads(params)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Format JSON invalide pour params: {e}")
        
    # Mettre à jour l'enregistrement
    dataset.status = "pending"
    dataset.analysis_type = analysis_endpoint
    dataset.analysis_parameters = params_dict
    db.commit()
    
    # Reconstruire le chemin du fichier
    storage_path = Path("storage")
    filename = dataset.file_hash
    if not filename.endswith('.csv'):
        filename = f"{filename}.csv"
    local_path = storage_path / filename
    
    # Lancer la tâche asynchrone
    process_and_analyze_task.delay(
        record_id=dataset.id,
        file_path=str(local_path),
        analysis_endpoint=analysis_endpoint,
        params=params_dict
    )
    
    return {
        "message": "Analyse lancée",
        "dataset_id": dataset.id,
        "status": "pending"
    }

@app.delete("/v1/data/analysis/{dataset_id}")
async def delete_analysis(dataset_id: int, db: Session = Depends(get_db)):
    """Supprime les données d'analyse d'un dataset sans supprimer le dataset lui-même"""
    try:
        # Récupérer l'enregistrement
        dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
        
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset non trouvé")
        
        # Nettoyer uniquement les données d'analyse, pas le fichier
        dataset.analysis_type = None
        dataset.analysis_parameters = None
        dataset.analysis_results = None
        dataset.status = "uploaded" # Remettre au statut d'origine
        
        db.commit()
        
        return {"message": "Analyse arrêtée/supprimée avec succès", "dataset_id": dataset_id}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la suppression de l'analyse: {str(e)}")

@app.delete("/v1/data/dataset/{dataset_id}")
async def delete_dataset(dataset_id: int, db: Session = Depends(get_db)):
    """Supprime complètement un dataset et son fichier associé"""
    try:
        dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset non trouvé")
            
        # Supprimer le fichier physique si présent
        if dataset.file_hash:
            storage_path = Path("storage")
            local_path = storage_path / dataset.file_hash
            if local_path.exists():
                local_path.unlink()
                
        # Supprimer de la base
        db.delete(dataset)
        db.commit()
        
        return {"message": "Dataset et fichier supprimés avec succès", "dataset_id": dataset_id}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la suppression du dataset: {str(e)}")

# Configuration Redis
redis_url = os.getenv("CELERY_BROKER_URL", "redis://redis_queue:6379/0")
# On utilise la version synchrone avec to_thread pour plus de compatibilité
redis_client = redis.from_url(redis_url, decode_responses=True)

@app.websocket("/ws/logs/{record_id}")
async def websocket_logs(websocket: WebSocket, record_id: int):
    """Endpoint WebSocket pour le streaming de logs en temps réel via Redis Pub/Sub"""
    await websocket.accept()
    
    # 1. Envoyer les logs historiques depuis la base de données
    db = next(get_db())
    try:
        dataset = db.query(DatasetModel).filter(DatasetModel.id == record_id).first()
        if dataset and dataset.processing_log:
            logs = dataset.processing_log
            if isinstance(logs, str):
                try: logs = json.loads(logs)
                except: logs = []
            
            if isinstance(logs, list):
                for entry in logs:
                    await websocket.send_json({
                        "timestamp": entry.get("timestamp", datetime.utcnow().isoformat()),
                        "level": "INFO",
                        "message": entry.get("message", ""),
                        "details": {"analysis_type": entry.get("step", "process"), "dataset_id": record_id}
                    })
    finally:
        db.close()

    # 2. S'abonner aux nouveaux logs via Redis Pub/Sub
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f"logs:{record_id}")
    
    async def redis_listener():
        try:
            while True:
                # On utilise to_thread pour l'appel bloquant get_message
                message = await asyncio.to_thread(pubsub.get_message, ignore_subscribe_messages=True, timeout=1.0)
                if message and message['data']:
                    await websocket.send_text(message['data'])
                await asyncio.sleep(0.1)
        except Exception as e:
            # Souvent causé par la fermeture du WebSocket ou l'annulation de la tâche
            pass
        finally:
            try:
                pubsub.unsubscribe(f"logs:{record_id}")
                pubsub.close()
            except: pass

    # Lancer le listener en arrière-plan
    listener_task = asyncio.create_task(redis_listener())
    
    try:
        # On attend la déconnexion du client
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error for record {record_id}: {e}")
    finally:
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass

@app.post("/v1/logs/{record_id}/stream")
async def stream_log(record_id: int, log_data: dict):
    """Endpoint pour envoyer un log en temps réel aux clients WebSocket"""
    await manager.send_log_update(record_id, {
        "timestamp": datetime.utcnow().isoformat(),
        "level": log_data.get("level", "INFO"),
        "message": log_data.get("message", ""),
        "details": log_data.get("details", {})
    })
    return {"status": "sent", "connections": len(manager.active_connections.get(record_id, []))}

@app.get("/v1/health")
async def health_check():
    """Vérifie la santé de tous les services"""
    import time
    import psutil
    from datetime import datetime, timedelta
    
    start_time = time.time()
    
    # Vérifier le service de collecte (lui-même)
    collection_healthy = True
    collection_latency = round((time.time() - start_time) * 1000, 2)
    
    # Vérifier le service d'analyse
    analysis_healthy = False
    analysis_latency = None
    try:
        import httpx
        analysis_start = time.time()
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get("http://analytics_service:8000/health")
            analysis_healthy = response.status_code == 200
            analysis_latency = round((time.time() - analysis_start) * 1000, 2)
    except Exception:
        analysis_healthy = False
    
    # Vérifier la base de données
    db_healthy = False
    try:
        db = next(get_db())
        db.execute("SELECT 1")
        db_healthy = True
    except Exception:
        db_healthy = False
    
    # Métriques système
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    
    # Calculer l'uptime (simplifié)
    uptime_seconds = time.time() - psutil.boot_time()
    uptime_days = int(uptime_seconds // 86400)
    uptime_hours = int((uptime_seconds % 86400) // 3600)
    
    services = []
    
    # Service de collecte
    services.append({
        "name": "Collection Service",
        "kind": "service",
        "status": "online" if collection_healthy else "offline",
        "uptime": f"{uptime_days}d {uptime_hours}h",
        "latencyMs": collection_latency,
        "cpu": cpu_percent,
        "memory": memory.percent,
        "description": "FastAPI data collection API",
        "details": {
            "database": "connected" if db_healthy else "disconnected",
            "version": "1.0.0"
        }
    })
    
    # Service d'analyse
    services.append({
        "name": "Analysis Service",
        "kind": "service",
        "status": "online" if analysis_healthy else "offline",
        "uptime": f"{uptime_days}d {uptime_hours}h",
        "latencyMs": analysis_latency or 999,
        "cpu": None,
        "memory": None,
        "description": "FastAPI + Scikit-learn workers (Celery)",
        "details": {
            "endpoint": "http://analytics_service:8000"
        }
    })
    
    # Les services restants sont sains
    overall_status = "online" if all(s["status"] == "online" for s in services) else "degraded"
    
    return {
        "status": overall_status,
        "timestamp": datetime.utcnow().isoformat(),
        "services": services,
        "system": {
            "cpu": cpu_percent,
            "memory": memory.percent,
            "disk": psutil.disk_usage('/').percent
        }
    }

@app.get("/v1/metrics/throughput")
async def get_throughput_metrics():
    """Récupère les métriques de throughput des dernières 24h"""
    db = next(get_db())
    
    # Récupérer les analyses des dernières 24h
    from datetime import datetime, timedelta
    start_time = datetime.utcnow() - timedelta(hours=24)
    
    analyses = db.query(DatasetModel).filter(
        DatasetModel.created_at >= start_time
    ).all()
    
    # Grouper par heure
    hourly_data = {}
    for analysis in analyses:
        hour = analysis.created_at.strftime("%H:00")
        if hour not in hourly_data:
            hourly_data[hour] = {"ingest": 0, "analyses": 0}
        hourly_data[hour]["ingest"] += 1
        hourly_data[hour]["analyses"] += 1
    
    # Compléter les heures manquantes
    throughput = []
    for i in range(24):
        hour = f"{str(i).zfill(2)}:00"
        data = hourly_data.get(hour, {"ingest": 0, "analyses": 0})
        throughput.append({
            "hour": hour,
            "ingest": data["ingest"],
            "analyses": data["analyses"]
        })
    
    return {"throughput": throughput}

@app.get("/v1/metrics/accuracy")
async def get_accuracy_metrics():
    """Récupère les métriques d'accuracy des 14 derniers jours"""
    db = next(get_db())
    
    from datetime import datetime, timedelta
    start_time = datetime.utcnow() - timedelta(days=14)
    
    # Récupérer les analyses avec résultats
    analyses = db.query(DatasetModel).filter(
        DatasetModel.created_at >= start_time
    ).all()
    
    # Calculer l'accuracy moyenne par jour
    daily_accuracy = {}
    for analysis in analyses:
        if analysis.analysis_results:
            day = analysis.created_at.strftime("%Y-%m-%d")
            
            # Extraire les métriques d'accuracy selon le type d'analyse
            accuracy = None
            results = analysis.analysis_results
            
            if analysis.analysis_type == "regression/linear":
                accuracy = results.get("r_squared", 0)
            elif analysis.analysis_type == "classification/supervised":
                accuracy = results.get("accuracy", 0) / 100  # Convertir en proportion
            elif analysis.analysis_type == "dimensionality/pca":
                accuracy = results.get("explained_variance_ratio", [0])[0] if results.get("explained_variance_ratio") else 0
            
            if accuracy is not None:
                if day not in daily_accuracy:
                    daily_accuracy[day] = []
                daily_accuracy[day].append(accuracy)
    
    # Calculer la moyenne par jour
    accuracy_series = []
    for i in range(14):
        day = (datetime.utcnow() - timedelta(days=13-i)).strftime("%Y-%m-%d")
        day_label = f"D-{14-i}"
        
        accuracies = daily_accuracy.get(day, [])
        avg_accuracy = sum(accuracies) / len(accuracies) if accuracies else 0.78  # Baseline si pas de données
        
        accuracy_series.append({
            "day": day_label,
            "accuracy": avg_accuracy,
            "baseline": 0.78
        })
    
    return {"accuracy": accuracy_series}

@app.get("/v1/data/logs/{record_id}")
async def get_processing_logs(record_id: int):
    """Récupère les logs de traitement d'un enregistrement"""
    db = next(get_db())
    
    # Récupérer le dataset
    dataset = db.query(DatasetModel).filter(DatasetModel.id == record_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Simuler des logs basés sur le statut
    logs = []
    
    if dataset.status == "processing":
        logs = [
            {"timestamp": datetime.utcnow().isoformat(), "level": "INFO", "message": "Starting data processing..."},
            {"timestamp": datetime.utcnow().isoformat(), "level": "INFO", "message": f"Loading dataset: {dataset.name}"},
            {"timestamp": datetime.utcnow().isoformat(), "level": "INFO", "message": "Preprocessing data..."},
            {"timestamp": datetime.utcnow().isoformat(), "level": "INFO", "message": "Running analysis..."},
        ]
    elif dataset.status == "completed":
        logs = [
            {"timestamp": datetime.utcnow().isoformat(), "level": "INFO", "message": "Starting data processing..."},
            {"timestamp": datetime.utcnow().isoformat(), "level": "INFO", "message": f"Loading dataset: {dataset.name}"},
            {"timestamp": datetime.utcnow().isoformat(), "level": "INFO", "message": "Preprocessing data..."},
            {"timestamp": datetime.utcnow().isoformat(), "level": "INFO", "message": "Running analysis..."},
            {"timestamp": datetime.utcnow().isoformat(), "level": "SUCCESS", "message": "Analysis completed successfully!"},
            {"timestamp": datetime.utcnow().isoformat(), "level": "INFO", "message": f"Results saved for dataset {dataset.name}"},
        ]
    elif dataset.status == "failed":
        logs = [
            {"timestamp": datetime.utcnow().isoformat(), "level": "INFO", "message": "Starting data processing..."},
            {"timestamp": datetime.utcnow().isoformat(), "level": "ERROR", "message": "Processing failed due to an error"},
            {"timestamp": datetime.utcnow().isoformat(), "level": "ERROR", "message": "Please check your data and try again"},
        ]
    else:
        logs = [
            {"timestamp": datetime.utcnow().isoformat(), "level": "INFO", "message": "Dataset is queued for processing"},
        ]
    
    return {"logs": logs}

@app.post("/test-notification/{dataset_id}")
async def test_notification(dataset_id: int):
    """Endpoint supprimé - notifications non supportées"""
    raise HTTPException(status_code=410, detail="Notifications feature has been removed")

@app.put("/v1/data/datasets/{dataset_id}")
async def update_dataset(dataset_id: int, dataset_data: dict):
    """Met à jour un dataset avec les données fournies"""
    try:
        db = next(get_db())
        dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
        
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Mettre à jour les champs fournis
        updateable_fields = [
            'quality_score', 'row_count', 'headers', 'status', 
            'metadata', 'processing_log', 'is_duplicate', 'cleaned_data'
        ]
        
        for field in updateable_fields:
            if field in dataset_data:
                setattr(dataset, field, dataset_data[field])
        
        # Mettre à jour le timestamp
        dataset.updated_at = datetime.utcnow()
        
        db.commit()
        db.refresh(dataset)
        
        return {
            "message": f"Dataset {dataset_id} updated successfully",
            "dataset_id": dataset_id,
            "updated_fields": [field for field in updateable_fields if field in dataset_data]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update dataset: {str(e)}")

@app.post("/v1/kaggle/publish/{dataset_id}")
async def trigger_kaggle_publication(dataset_id: int):
    """Endpoint supprimé - publication Kaggle non supportée"""
    raise HTTPException(status_code=410, detail="Kaggle publication feature has been removed")

@app.get("/v1/tasks/status/{task_id}")
async def get_task_status(task_id: str):
    """Récupère le statut d'une tâche Celery"""
    try:
        from app.celery_app import celery_app
        
        # Récupérer le résultat de la tâche
        result = celery_app.AsyncResult(task_id)
        
        response = {
            "task_id": task_id,
            "state": result.state,
            "meta": {}
        }
        
        if result.state == 'SUCCESS':
            response["meta"] = result.result
        elif result.state == 'FAILURE':
            response["meta"] = {"error": str(result.result)}
        elif result.state in ['PENDING', 'PROCESSING']:
            response["meta"] = result.info or {}
        
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get task status: {str(e)}")

@app.post("/set-published/{dataset_id}")
async def set_published(dataset_id: int):
    """Met un dataset en statut 'published' pour les tests"""
    try:
        db = next(get_db())
        dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
        
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Mettre à jour le statut
        dataset.status = "published"
        
        db.commit()
        
        return {
            "message": f"Dataset {dataset_id} set to published",
            "dataset_name": dataset.name,
            "status": dataset.status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to set published: {str(e)}")

@app.get("/")
async def root_health():
    """Health check endpoint pour le service principal"""
    return {
        "status": "healthy",
        "service": "ingestion_service",
        "version": "1.0.0"
    }

@app.get("/notifications")
async def get_notifications(db: Session = Depends(get_db)):
    """Récupère les logs des emails envoyés depuis la base de données"""
    try:
        logs = db.query(EmailLogModel).order_by(EmailLogModel.sent_at.desc()).limit(50).all()
        return [
            {
                "id": log.id,
                "type": log.email_type,
                "recipient": log.recipient_email,
                "subject": log.subject,
                "status": log.status,
                "error": log.error_message,
                "timestamp": log.sent_at.isoformat() if log.sent_at else None
            }
            for log in logs
        ]
    except Exception as e:
        print(f"Error fetching notifications: {str(e)}")
        return []

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)