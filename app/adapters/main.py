from fastapi import FastAPI, Depends, UploadFile, File, Form, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import json
from datetime import datetime
from app.infrastructure.database.config import get_db, engine
from app.infrastructure.database.models import Base, DatasetModel
from app.use_cases.process_analysis import ProcessAnalysisUseCase
from app.adapters.api.endpoints import router

# Créer les tables au démarrage
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Data Collection & Storage Service")

# Ajouter le middleware CORS pour autoriser les requêtes du frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080", "http://127.0.0.1:8080"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Include les API router
app.include_router(router)

@app.post("/collect-and-analyze")
async def collect_and_analyze(
    user_id: str = Form(...),
    dataset_name: str = Form(...),
    analysis_endpoint: str = Form(...), # ex: "regression/linear"
    params: str = Form(...),            # JSON string des colonnes
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    # DEBUG: Log des données reçues
    print(f"=== DEBUG REQUÊTE ===")
    print(f"user_id: {user_id}")
    print(f"dataset_name: {dataset_name}")
    print(f"analysis_endpoint: {analysis_endpoint}")
    print(f"params: {params}")
    
    try:
        params_dict = json.loads(params)
        print(f"params_dict: {params_dict}")
    except Exception as e:
        print(f"ERREUR params JSON: {e}")
        raise HTTPException(status_code=400, detail=f"Format JSON invalide: {e}")
    
    # 1. Lire le fichier
    csv_bytes = await file.read()
    print(f"Taille CSV: {len(csv_bytes)} bytes")
    if len(csv_bytes) < 500:
        print(f"Contenu CSV: {csv_bytes}")
    
    try:
        params_dict = json.loads(params)
    except Exception as e:
        print(f"ERREUR params JSON: {e}")
        raise HTTPException(status_code=400, detail=f"Format JSON invalide: {e}")
    
    # 2. Lancer le use case
    try:
        use_case = ProcessAnalysisUseCase(db)
        record = await use_case.execute(
            user_id=user_id,
            dataset_name=dataset_name,
            analysis_type=analysis_endpoint,
            csv_data=csv_bytes,
            params=params_dict,
            endpoint=analysis_endpoint
        )
        
        return {"message": "Analyse réussie et stockée", "record_id": record.id, "results": record.analysis_results}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de l'analyse: {str(e)}")

@app.delete("/v1/data/analysis/{analysis_id}")
async def delete_analysis(analysis_id: int, db: Session = Depends(get_db)):
    """Supprime une analyse spécifique"""
    try:
        # Récupérer l'analyse
        analysis = db.query(DatasetModel).filter(DatasetModel.id == analysis_id).first()
        
        if not analysis:
            raise HTTPException(status_code=404, detail="Analyse non trouvée")
        
        # Supprimer l'analyse
        db.delete(analysis)
        db.commit()
        
        return {"message": "Analyse supprimée avec succès", "analysis_id": analysis_id}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la suppression: {str(e)}")

# Gestionnaire de connexions WebSocket pour le streaming de logs
class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[int, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, record_id: int):
        await websocket.accept()
        if record_id not in self.active_connections:
            self.active_connections[record_id] = []
        self.active_connections[record_id].append(websocket)

    def disconnect(self, websocket: WebSocket, record_id: int):
        if record_id in self.active_connections:
            self.active_connections[record_id].remove(websocket)
            if not self.active_connections[record_id]:
                del self.active_connections[record_id]

    async def send_log_update(self, record_id: int, log_data: dict):
        if record_id in self.active_connections:
            disconnected = []
            for connection in self.active_connections[record_id]:
                try:
                    await connection.send_json(log_data)
                except:
                    disconnected.append(connection)
            # Nettoyer les connexions déconnectées
            for conn in disconnected:
                self.disconnect(conn, record_id)

manager = ConnectionManager()

@app.websocket("/ws/logs/{record_id}")
async def websocket_logs(websocket: WebSocket, record_id: int):
    """Endpoint WebSocket pour le streaming de logs en temps réel"""
    await manager.connect(websocket, record_id)
    try:
        # Envoyer les logs existants
        db = next(get_db())
        dataset = db.query(DatasetModel).filter(DatasetModel.id == record_id).first()
        
        if dataset:
            # Envoyer les logs historiques
            historical_logs = [
                {
                    "timestamp": dataset.created_at.isoformat() if dataset.created_at else datetime.utcnow().isoformat(),
                    "level": "INFO",
                    "message": f"Analyse '{dataset.name}' démarrée",
                    "details": {
                        "analysis_type": dataset.analysis_type,
                        "dataset_id": record_id
                    }
                }
            ]
            
            for log in historical_logs:
                await websocket.send_json(log)
        
        # Maintenir la connexion ouverte et écouter les nouveaux logs
        while True:
            try:
                # Attendre des messages ping/pong pour maintenir la connexion
                await websocket.receive_text()
            except WebSocketDisconnect:
                break
                
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket, record_id)

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
            response = await client.get("http://analysis_service:8000/health")
            analysis_healthy = response.status_code == 200
            analysis_latency = round((time.time() - analysis_start) * 1000, 2)
    except Exception:
        analysis_healthy = False
    
    # Vérifier le service de notification
    notification_healthy = False
    notification_latency = None
    try:
        notification_start = time.time()
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get("http://notification_service:8002/health")
            notification_healthy = response.status_code == 200
            notification_latency = round((time.time() - notification_start) * 1000, 2)
    except Exception:
        notification_healthy = False
    
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
            "endpoint": "http://analysis_service:8000"
        }
    })
    
    # Service de notification
    services.append({
        "name": "Notification Service",
        "kind": "service",
        "status": "online" if notification_healthy else "offline",
        "uptime": f"{uptime_days}d {uptime_hours}h",
        "latencyMs": notification_latency or 999,
        "cpu": None,
        "memory": None,
        "description": "FastAPI notification service",
        "details": {
            "endpoint": "http://notification_service:8002"
        }
    })
    
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
    """Teste l'envoi d'une notification Kaggle"""
    try:
        from app.services.notification_client import NotificationClient
        import asyncio
        
        # Récupérer le dataset
        db = next(get_db())
        dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
        
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Préparer les données de test
        dataset_data = {
            'id': dataset.id,
            'name': dataset.name,
            'status': dataset.status,
            'quality_score': float(dataset.quality_score) if dataset.quality_score else 0,
        }
        
        result = {
            'success': True,
            'action': 'created',
            'visibility': 'public',
            'dataset_url': f'https://kaggle.com/datasets/test/{dataset.name}',
            'published_at': datetime.utcnow().isoformat()
        }
        
        # Envoyer la notification
        notification_client = NotificationClient()
        notification_sent = await notification_client.send_kaggle_notification(dataset_data, result)
        
        return {
            "message": "Test notification sent",
            "dataset_id": dataset_id,
            "notification_sent": notification_sent
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send test notification: {str(e)}")

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
    """Déclenche la publication automatique Kaggle avec vérification des conditions"""
    try:
        from app.tasks.kaggle_tasks import publish_to_kaggle_task
        
        # Lancer la tâche Celery
        task = publish_to_kaggle_task.delay(dataset_id)
        
        return {
            "message": f"Kaggle publication task started for dataset {dataset_id}",
            "task_id": task.id,
            "dataset_id": dataset_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start Kaggle publication: {str(e)}")

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
        
        # Mettre à jour le statut et les infos Kaggle
        dataset.status = "published"
        dataset.kaggle_info = {
            "published": True,
            "action": "created",
            "visibility": "public",
            "dataset_url": f"https://kaggle.com/datasets/test/{dataset.name}",
            "published_at": datetime.utcnow().isoformat(),
            "download_count": 150,
            "vote_count": 25
        }
        
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
        "service": "data_collection_service",
        "version": "1.0.0"
    }

@app.get("/notifications")
async def get_notifications():
    """Proxy pour récupérer les notifications depuis le service de notification"""
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get("http://notification_service:8002/logs")
            if response.status_code == 200:
                return response.json()
            else:
                return []
    except Exception as e:
        print(f"Error fetching notifications: {str(e)}")
        return []

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)