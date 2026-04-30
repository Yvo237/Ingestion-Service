from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
import json
import os
from app.infrastructure.database.config import get_db
from app.infrastructure.database.models import DatasetModel
from .schemas import CollectionResponse

router = APIRouter(prefix="/v1/data")

@router.get("/history/{user_id}")
async def get_user_history(user_id: str, db: Session = Depends(get_db)):
    analyses = db.query(DatasetModel).filter(DatasetModel.user_id == user_id).all()
    return analyses if analyses else []

@router.get("/result/{record_id}")
async def get_specific_result(record_id: int, db: Session = Depends(get_db)):
    record = db.query(DatasetModel).filter(DatasetModel.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Résultat introuvable")
    return record

@router.get("/result/{record_id}/full")
async def get_full_analysis_result(record_id: int, db: Session = Depends(get_db)):
    record = db.query(DatasetModel).filter(DatasetModel.id == record_id).first()
    if not record or not record.results_path or not os.path.exists(record.results_path):
        raise HTTPException(status_code=404, detail="Résultats complets introuvables sur le disque")
    
    with open(record.results_path, 'r') as f:
        return json.load(f)

@router.get("/result/{record_id}/csv")
async def get_cleaned_csv(record_id: int, db: Session = Depends(get_db)):
    record = db.query(DatasetModel).filter(DatasetModel.id == record_id).first()
    if not record or not record.storage_path or not os.path.exists(record.storage_path):
        raise HTTPException(status_code=404, detail="Fichier CSV introuvable sur le disque")
    
    return FileResponse(
        path=record.storage_path, 
        filename=f"cleaned_{record.name}.csv",
        media_type="text/csv"
    )