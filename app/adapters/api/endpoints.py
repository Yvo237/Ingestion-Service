from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.infrastructure.database.config import get_db
from app.infrastructure.database.models import DatasetModel
from .schemas import CollectionResponse

router = APIRouter(prefix="/v1/data")

@router.get("/history/{user_id}")
async def get_user_history(user_id: str, db: Session = Depends(get_db)):
    analyses = db.query(DatasetModel).filter(DatasetModel.user_id == user_id).all()
    if not analyses:
        raise HTTPException(status_code=404, detail="Aucun historique trouvé pour cet utilisateur")
    return analyses

@router.get("/result/{record_id}")
async def get_specific_result(record_id: int, db: Session = Depends(get_db)):
    record = db.query(DatasetModel).filter(DatasetModel.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Résultat introuvable")
    return record