from sqlalchemy.orm import Session
from app.infrastructure.database.models import DatasetModel
from app.infrastructure.clients.analysis_client import AnalysisClient
import pandas as pd
import hashlib
import io

class ProcessAnalysisUseCase:
    def __init__(self, db: Session):
        self.db = db
        self.client = AnalysisClient()

    async def execute(self, user_id: str, dataset_name: str, analysis_type: str, 
                      csv_data: bytes, params: dict, endpoint: str):
        
        # 1. Valider que le fichier n'est pas vide
        if not csv_data or len(csv_data) == 0:
            raise ValueError("Le fichier CSV est vide")
        
        # 2. Parser le CSV pour extraire les métadonnées
        try:
            df = pd.read_csv(io.BytesIO(csv_data))
            if df.empty:
                raise ValueError("Le fichier CSV ne contient aucune donnée")
            headers = df.columns.tolist()
            row_count = len(df)
            file_hash = hashlib.sha256(csv_data).hexdigest()
        except pd.errors.EmptyDataError:
            raise ValueError("Le fichier CSV est vide ou invalide")
        
        # 2. Appeler le service d'analyse
        analysis_results = await self.client.call_analysis(endpoint, csv_data, params)
        
        # 3. Vérifier si le fichier existe déjà
        existing_record = self.db.query(DatasetModel).filter(
            DatasetModel.file_hash == file_hash,
            DatasetModel.user_id == user_id
        ).first()
        
        if existing_record:
            # Mettre à jour l'enregistrement existant avec les nouvelles analyses
            existing_record.analysis_type = analysis_type
            existing_record.analysis_parameters = params
            existing_record.analysis_results = analysis_results
            existing_record.updated_at = pd.Timestamp.now()
            self.db.commit()
            self.db.refresh(existing_record)
            return existing_record
        
        # 4. Créer un nouvel enregistrement
        new_record = DatasetModel(
            user_id=user_id,
            name=dataset_name,
            analysis_type=analysis_type,
            analysis_parameters=params,
            analysis_results=analysis_results,
            headers=headers,
            row_count=row_count,
            file_hash=file_hash,
            file_size=len(csv_data)
        )
        
        # 5. Sauvegarder
        self.db.add(new_record)
        self.db.commit()
        self.db.refresh(new_record)
        
        return new_record