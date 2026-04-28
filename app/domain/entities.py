from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any

@dataclass
class AnalysisRecord:
    id: Optional[int]
    user_id: str
    dataset_name: str
    analysis_type: str  # ex: "regression", "pca"
    parameters: dict    # Les colonnes choisies
    results: dict       # Le JSON renvoyé par le service d'analyse
    created_at: datetime = datetime.now()