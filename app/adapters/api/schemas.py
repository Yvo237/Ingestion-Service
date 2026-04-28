from pydantic import BaseModel
from typing import Dict, Any, Optional

class CollectionResponse(BaseModel):
    message: str
    record_id: int
    analysis_type: str
    results: Dict[str, Any]

class HistoryRequest(BaseModel):
    user_id: str

class ExternalApiRequest(BaseModel):
    api_key: str
    external_url: str
    target_analysis: str # ex: "regression/linear"