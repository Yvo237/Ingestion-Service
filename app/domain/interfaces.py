from abc import ABC, abstractmethod
from typing import List, Optional
from .entities import AnalysisRecord

class IAnalysisRepository(ABC):
    @abstractmethod
    def save(self, record: AnalysisRecord) -> AnalysisRecord:
        pass

    @abstractmethod
    def get_by_user(self, user_id: str) -> List[AnalysisRecord]:
        pass

class IAnalysisClient(ABC):
    @abstractmethod
    async def call_analysis(self, endpoint: str, csv_data: bytes, params: dict) -> dict:
        pass