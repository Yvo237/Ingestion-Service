import httpx
import json
import os

class AnalysisClient:
    def __init__(self):
        # URL du premier microservice
        self.base_url = os.getenv("ANALYSIS_SERVICE_URL", "http://localhost:8000/v1/analysis")

    async def call_analysis(self, endpoint: str, csv_data: bytes, params: dict):
        async with httpx.AsyncClient(timeout=60.0) as client:
            files = {'file': ('data.csv', csv_data, 'text/csv')}
            data = {'params': json.dumps(params)}
            
            response = await client.post(f"{self.base_url}/{endpoint}", files=files, data=data)
            
            if response.status_code != 200:
                raise Exception(f"Erreur Service Analyse: {response.text}")
                
            return response.json()