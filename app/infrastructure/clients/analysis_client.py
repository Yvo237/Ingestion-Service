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

    def sync_call_analysis(self, endpoint: str, csv_data: bytes, params: dict):
        with httpx.Client(timeout=60.0) as client:
            files = {'file': ('data.csv', csv_data, 'text/csv')}
            data = {'params': json.dumps(params)}
            
            # Ajuster l'URL si nécessaire (docker internal name)
            url = self.base_url.replace("localhost", "analytics_service")
            response = client.post(f"{url}/{endpoint}", files=files, data=data)
            
            if response.status_code != 200:
                raise Exception(f"Erreur Service Analyse: {response.text}")
                
            return response.json()