import httpx
from typing import Dict, Any
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class NotificationClient:
    """Client pour communiquer avec le microservice de notification"""
    
    def __init__(self):
        self.notification_service_url = os.getenv("NOTIFICATION_SERVICE_URL", "http://notification_service:8002")
        self.timeout = 30.0
    
    async def send_kaggle_notification(self, dataset_data: Dict, publication_result: Dict) -> bool:
        """Appelle le service de notification pour une publication Kaggle"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.notification_service_url}/send-kaggle-notification",
                    json={
                        "dataset_data": dataset_data,
                        "publication_result": publication_result
                    }
                )
                
                if response.status_code == 200:
                    logger.info(f"Kaggle notification sent successfully for dataset {dataset_data.get('name')}")
                    return True
                else:
                    logger.error(f"Failed to send notification: {response.status_code} - {response.text}")
                    return False
                    
        except httpx.TimeoutException:
            logger.error("Timeout while calling notification service")
            return False
        except httpx.ConnectError:
            logger.error("Could not connect to notification service")
            return False
        except Exception as e:
            logger.error(f"Error calling notification service: {str(e)}")
            return False
    
    async def send_digest(self, period: str, summary_data: Dict) -> bool:
        """Appelle le service de notification pour un rapport digest"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.notification_service_url}/send-digest",
                    json={
                        "period": period,
                        "summary_data": summary_data
                    }
                )
                
                if response.status_code == 200:
                    logger.info(f"Digest notification sent successfully for period: {period}")
                    return True
                else:
                    logger.error(f"Failed to send digest: {response.status_code} - {response.text}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error calling notification service for digest: {str(e)}")
            return False
    
    async def health_check(self) -> bool:
        """Vérifie si le service de notification est en ligne"""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(f"{self.notification_service_url}/health")
                
                if response.status_code == 200:
                    health_data = response.json()
                    return health_data.get("status") == "healthy"
                else:
                    return False
                    
        except Exception as e:
            logger.error(f"Health check failed for notification service: {str(e)}")
            return False
