import os
import json
import tempfile
import zipfile
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
import logging
import requests
from pathlib import Path

logger = logging.getLogger(__name__)


class KagglePublisher:
    """Service sécurisé pour publier des datasets sur Kaggle"""
    
    def __init__(self):
        self.api = None
        self._initialized = False
    
    def _ensure_api_initialized(self):
        """Initialise l'API Kaggle avec les credentials si ce n'est pas déjà fait"""
        if not self._initialized:
            self._initialize_api()
            self._initialized = True
    
    def _initialize_api(self):
        """Initialise l'API Kaggle avec les credentials"""
        try:
            # Importer l'API Kaggle seulement maintenant
            from kaggle.api.kaggle_api_extended import KaggleApi
            
            # Vérifier les credentials
            kaggle_json_path = os.path.expanduser("~/.kaggle/kaggle.json")
            
            if not os.path.exists(kaggle_json_path):
                # Créer le fichier depuis les variables d'environnement
                kaggle_username = os.getenv("KAGGLE_USERNAME")
                kaggle_key = os.getenv("KAGGLE_KEY")
                
                if not kaggle_username or not kaggle_key:
                    raise ValueError("Kaggle credentials not found. Set KAGGLE_USERNAME and KAGGLE_KEY environment variables.")
                
                # Créer le répertoire .kaggle s'il n'existe pas
                os.makedirs(os.path.dirname(kaggle_json_path), exist_ok=True)
                
                # Écrire le fichier de credentials
                credentials = {
                    "username": kaggle_username,
                    "key": kaggle_key
                }
                
                with open(kaggle_json_path, 'w') as f:
                    json.dump(credentials, f)
                
                # Changer les permissions pour la sécurité
                os.chmod(kaggle_json_path, 0o600)
            
            # Initialiser l'API
            self.api = KaggleApi()
            self.api.authenticate()
            
            logger.info("Kaggle API initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kaggle API: {str(e)}")
            raise
    
    def can_publish_to_kaggle(self, dataset_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Vérifie si un dataset peut être publié sur Kaggle"""
        self._ensure_api_initialized()
        
        conditions = {
            'quality_score': dataset_data.get('quality_score', 0) >= 8.0,
            'min_rows': dataset_data.get('row_count', 0) >= 100,
            'max_rows': dataset_data.get('row_count', 0) <= 10000,
            'min_cols': len(dataset_data.get('headers', [])) >= 5,
            'max_cols': len(dataset_data.get('headers', [])) <= 50,
            'completeness': self._get_completeness_ratio(dataset_data) >= 0.95,
            'duplicate_check': not dataset_data.get('is_duplicate', False),
            'lineage_complete': bool(dataset_data.get('processing_log')),
            'status_valid': dataset_data.get('status') == 'premium'
        }
        
        can_publish = all(conditions.values())
        
        return can_publish, conditions
    
    def _get_completeness_ratio(self, dataset_data: Dict[str, Any]) -> float:
        """Extrait le ratio de complétude des métadonnées"""
        metadata = dataset_data.get('metadata', {})
        return metadata.get('completeness_ratio', 1.0)
    
    def check_kaggle_duplicates(self, dataset_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Vérifie si le dataset existe déjà sur Kaggle"""
        try:
            self._ensure_api_initialized()
            # Extraire un hash ou identifier unique du contenu
            content_hash = dataset_data.get('file_hash', '')
            dataset_name = dataset_data.get('name', '')
            
            # Rechercher des datasets similaires sur Kaggle
            search_results = self.api.dataset_list(search=dataset_name, page_size=10)
            
            for dataset in search_results:
                # Vérifier la similarité du nom
                if self._calculate_name_similarity(dataset_name, dataset.ref) > 0.8:
                    return True, f"Similar dataset found on Kaggle: {dataset.ref}"
            
            return False, None
            
        except Exception as e:
            logger.error(f"Error checking Kaggle duplicates: {str(e)}")
            return False, None
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calcule la similarité entre deux noms (simple implementation)"""
        name1_lower = name1.lower().replace('_', ' ').replace('-', ' ')
        name2_lower = name2.lower().replace('_', ' ').replace('-', ' ')
        
        # Similarité basique par mots communs
        words1 = set(name1_lower.split())
        words2 = set(name2_lower.split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        return len(intersection) / len(union)
    
    def generate_dataset_card(self, dataset_data: Dict[str, Any]) -> Dict[str, str]:
        """Génère une dataset card pour Kaggle"""
        
        # Extraire les informations importantes
        quality_score = dataset_data.get('quality_score', 0)
        row_count = dataset_data.get('row_count', 0)
        col_count = len(dataset_data.get('headers', []))
        processing_log = dataset_data.get('processing_log', {})
        metadata = dataset_data.get('metadata', {})
        
        # Générer le titre
        title = dataset_data.get('name', 'Untitled Dataset')
        
        # Générer la description
        description = f"""# {title}

## Overview
This dataset was automatically processed and validated by our AI-powered data processing pipeline.

**Quality Score:** {quality_score}/10.0
**Rows:** {row_count:,}
**Columns:** {col_count}

## Data Processing
This dataset underwent the following processing steps:

"""
        
        # Ajouter les étapes de traitement
        transformations = processing_log.get('transformations', [])
        for transform in transformations:
            step = transform.get('step', 'Unknown')
            action = transform.get('action', 'Unknown')
            details = transform.get('details', {})
            
            description += f"- **{step}**: {action}\n"
            
            # Ajouter des détails importants
            if 'columns_dropped' in details:
                description += f"  - Dropped {len(details['columns_dropped'])} columns\n"
            if 'rows_dropped' in details:
                description += f"  - Removed {details['rows_dropped']} rows\n"
            if 'missing_count' in details:
                description += f"  - Handled {details['missing_count']} missing values\n"
        
        # Ajouter les métriques de qualité
        completeness_ratio = self._get_completeness_ratio(dataset_data)
        description += f"""

## Quality Metrics
- **Completeness**: {completeness_ratio:.1%}
- **Duplicate Rows**: {metadata.get('duplicate_rows', 'N/A')}
- **Memory Size**: {metadata.get('memory_size', 'N/A')}

## Data Types
"""
        
        # Ajouter les informations sur les types
        type_metrics = metadata.get('type_metrics', {})
        for data_type, info in type_metrics.items():
            description += f"- **{data_type}**: {info.get('count', 0)} columns\n"
        
        # Ajouter les recommandations
        description += """

## Usage
This dataset is suitable for:
- Machine learning projects
- Data analysis and visualization
- Statistical analysis

## License
This dataset is provided under the CC0 1.0 Universal (CC0 1.0) Public Domain Dedication license.

## Acknowledgments
This dataset was processed using advanced data cleaning and quality assessment techniques.
"""
        
        return {
            'title': title,
            'description': description,
            'subtitle': f"High-quality dataset (Score: {quality_score}/10)"
        }
    
    def create_dataset_package(self, dataset_data: Dict[str, Any]) -> str:
        """Crée un package zip pour Kaggle"""
        
        # Créer un répertoire temporaire
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Créer le fichier CSV
            cleaned_data = dataset_data.get('cleaned_data', [])
            headers = dataset_data.get('headers', [])
            
            csv_path = temp_path / f"{dataset_data.get('name', 'dataset')}.csv"
            
            if cleaned_data and headers:
                import pandas as pd
                df = pd.DataFrame(cleaned_data)
                df.to_csv(csv_path, index=False)
            
            # Créer le fichier dataset-metadata.json
            dataset_card = self.generate_dataset_card(dataset_data)
            
            metadata = {
                'title': dataset_card['title'],
                'subtitle': dataset_card['subtitle'],
                'description': dataset_card['description'],
                'id': f"{os.getenv('KAGGLE_USERNAME', 'user')}/{dataset_data.get('name', 'dataset').lower().replace(' ', '-')}",
                'licenses': [{'name': 'CC0-1.0'}]
            }
            
            metadata_path = temp_path / 'dataset-metadata.json'
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Créer le fichier README
            readme_path = temp_path / 'README.md'
            with open(readme_path, 'w') as f:
                f.write(dataset_card['description'])
            
            # Créer le zip
            zip_path = temp_path / f"{dataset_data.get('name', 'dataset')}.zip"
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                if csv_path.exists():
                    zipf.write(csv_path, csv_path.name)
                zipf.write(metadata_path, metadata_path.name)
                zipf.write(readme_path, readme_path.name)
            
            # Retourner le chemin du zip
            return str(zip_path)
    
    def publish_to_kaggle(self, dataset_data: Dict[str, Any]) -> Dict[str, Any]:
        """Publie le dataset sur Kaggle"""
        
        try:
            self._ensure_api_initialized()
            # Vérifier si on peut publier
            can_publish, conditions = self.can_publish_to_kaggle(dataset_data)
            
            if not can_publish:
                return {
                    'success': False,
                    'error': 'Dataset does not meet publication requirements',
                    'conditions': conditions
                }
            
            # Vérifier les doublons Kaggle
            is_duplicate, duplicate_reason = self.check_kaggle_duplicates(dataset_data)
            
            if is_duplicate:
                return {
                    'success': False,
                    'error': 'Duplicate dataset found on Kaggle',
                    'duplicate_reason': duplicate_reason
                }
            
            # Créer le package
            zip_path = self.create_dataset_package(dataset_data)
            
            # Définir les métadonnées Kaggle
            dataset_card = self.generate_dataset_card(dataset_data)
            
            # Publier sur Kaggle
            dataset_name = dataset_data.get('name', 'dataset')
            username = os.getenv('KAGGLE_USERNAME', 'user')
            
            # Créer ou mettre à jour le dataset
            try:
                # Essayer de créer un nouveau dataset
                self.api.dataset_create_new(
                    folder=os.path.dirname(zip_path),
                    public=False,  # Commencer en privé
                    quiet=False
                )
                
                action = 'created'
                
            except Exception as e:
                if 'already exists' in str(e).lower():
                    # Mettre à jour le dataset existant
                    self.api.dataset_create_version(
                        folder=os.path.dirname(zip_path),
                        version_notes=f"Updated on {datetime.utcnow().isoformat()}",
                        quiet=False
                    )
                    action = 'updated'
                else:
                    raise
            
            # Rendre public si la qualité est excellente
            if dataset_data.get('quality_score', 0) >= 9.0:
                try:
                    self.api.dataset_toggle_enable(f"{username}/{dataset_name}")
                    visibility = 'public'
                except:
                    visibility = 'private'
            else:
                visibility = 'private'
            
            return {
                'success': True,
                'action': action,
                'visibility': visibility,
                'dataset_url': f"https://www.kaggle.com/datasets/{username}/{dataset_name.lower().replace(' ', '-')}",
                'quality_score': dataset_data.get('quality_score', 0),
                'published_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error publishing to Kaggle: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'error_type': type(e).__name__
            }
    
    def get_kaggle_dataset_info(self, dataset_ref: str) -> Dict[str, Any]:
        """Récupère les informations d'un dataset Kaggle"""
        try:
            self._ensure_api_initialized()
            dataset = self.api.dataset_view(dataset_ref)
            
            return {
                'ref': dataset.ref,
                'title': dataset.title,
                'subtitle': dataset.subtitle,
                'description': dataset.description,
                'total_bytes': dataset.total_bytes,
                'view_count': dataset.view_count,
                'download_count': dataset.download_count,
                'vote_count': dataset.vote_count,
                'last_updated': dataset.last_updated,
                'usability_rating': dataset.usability_rating
            }
            
        except Exception as e:
            logger.error(f"Error getting Kaggle dataset info: {str(e)}")
            return {'error': str(e)}
