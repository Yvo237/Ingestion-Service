import pandas as pd
import json
import hashlib
from typing import Dict, Any, Tuple, List
# from ydata_profiling import ProfileReport
# from ydata_profiling.model.alerts import AlertType
import numpy as np
from datetime import datetime


class DataProfiler:
    """Service pour analyser la qualité des données avec ydata-profiling"""
    
    def __init__(self):
        self.alert_weights = {
            'HIGH_CARDINALITY': 0.1,
            'UNIFORMITY': 0.2,
            'MISSING': 0.3,
            'SKEWED': 0.15,
            'ZEROS': 0.1,
            'NEGATIVES': 0.05,
            'INFINITE': 0.3,
            'TYPE_DATE': 0.05,
            'DUPLICATES': 0.25,
            'CORRELATION': 0.1,
            'REJECTED': 0.4,
            'CONSTANT': 0.2,
            'UNSUPPORTED': 0.3
        }
    
    def generate_file_hash(self, content: str) -> str:
        """Génère un hash SHA256 du contenu du fichier"""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    def generate_content_hash(self, df: pd.DataFrame) -> str:
        """Génère un hash basé sur le contenu des données (structure + valeurs)"""
        # Normaliser les données pour une comparaison fiable
        df_normalized = df.copy()
        for col in df_normalized.select_dtypes(include=[np.number]).columns:
            df_normalized[col] = df_normalized[col].round(6)
        
        # Trier par toutes les colonnes pour ordre cohérent
        df_sorted = df_normalized.sort_values(by=list(df_normalized.columns))
        
        # Convertir en JSON et hasher
        content = df_sorted.to_json(orient='records', sort_keys=True)
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    def analyze_dataset(self, df: pd.DataFrame, dataset_name: str = None) -> Dict[str, Any]:
        """Analyse complète du dataset avec méthodes simplifiées"""
        
        # Métriques de base
        n_rows = len(df)
        n_columns = len(df.columns)
        n_cells = n_rows * n_columns
        n_cells_with_missing = df.isnull().sum().sum()
        n_duplicates = df.duplicated().sum()
        
        # Métriques de complétude
        completeness_ratio = 1 - (n_cells_with_missing / n_cells) if n_cells > 0 else 0
        
        # Métriques par type
        type_metrics = {}
        for col_name in df.columns:
            col_type = str(df[col_name].dtype)
            if col_type not in type_metrics:
                type_metrics[col_type] = {'count': 0, 'columns': []}
            type_metrics[col_type]['count'] += 1
            type_metrics[col_type]['columns'].append(col_name)
        
        # Détecter les alertes simples
        alerts = []
        
        # Alertes pour valeurs manquantes
        missing_cols = df.columns[df.isnull().any()].tolist()
        for col in missing_cols:
            missing_ratio = df[col].isnull().sum() / n_rows
            alerts.append({
                'type': 'MISSING',
                'column': col,
                'description': f'{missing_ratio:.1%} of values missing',
                'severity': 'high' if missing_ratio > 0.3 else 'medium' if missing_ratio > 0.1 else 'low',
                'weight': 0.3 * missing_ratio
            })
        
        # Alertes pour doublons
        if n_duplicates > 0:
            duplicate_ratio = n_duplicates / n_rows
            alerts.append({
                'type': 'DUPLICATES',
                'column': None,
                'description': f'{n_duplicates} duplicate rows found',
                'severity': 'high' if duplicate_ratio > 0.1 else 'medium',
                'weight': 0.25 * duplicate_ratio
            })
        
        # Calculer les corrélations simples (pour les colonnes numériques)
        correlations = {}
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) >= 2:
            corr_matrix = df[numeric_cols].corr()
            correlations = corr_matrix.to_dict()
        
        # Calculer le score de qualité
        quality_score = self._calculate_quality_score(
            n_rows, n_columns, completeness_ratio, n_duplicates/n_rows, alerts, type_metrics
        )
        
        # Métriques de base
        basic_metrics = {
            'n_rows': n_rows,
            'n_columns': n_columns,
            'n_cells': n_cells,
            'n_cells_with_missing': n_cells_with_missing,
            'n_duplicates': n_duplicates,
            'memory_size': df.memory_usage(deep=True).sum()
        }
        
        # Métriques de complétude
        completeness_metrics = {
            'completeness_ratio': completeness_ratio,
            'complete_columns': len([c for c in df.columns if df[c].isnull().sum() == 0]),
            'columns_with_missing': len(missing_cols)
        }
        
        return {
            'basic_metrics': basic_metrics,
            'completeness_metrics': completeness_metrics,
            'type_metrics': type_metrics,
            'alerts': alerts,
            'correlations': correlations,
            'quality_score': quality_score,
            'profiling_json': json.dumps({
                'basic_metrics': basic_metrics,
                'quality_score': quality_score,
                'alerts_count': len(alerts)
            }),
            'analysis_timestamp': datetime.utcnow().isoformat()
        }
    
    def _get_alert_severity(self, alert_type) -> str:
        """Détermine la sévérité d'une alerte"""
        high_severity_alerts = [
            'MISSING', 'INFINITE', 'REJECTED', 'UNSUPPORTED'
        ]
        medium_severity_alerts = [
            'DUPLICATES', 'CONSTANT', 'UNIFORMITY'
        ]
        
        if alert_type in high_severity_alerts:
            return 'high'
        elif alert_type in medium_severity_alerts:
            return 'medium'
        else:
            return 'low'
    
    def _calculate_quality_score(self, n_rows: int, n_cols: int, completeness_ratio: float, 
                               duplicate_ratio: float, alerts: List[Dict], type_metrics: Dict) -> float:
        """Calcule un score de qualité de 0 à 10"""
        
        score = 10.0
        
        # Pénalités pour les valeurs manquantes
        missing_penalty = (1 - completeness_ratio) * 3
        score -= missing_penalty
        
        # Pénalités pour les doublons
        duplicate_penalty = duplicate_ratio * 2
        score -= duplicate_penalty
        
        # Pénalités pour les alertes
        total_alert_weight = sum(alert['weight'] for alert in alerts)
        alert_penalty = min(total_alert_weight * 2, 4)  # Max 4 points de pénalité
        score -= alert_penalty
        
        # Bonus pour les types de données variés
        type_diversity_bonus = min(len(type_metrics) * 0.2, 1)  # Max 1 point de bonus
        score += type_diversity_bonus
        
        # Bonus pour la taille appropriée
        if 100 <= n_rows <= 10000:
            size_bonus = 0.5
        elif 50 <= n_rows <= 50000:
            size_bonus = 0.3
        else:
            size_bonus = 0
        score += size_bonus
        
        # Bonus pour le nombre de colonnes approprié
        if 5 <= n_cols <= 50:
            columns_bonus = 0.5
        elif 3 <= n_cols <= 100:
            columns_bonus = 0.3
        else:
            columns_bonus = 0
        score += columns_bonus
        
        # Assurer que le score reste entre 0 et 10
        return max(0.0, min(10.0, round(score, 2)))
    
    def get_quality_recommendations(self, profile_result: Dict[str, Any]) -> List[str]:
        """Génère des recommandations basées sur l'analyse"""
        recommendations = []
        
        # Recommandations pour les valeurs manquantes
        completeness = profile_result['completeness_metrics']['completeness_ratio']
        if completeness < 0.95:
            recommendations.append(
                f"Le dataset a {(1-completeness)*100:.1f}% de valeurs manquantes. "
                "Considérez l'imputation ou la suppression des lignes/colonnes concernées."
            )
        
        # Recommandations pour les doublons
        if profile_result['basic_metrics']['n_duplicates'] > 0:
            recommendations.append(
                f"{profile_result['basic_metrics']['n_duplicates']} doublons détectés. "
                "Supprimez-les pour améliorer la qualité."
            )
        
        # Recommandations pour les alertes
        high_alerts = [a for a in profile_result['alerts'] if a['severity'] == 'high']
        if high_alerts:
            recommendations.append(
                f"{len(high_alerts)} alertes de haute sévérité détectées. "
                "Examinez ces colonnes en priorité."
            )
        
        # Recommandations pour la taille
        n_rows = profile_result['basic_metrics']['n_rows']
        if n_rows < 100:
            recommendations.append(
                "Dataset très petit (< 100 lignes). Les résultats d'analyse pourraient ne pas être fiables."
            )
        elif n_rows > 50000:
            recommendations.append(
                "Dataset très grand (> 50k lignes). Considérez l'échantillonnage pour accélérer le traitement."
            )
        
        # Recommandations pour les types
        if 'Unsupported' in profile_result['type_metrics']:
            recommendations.append(
                "Colonnes avec types non supportés détectés. "
                "Convertissez-les en types standards (numérique, texte, date)."
            )
        
        return recommendations if recommendations else ["Dataset de bonne qualité, aucune action particulière requise."]
