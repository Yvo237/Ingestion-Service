import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder
from sklearn.impute import SimpleImputer
from feature_engine.imputation import MeanMedianImputer, ArbitraryNumberImputer
from feature_engine.outliers import Winsorizer, OutlierTrimmer
from feature_engine.encoding import RareLabelEncoder
import json
from datetime import datetime


class DataProcessor:
    """Service pour nettoyer et enrichir les données avec tracking du lineage"""
    
    def __init__(self):
        self.processing_log = []
        self.original_shape = None
        self.current_step = 0
    
    def process_dataset(self, df: pd.DataFrame, profile_result: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Pipeline complet de traitement avec data lineage"""
        
        # Always work on a copy to avoid SettingWithCopyWarning and keep original data intact
        df = df.copy()
        self.original_shape = df.shape
        self.processing_log = []
        
        # Étape 1: Validation et nettoyage de base
        df = self._basic_cleaning(df)
        
        # Étape 2: Gestion des valeurs manquantes
        df = self._handle_missing_values(df)
        
        # Étape 3: Gestion des outliers
        df = self._handle_outliers(df)
        
        # Étape 4: Normalisation des types
        df = self._normalize_types(df)
        
        # Étape 5: Feature engineering (si applicable)
        df = self._feature_engineering(df)
        
        # Générer le data lineage final
        lineage = self._generate_lineage(final_shape=df.shape)
        
        return df, lineage
    
    def _add_log(self, step: str, action: str, details: Dict[str, Any] = None):
        """Ajoute une entrée au log de traitement"""
        log_entry = {
            'step': step,
            'action': action,
            'timestamp': datetime.utcnow().isoformat(),
            'details': details or {}
        }
        self.processing_log.append(log_entry)
        self.current_step += 1
    
    def _basic_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """Nettoyage de base du dataset"""
        self._add_log('basic_cleaning', 'start', {'original_shape': df.shape})
        
        # Supprimer les colonnes entièrement vides
        empty_cols = df.columns[df.isnull().all()].tolist()
        if empty_cols:
            df = df.drop(columns=empty_cols)
            self._add_log('basic_cleaning', 'drop_empty_columns', {
                'columns_dropped': empty_cols,
                'reason': 'entirely empty'
            })
        
        # Supprimer les lignes entièrement vides
        empty_rows = df[df.isnull().all(axis=1)].index
        if len(empty_rows) > 0:
            df = df.drop(index=empty_rows)
            self._add_log('basic_cleaning', 'drop_empty_rows', {
                'rows_dropped': len(empty_rows),
                'reason': 'entirely empty'
            })
        
        # Nettoyer les noms de colonnes
        original_columns = df.columns.tolist()
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace(r'[^\w]', '_', regex=True)
        
        if df.columns.tolist() != original_columns:
            self._add_log('basic_cleaning', 'clean_column_names', {
                'original_columns': original_columns,
                'new_columns': df.columns.tolist()
            })
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gestion intelligente des valeurs manquantes"""
        self._add_log('missing_values', 'start')
        
        for column in df.columns:
            missing_count = df[column].isnull().sum()
            if missing_count == 0:
                continue
            
            missing_ratio = missing_count / len(df)
            
            # Stratégie basée sur le type et le ratio de valeurs manquantes
            if missing_ratio > 0.5:
                # Plus de 50% de valeurs manquantes -> supprimer la colonne
                df = df.drop(columns=[column])
                self._add_log('missing_values', 'drop_column', {
                    'column': column,
                    'missing_ratio': missing_ratio,
                    'reason': 'too many missing values (>50%)'
                })
            elif pd.api.types.is_numeric_dtype(df[column]):
                # Colonnes numériques
                if missing_ratio < 0.1:
                    # Moins de 10% -> imputation par médiane
                    median_value = df[column].median()
                    df.loc[:, column] = df[column].fillna(median_value)
                    self._add_log('missing_values', 'impute_median', {
                        'column': column,
                        'missing_count': missing_count,
                        'imputed_value': median_value
                    })
                else:
                    # 10-50% -> imputation par médiane pour réduire la complexité
                    imputer = SimpleImputer(strategy='median')
                    df.loc[:, column] = imputer.fit_transform(df[[column]]).ravel()
                    self._add_log('missing_values', 'impute_median', {
                        'column': column,
                        'missing_count': missing_count,
                        'method': 'median'
                    })
            else:
                # Colonnes catégorielles et non numériques
                if missing_ratio < 0.2:
                    # Moins de 20% -> imputation par mode
                    mode_value = df[column].mode()[0] if not df[column].mode().empty else 'unknown'
                    df.loc[:, column] = df[column].fillna(mode_value)
                    self._add_log('missing_values', 'impute_mode', {
                        'column': column,
                        'missing_count': missing_count,
                        'imputed_value': mode_value
                    })
                else:
                    # Plus de 20% -> catégorie 'missing'
                    df.loc[:, column] = df[column].fillna('missing')
                    self._add_log('missing_values', 'impute_missing_category', {
                        'column': column,
                        'missing_count': missing_count,
                        'new_category': 'missing'
                    })
        
        return df
    
    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gestion des outliers avec méthode IQR"""
        self._add_log('outliers', 'start')
        
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        for column in numeric_columns:
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
            
            if len(outliers) > 0:
                outlier_ratio = len(outliers) / len(df)
                
                if outlier_ratio > 0.1:
                    # Plus de 10% d'outliers -> utiliser Winsorization
                    df[column] = np.clip(df[column], lower_bound, upper_bound)
                    self._add_log('outliers', 'winsorize', {
                        'column': column,
                        'outliers_count': len(outliers),
                        'outlier_ratio': outlier_ratio,
                        'method': 'IQR clipping',
                        'bounds': [lower_bound, upper_bound]
                    })
                else:
                    # Moins de 10% -> supprimer les outliers
                    df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
                    self._add_log('outliers', 'remove', {
                        'column': column,
                        'outliers_count': len(outliers),
                        'outlier_ratio': outlier_ratio,
                        'method': 'IQR removal'
                    })
        
        return df
    
    def _normalize_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalisation et conversion des types de données"""
        self._add_log('type_normalization', 'start')
        
        for column in df.columns:
            # Tentative de conversion en numérique
            if df[column].dtype == 'object':
                # Vérifier si c'est numérique
                try:
                    numeric_series = pd.to_numeric(df[column], errors='coerce')
                    if not numeric_series.isna().all():
                        df[column] = numeric_series
                        self._add_log('type_normalization', 'convert_to_numeric', {
                            'column': column,
                            'original_type': 'object',
                            'new_type': str(numeric_series.dtype)
                        })
                        continue
                except:
                    pass
                
                # Vérifier si c'est une date
                try:
                    date_series = pd.to_datetime(df[column], errors='coerce')
                    if not date_series.isna().all():
                        df[column] = date_series
                        self._add_log('type_normalization', 'convert_to_datetime', {
                            'column': column,
                            'original_type': 'object',
                            'new_type': 'datetime64[ns]'
                        })
                        continue
                except:
                    pass
                
                # Garder comme string mais nettoyer
                df[column] = df[column].astype(str).str.strip()
                self._add_log('type_normalization', 'clean_string', {
                    'column': column,
                    'action': 'strip and convert to string'
                })
        
        return df
    
    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """Feature engineering de base"""
        self._add_log('feature_engineering', 'start')
        
        # Ajouter des features temporelles si des dates sont détectées
        date_columns = df.select_dtypes(include=['datetime64']).columns
        
        for col in date_columns:
            try:
                df[f'{col}_year'] = df[col].dt.year
                df[f'{col}_month'] = df[col].dt.month
                df[f'{col}_day'] = df[col].dt.day
                df[f'{col}_weekday'] = df[col].dt.weekday
                
                self._add_log('feature_engineering', 'add_temporal_features', {
                    'source_column': col,
                    'new_features': [f'{col}_year', f'{col}_month', f'{col}_day', f'{col}_weekday']
                })
            except Exception as e:
                self._add_log('feature_engineering', 'temporal_features_failed', {
                    'column': col,
                    'error': str(e)
                })
        
        # Ajouter des features d'interaction pour les colonnes numériques
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        if len(numeric_columns) >= 2:
            # Ajouter le ratio entre les deux premières colonnes numériques
            try:
                col1, col2 = numeric_columns[0], numeric_columns[1]
                if (df[col2] != 0).all():
                    df[f'{col1}_{col2}_ratio'] = df[col1] / df[col2]
                    self._add_log('feature_engineering', 'add_ratio_feature', {
                        'columns': [col1, col2],
                        'new_feature': f'{col1}_{col2}_ratio'
                    })
            except Exception as e:
                self._add_log('feature_engineering', 'ratio_feature_failed', {
                    'columns': [numeric_columns[0], numeric_columns[1]],
                    'error': str(e)
                })
        
        return df
    
    def _generate_lineage(self, final_shape: tuple) -> Dict[str, Any]:
        """Génère le data lineage complet"""
        return {
            'original_shape': self.original_shape,
            'final_shape': final_shape,
            'transformations': self.processing_log,
            'quality_improvements': self._calculate_quality_improvements(),
            'processing_summary': {
                'total_steps': len(self.processing_log),
                'processing_time': datetime.utcnow().isoformat(),
                'major_transformations': len([log for log in self.processing_log if log['details'].get('rows_dropped', 0) > 0 or log['details'].get('columns_dropped', 0) > 0])
            }
        }
    
    def _calculate_quality_improvements(self) -> Dict[str, Any]:
        """Calcule les améliorations de qualité"""
        improvements = {
            'missing_values_handled': 0,
            'outliers_handled': 0,
            'columns_added': 0,
            'columns_removed': 0,
            'rows_removed': 0
        }
        
        for log in self.processing_log:
            if 'missing' in log['action'].lower():
                improvements['missing_values_handled'] += 1
            elif 'outlier' in log['action'].lower():
                improvements['outliers_handled'] += 1
            elif 'add' in log['action'].lower() and 'feature' in log['action'].lower():
                improvements['columns_added'] += len(log['details'].get('new_features', []))
            elif 'drop' in log['action'].lower():
                improvements['columns_removed'] += len(log['details'].get('columns_dropped', []))
                improvements['rows_removed'] += log['details'].get('rows_dropped', 0)
        
        return improvements
