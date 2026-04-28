from sqlalchemy import Column, Integer, String, JSON, DateTime, Text, DECIMAL, ARRAY, BigInteger, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class DatasetModel(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String(255), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    status = Column(String(20), default='raw', index=True)  # raw, processing, cleaned, premium, published, rejected
    quality_score = Column(DECIMAL(3, 2), index=True)  # 0.00-10.00
    raw_data = Column(JSON)
    cleaned_data = Column(JSON)
    headers = Column(ARRAY(Text), nullable=False)
    row_count = Column(Integer)
    file_size = Column(BigInteger)
    file_hash = Column(String(64), unique=True, index=True)
    dataset_metadata = Column('metadata', JSON)
    processing_log = Column(JSON)
    kaggle_info = Column(JSON)
    analysis_type = Column(String(100))
    analysis_parameters = Column(JSON)
    analysis_results = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, index=True)
    
    # Relation avec les logs de traitement
    processing_logs = relationship("ProcessingLogModel", back_populates="dataset", cascade="all, delete-orphan")
    
    # Relation avec les logs d'emails
    email_logs = relationship("EmailLogModel", back_populates="dataset", cascade="all, delete-orphan")

class ProcessingLogModel(Base):
    __tablename__ = "processing_logs"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False, index=True)
    log_level = Column(String(20), nullable=False)
    step = Column(String(50), nullable=False, index=True)
    message = Column(Text, nullable=False)
    details = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Relation avec le dataset
    dataset = relationship("DatasetModel", back_populates="processing_logs")

class EmailLogModel(Base):
    """Modèle pour logger les envois d'emails"""
    __tablename__ = "email_logs"

    id = Column(Integer, primary_key=True, index=True)
    email_type = Column(String(50), nullable=False, index=True)  # kaggle_publication, digest, etc.
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=True, index=True)
    recipient_email = Column(String(255), nullable=False)
    subject = Column(Text, nullable=False)
    status = Column(String(20), nullable=False, index=True)  # sent, failed
    error_message = Column(Text, nullable=True)
    sent_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Relation avec le dataset (optionnel)
    dataset = relationship("DatasetModel", back_populates="email_logs")