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
    quality_score = Column(DECIMAL(4, 2), index=True)  # 0.00-10.00
    raw_data = Column(JSON)
    cleaned_data = Column(JSON)
    headers = Column(ARRAY(Text), nullable=False)
    row_count = Column(Integer)
    file_size = Column(BigInteger)
    file_hash = Column(String(64), unique=True, index=True)
    dataset_metadata = Column('metadata', JSON)
    processing_log = Column(JSON)
    analysis_type = Column(String(100))
    analysis_parameters = Column(JSON)
    analysis_results = Column(JSON)
    storage_path = Column(String(512), nullable=True)
    results_path = Column(String(512), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, index=True)
    
    # Relation avec les logs de traitement
    processing_logs = relationship("ProcessingLogModel", back_populates="dataset", cascade="all, delete-orphan")

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