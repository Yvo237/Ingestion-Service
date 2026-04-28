from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

# Utiliser SQLite pour le développement (remplacer par PostgreSQL en production)
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data_collection.db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()