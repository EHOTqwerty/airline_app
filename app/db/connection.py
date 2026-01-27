from sqlalchemy import create_engine
from app.config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
        _engine = create_engine(url, pool_pre_ping=True, future=True)
    return _engine
