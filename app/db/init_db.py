from pathlib import Path

from sqlalchemy import create_engine

from app.db.models import Base


DB_FILE = "metro_requests.db"


def recreate_database() -> None:
    db_path = Path(DB_FILE)

    if db_path.exists():
        db_path.unlink()

    engine = create_engine(f"sqlite:///{DB_FILE}", echo=False, future=True)
    Base.metadata.create_all(bind=engine)