from app.db.init_db import recreate_database
from app.db.session import SessionLocal
from app.importers.solicitudes_importer import import_solicitudes_excel
from app.importers.planning_importer import import_planning_excel


REQUESTS_EXCEL_PATH = "P1801 Tabla Solicitudes FPeralta.xlsx"
PLANNING_EXCEL_PATH = "P1801 Planif estrategica Marzo).xlsx"


def main() -> None:
    recreate_database()
    print("Database recreated successfully.")

    session = SessionLocal()
    try:
        import_solicitudes_excel(session, REQUESTS_EXCEL_PATH)
        print("Requests Excel imported successfully.")

        import_planning_excel(session, PLANNING_EXCEL_PATH)
        print("Planning Excel imported successfully.")
    finally:
        session.close()


if __name__ == "__main__":
    main()