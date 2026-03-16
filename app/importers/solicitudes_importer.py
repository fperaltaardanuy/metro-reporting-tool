from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import (
    ApprovalStatus,
    ClosureResult,
    FunctionalArea,
    InterestGroupActivityType,
    InvestmentType,
    OutOfHoursType,
    Person,
    Priority,
    Request,
    RequestStatus,
    Requester,
    ServiceActivityType,
    System,
    WorkStatus,
)


COLUMN_MAP = {
    "ID": "id",
    "Solicitante": "requester",
    "Ámbito Funcional": "functional_area",
    "Fecha Solicitud": "request_date",
    "Asistencia Técnica Solicitada": "requested_assistance",
    "Relación de Necesidades": "needs_description",
    "Prioridad de AT": "priority",
    "Fecha Inicio AT": "planned_start_date",
    "Fecha Fin AT": "planned_end_date",
    "Días / Semana": "days_per_week_raw",
    "Horas / Día": "hours_per_day_raw",
    "Semanas / Mes": "weeks_per_month_raw",
    "¿Fuera de Jornada Laboral?": "out_of_hours_type",
    "% Fuera de Jornada Laboral": "out_of_hours_percentage",
    "Sistemas implicados": "systems",
    "Nombre del Trabajo del Área (destinatario de la AT)": "work_name",
    "Comentarios": "comments",
    "Documentación Confidencial": "confidential_documentation",
    "Inversión del Área": "investment_type",
    "SC": "sc_code",
    "Pedido": "purchase_order",
    "Duración AT (días)": "duration_days",
    "Estado Aprobación": "approval_status",
    "Fecha estado aprobación": "approval_status_date",
    "Solicitud Modificada": "modified_request_flag",
    "Solicitud Re-evaluada": "reevaluated_request_flag",
    "Estado Solicitud": "request_status",
    "Cerrada": "closure_result",
    "Encuesta respondida": "survey_responded_flag",
    "Fecha estado solicitud": "request_status_date",
    "Fecha comentarios": "comment_date",
    "Comentarios estado solicitud": "request_status_comments",
    "Responsable Ardanuy": "ardanuy_responsible",
    "Fecha inicio real": "actual_start_date",
    "Fecha fin real": "actual_end_date",
    "Modificado": "last_modified_at",
    "Modificado por": "last_modified_by",
    "Tipo de Actividad del Servicio (ARD)": "service_activity_type",
    "Tipo de Actividad del AII (Grupo de Interés)": "interest_group_activity_types",
    "Estado Trabajos": "work_status",
    "Fecha estado trabajos": "work_status_date",
    "Nº STC": "stc_number",
    "Fecha aprobación STC": "stc_approval_date",
    "Tipo de elemento": "element_type",
    "Ruta de acceso": "source_path",
}

REQUIRED_COLUMNS = [
    "ID",
    "Solicitante",
    "Ámbito Funcional",
    "Fecha Solicitud",
    "Asistencia Técnica Solicitada",
]


def import_solicitudes_excel(session: Session, excel_path: str) -> None:
    excel_file = Path(excel_path)

    if not excel_file.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_file}")

    df = pd.read_excel(excel_file)

    validate_required_columns(df)
    df = sanitize_dataframe(df)

    for _, row in df.iterrows():
        request_id = parse_int(row.get("ID"))
        if request_id is None:
            continue

        request = Request(
            id=request_id,
            requester=get_or_create_requester(session, row.get("Solicitante")),
            functional_area=get_or_create_functional_area(session, row.get("Ámbito Funcional")),
            request_date=parse_date(row.get("Fecha Solicitud")),
            requested_assistance=normalize_text(row.get("Asistencia Técnica Solicitada")),
            needs_description=normalize_text(row.get("Relación de Necesidades")),
            priority=get_or_create_priority(session, row.get("Prioridad de AT")),
            planned_start_date=parse_date(row.get("Fecha Inicio AT")),
            planned_end_date=parse_date(row.get("Fecha Fin AT")),
            days_per_week_raw=normalize_text(row.get("Días / Semana")),
            hours_per_day_raw=normalize_text(row.get("Horas / Día")),
            weeks_per_month_raw=normalize_text(row.get("Semanas / Mes")),
            out_of_hours_type=get_or_create_out_of_hours_type(session, row.get("¿Fuera de Jornada Laboral?")),
            out_of_hours_percentage=parse_percentage(row.get("% Fuera de Jornada Laboral")),
            work_name=normalize_text(row.get("Nombre del Trabajo del Área (destinatario de la AT)")),
            comments=normalize_text(row.get("Comentarios")),
            confidential_documentation=parse_bool(row.get("Documentación Confidencial")),
            investment_type=get_or_create_investment_type(session, row.get("Inversión del Área")),
            sc_code=normalize_text(row.get("SC")),
            purchase_order=normalize_text(row.get("Pedido")),
            duration_days=parse_int(row.get("Duración AT (días)")),
            approval_status=get_or_create_approval_status(session, row.get("Estado Aprobación")),
            approval_status_date=parse_date(row.get("Fecha estado aprobación")),
            modified_request_flag=parse_bool(row.get("Solicitud Modificada")),
            reevaluated_request_flag=parse_bool(row.get("Solicitud Re-evaluada")),
            request_status=get_or_create_request_status(session, row.get("Estado Solicitud")),
            closure_result=get_or_create_closure_result(session, row.get("Cerrada")),
            survey_responded_flag=parse_ok_flag(row.get("Encuesta respondida")),
            request_status_date=parse_date(row.get("Fecha estado solicitud")),
            comment_date=parse_date(row.get("Fecha comentarios")),
            request_status_comments=normalize_text(row.get("Comentarios estado solicitud")),
            ardanuy_responsible=get_or_create_person(session, row.get("Responsable Ardanuy")),
            actual_start_date=parse_date(row.get("Fecha inicio real")),
            actual_end_date=parse_date(row.get("Fecha fin real")),
            last_modified_at=parse_datetime(row.get("Modificado")),
            last_modified_by=get_or_create_person(session, row.get("Modificado por")),
            service_activity_type=get_or_create_service_activity_type(
                session, row.get("Tipo de Actividad del Servicio (ARD)")
            ),
            work_status=get_or_create_work_status(session, row.get("Estado Trabajos")),
            work_status_date=parse_date(row.get("Fecha estado trabajos")),
            stc_number=normalize_text(row.get("Nº STC")),
            stc_approval_date=parse_date(row.get("Fecha aprobación STC")),
            element_type=normalize_text(row.get("Tipo de elemento")),
            source_path=normalize_text(row.get("Ruta de acceso")),
        )

        request.systems = get_or_create_systems(session, row.get("Sistemas implicados"))
        request.interest_group_activity_types = get_or_create_interest_group_activity_types(
            session,
            row.get("Tipo de Actividad del AII (Grupo de Interés)"),
        )

        session.add(request)

    session.commit()


def validate_required_columns(df: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]
    return df


def normalize_text(value: object) -> Optional[str]:
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    replacements = {
        "NO aplica": "No aplica",
        "No Aplica": "No aplica",
    }
    return replacements.get(text, text)


def parse_int(value: object) -> Optional[int]:
    if pd.isna(value):
        return None

    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def parse_float(value: object) -> Optional[float]:
    if pd.isna(value):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_percentage(value: object) -> Optional[float]:
    if pd.isna(value):
        return None

    text = str(value).strip().replace("%", "").replace(",", ".")
    if not text:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def parse_bool(value: object) -> Optional[bool]:
    if pd.isna(value):
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        if value == 1 or value == 1.0:
            return True
        if value == 0 or value == 0.0:
            return False

    text = str(value).strip().lower()

    truthy = {
        "true",
        "verdadero",
        "sí",
        "si",
        "yes",
        "1",
        "1.0",
        "ok",
        "x",
    }
    falsy = {
        "false",
        "falso",
        "no",
        "0",
        "0.0",
    }

    if text in truthy:
        return True
    if text in falsy:
        return False

    return None

def parse_ok_flag(value: object) -> Optional[bool]:
    if pd.isna(value):
        return None

    text = str(value).strip().lower()
    if text in {"ok", "true", "verdadero", "sí", "si", "yes", "1"}:
        return True
    if text in {"false", "falso", "no", "0"}:
        return False

    return None


def parse_date(value: object) -> Optional[date]:
    if pd.isna(value):
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None

    return parsed.date()


def parse_datetime(value: object) -> Optional[datetime]:
    if pd.isna(value):
        return None

    if isinstance(value, datetime):
        return value

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None

    return parsed.to_pydatetime()


def split_multivalue(value: object) -> list[str]:
    text = normalize_text(value)
    if text is None:
        return []

    # SharePoint-style separator found in this file
    if ";#" in text:
        parts = [part.strip() for part in text.split(";#")]
    else:
        parts = [text.strip()]

    cleaned = []
    seen = set()

    for part in parts:
        if not part:
            continue
        if part not in seen:
            cleaned.append(part)
            seen.add(part)

    return cleaned


def get_or_create_by_name(session: Session, model, name: object):
    normalized_name = normalize_text(name)
    if normalized_name is None:
        return None

    existing = session.scalar(select(model).where(model.name == normalized_name))
    if existing is not None:
        return existing

    obj = model(name=normalized_name)
    session.add(obj)
    session.flush()
    return obj


def get_or_create_requester(session: Session, value: object) -> Optional[Requester]:
    return get_or_create_by_name(session, Requester, value)


def get_or_create_functional_area(session: Session, value: object) -> Optional[FunctionalArea]:
    return get_or_create_by_name(session, FunctionalArea, value)


def get_or_create_priority(session: Session, value: object) -> Optional[Priority]:
    return get_or_create_by_name(session, Priority, value)


def get_or_create_out_of_hours_type(session: Session, value: object) -> Optional[OutOfHoursType]:
    return get_or_create_by_name(session, OutOfHoursType, value)


def get_or_create_investment_type(session: Session, value: object) -> Optional[InvestmentType]:
    return get_or_create_by_name(session, InvestmentType, value)


def get_or_create_approval_status(session: Session, value: object) -> Optional[ApprovalStatus]:
    return get_or_create_by_name(session, ApprovalStatus, value)


def get_or_create_request_status(session: Session, value: object) -> Optional[RequestStatus]:
    return get_or_create_by_name(session, RequestStatus, value)


def get_or_create_closure_result(session: Session, value: object) -> Optional[ClosureResult]:
    return get_or_create_by_name(session, ClosureResult, value)


def get_or_create_person(session: Session, value: object) -> Optional[Person]:
    return get_or_create_by_name(session, Person, value)


def get_or_create_service_activity_type(session: Session, value: object) -> Optional[ServiceActivityType]:
    return get_or_create_by_name(session, ServiceActivityType, value)


def get_or_create_work_status(session: Session, value: object) -> Optional[WorkStatus]:
    return get_or_create_by_name(session, WorkStatus, value)


def get_or_create_systems(session: Session, value: object) -> list[System]:
    names = split_multivalue(value)
    return [get_or_create_by_name(session, System, name) for name in names]


def get_or_create_interest_group_activity_types(
    session: Session, value: object
) -> list[InterestGroupActivityType]:
    names = split_multivalue(value)
    return [get_or_create_by_name(session, InterestGroupActivityType, name) for name in names]