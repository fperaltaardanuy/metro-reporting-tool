from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import (
    BaselineUpdate,
    ChangeRequest,
    ChangeRequestStatus,
    ChangeType,
    ContractImpact,
    ElementType,
    FunctionalArea,
    ImpactAssessment,
    Person,
    Priority,
    Request,
    ServiceImpact,
    StakeholderImpact,
    WorkRequestImpact,
)


REQUIRED_COLUMNS = [
    "Departamento",
    "Fecha de solicitud",
    "Cambio propuesto",
    "Número de ST objeto de la solicitud del cambio",
    "Prioridad",
    "Tipo de Cambio",
    "Estado",
]

CHANGE_REQUEST_REFERENCE_COLUMN = "Número de ST objeto de la solicitud del cambio"
HEADER_DETECTION_COLUMN = CHANGE_REQUEST_REFERENCE_COLUMN


def import_change_requests_excel(session: Session, excel_path: str) -> None:
    excel_file = Path(excel_path)

    if not excel_file.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_file}")

    df = read_change_requests_dataframe(excel_file)
    validate_required_columns(df)

    for row_index, row in df.iterrows():
        change_request_id = parse_change_request_id(row.get("ID"))
        if change_request_id is None:
            continue
        functional_area = get_or_create_functional_area(session, row.get("Departamento"))
        priority = get_or_create_priority(session, row.get("Prioridad"))
        requester = get_or_create_person(session, row.get("Solicitante"))
        approver = get_or_create_person(session, row.get("Aprobador"))
        contract_impact = get_or_create_contract_impact(session, row.get("Impacto en Contrato"))
        work_request_impact = get_or_create_work_request_impact(
            session, row.get("Impacto en Solicitud de Trabajo")
        )
        stakeholder_impact = get_or_create_stakeholder_impact(
            session, row.get("Impacto en Implicados")
        )
        service_impact = get_or_create_service_impact(
            session, row.get("Impacto en Servicio")
        )
        impact_assessment = get_or_create_impact_assessment(
            session, row.get("Valoración del Impacto")
        )
        status = get_or_create_change_request_status(session, row.get("Estado"))
        baseline_update = get_or_create_baseline_update(
            session, row.get("Actualización de Línea base")
        )
        element_type = get_or_create_element_type(
            session, row.get("Tipo de elemento")
        )

        source_request_reference_raw = normalize_text(
            row.get(CHANGE_REQUEST_REFERENCE_COLUMN)
        )
        request = get_request_from_change_reference(
            session,
            source_request_reference_raw,
        )

        change_request = ChangeRequest(
            id=change_request_id,
            request_id=request.id if request is not None else None,
            source_request_reference_raw=source_request_reference_raw,
            functional_area_id=functional_area.id if functional_area is not None else None,
            priority_id=priority.id if priority is not None else None,
            requester_id=requester.id if requester is not None else None,
            approver_id=approver.id if approver is not None else None,
            contract_impact_id=contract_impact.id if contract_impact is not None else None,
            work_request_impact_id=(
                work_request_impact.id if work_request_impact is not None else None
            ),
            stakeholder_impact_id=(
                stakeholder_impact.id if stakeholder_impact is not None else None
            ),
            service_impact_id=(
                service_impact.id if service_impact is not None else None
            ),
            impact_assessment_id=(
                impact_assessment.id if impact_assessment is not None else None
            ),
            status_id=status.id if status is not None else None,
            baseline_update_id=(
                baseline_update.id if baseline_update is not None else None
            ),
            element_type_id=(
                element_type.id if element_type is not None else None
            ),
            modified_work_request_flag=parse_yes_no_bool(
                row.get("Modificada Solicitud de Trabajo")
            ),
            created_work_request_flag=parse_yes_no_bool(
                row.get("Creación de Solicitud de Trabajo")
            ),
            title=normalize_text(row.get("Cambio propuesto")),
            description=normalize_text(row.get("Justificación")),
            comments=normalize_text(row.get("Comentarios")),
            revised_assessment=normalize_text(row.get("Valoración Revisada")),
            request_date=parse_date(row.get("Fecha de solicitud")),
            approval_date=parse_date(row.get("Fecha de Estado")),
            estimated_implementation_date=parse_date(
                row.get("Plazo estimado implantación")
            ),
            last_modified_at=None,
            excel_row_number=int(row_index) + 2,
        )

        session.add(change_request)
        session.flush()

        change_request.change_types = get_or_create_change_types(
            session,
            row.get("Tipo de Cambio"),
        )

    session.commit()


def read_change_requests_dataframe(excel_file: Path) -> pd.DataFrame:
    raw_df = pd.read_excel(excel_file, header=None)
    header_row_index = find_header_row_index(raw_df)

    if header_row_index is None:
        preview_rows = raw_df.head(10).fillna("").astype(str).values.tolist()
        raise ValueError(
            "Could not locate the header row in the change requests Excel. "
            f"Expected column '{HEADER_DETECTION_COLUMN}'. "
            f"Top rows preview: {preview_rows}"
        )

    header_values = [normalize_header_value(value) for value in raw_df.iloc[header_row_index].tolist()]
    data_df = raw_df.iloc[header_row_index + 1 :].copy()
    data_df.columns = header_values
    data_df = sanitize_dataframe(data_df)

    # Drop fully empty rows
    data_df = data_df.dropna(how="all").reset_index(drop=True)

    return data_df


def find_header_row_index(raw_df: pd.DataFrame) -> Optional[int]:
    for row_index in range(len(raw_df)):
        row_values = [normalize_header_value(value) for value in raw_df.iloc[row_index].tolist()]
        if HEADER_DETECTION_COLUMN in row_values:
            return row_index
    return None


def validate_required_columns(df: pd.DataFrame) -> None:
    actual_columns = [str(col).strip() for col in df.columns]
    missing = [col for col in REQUIRED_COLUMNS if col not in actual_columns]

    if missing:
        raise ValueError(
            "Missing required columns in change requests Excel.\n"
            f"Missing: {missing}\n"
            f"Detected columns: {actual_columns}"
        )


def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_header_value(col) for col in df.columns]
    return df


def normalize_header_value(value: object) -> str:
    if pd.isna(value):
        return ""

    text = str(value).strip()
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_text(value: object) -> Optional[str]:
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    return text


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


def parse_yes_no_bool(value: object) -> Optional[bool]:
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

    truthy = {"si", "sí", "yes", "true", "verdadero", "1", "1.0"}
    falsy = {"no", "false", "falso", "0", "0.0"}

    if text in truthy:
        return True
    if text in falsy:
        return False

    return None


def split_multivalue(value: object) -> list[str]:
    text = normalize_text(value)
    if text is None:
        return []

    if ";#" in text:
        parts = [part.strip() for part in text.split(";#")]
    else:
        parts = [text.strip()]

    cleaned: list[str] = []
    seen: set[str] = set()

    for part in parts:
        if not part:
            continue
        if part not in seen:
            cleaned.append(part)
            seen.add(part)

    return cleaned


def parse_request_id_from_reference(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None

    stripped = value.strip()

    if stripped.isdigit():
        return int(stripped)

    st_match = re.search(r"\bST\s*0*(\d+)\b", stripped, flags=re.IGNORECASE)
    if st_match:
        return int(st_match.group(1))

    first_number_match = re.search(r"\b0*(\d+)\b", stripped)
    if first_number_match:
        return int(first_number_match.group(1))

    return None


def get_request_from_change_reference(
    session: Session,
    source_request_reference_raw: Optional[str],
) -> Optional[Request]:
    request_id = parse_request_id_from_reference(source_request_reference_raw)
    if request_id is None:
        return None

    return session.scalar(
        select(Request).where(Request.id == request_id)
    )


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


def get_or_create_functional_area(
    session: Session,
    value: object,
) -> Optional[FunctionalArea]:
    return get_or_create_by_name(session, FunctionalArea, value)


def get_or_create_priority(
    session: Session,
    value: object,
) -> Optional[Priority]:
    return get_or_create_by_name(session, Priority, value)


def get_or_create_person(
    session: Session,
    value: object,
) -> Optional[Person]:
    return get_or_create_by_name(session, Person, value)


def get_or_create_contract_impact(
    session: Session,
    value: object,
) -> Optional[ContractImpact]:
    return get_or_create_by_name(session, ContractImpact, value)


def get_or_create_work_request_impact(
    session: Session,
    value: object,
) -> Optional[WorkRequestImpact]:
    return get_or_create_by_name(session, WorkRequestImpact, value)


def get_or_create_stakeholder_impact(
    session: Session,
    value: object,
) -> Optional[StakeholderImpact]:
    return get_or_create_by_name(session, StakeholderImpact, value)


def get_or_create_service_impact(
    session: Session,
    value: object,
) -> Optional[ServiceImpact]:
    return get_or_create_by_name(session, ServiceImpact, value)


def get_or_create_impact_assessment(
    session: Session,
    value: object,
) -> Optional[ImpactAssessment]:
    return get_or_create_by_name(session, ImpactAssessment, value)


def get_or_create_change_request_status(
    session: Session,
    value: object,
) -> Optional[ChangeRequestStatus]:
    return get_or_create_by_name(session, ChangeRequestStatus, value)


def get_or_create_baseline_update(
    session: Session,
    value: object,
) -> Optional[BaselineUpdate]:
    return get_or_create_by_name(session, BaselineUpdate, value)


def get_or_create_element_type(
    session: Session,
    value: object,
) -> Optional[ElementType]:
    return get_or_create_by_name(session, ElementType, value)


def get_or_create_change_types(
    session: Session,
    value: object,
) -> list[ChangeType]:
    names = split_multivalue(value)
    result: list[ChangeType] = []

    for name in names:
        existing = session.scalar(select(ChangeType).where(ChangeType.name == name))
        if existing is not None:
            result.append(existing)
            continue

        obj = ChangeType(name=name)
        session.add(obj)
        session.flush()
        result.append(obj)

    return result

def parse_change_request_id(value: object) -> Optional[int]:
    if pd.isna(value):
        return None

    try:
        return int(float(value))
    except (TypeError, ValueError):
        text = str(value).strip()
        if not text:
            return None
        try:
            return int(float(text.replace(",", ".")))
        except ValueError:
            return None