from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import (
    FunctionalArea,
    PlanningItem,
    PlanningLine,
    PlanningTimeValue,
    ReportCode,
    Request,
    Responsible,
)


@dataclass
class WeeklyColumn:
    column_index: int
    week_index: int
    year: int
    month: int


PLAN_EST_SHEET = "2. PlanEst Consolidada"
PLAN_REAL_SHEET = "3. PlanReal"
REPORT_CODE_SHEET = "090"
REQUEST_AMOUNT_SHEET = "Importe ST"
STOP_PLANNING_ID = 999


def import_planning_excel(session: Session, excel_path: str) -> None:
    excel_file = Path(excel_path)

    if not excel_file.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_file}")

    import_report_codes(session, excel_path)
    import_request_amounts(session, excel_path)

    import_planning_sheet(
        session=session,
        excel_path=excel_path,
        sheet_name=PLAN_EST_SHEET,
        source_type="estimated",
        header_row_index=6,
        dates_row_index=5,
        finish_column_name="Fin",
        state_column_name="Estado",
    )
    import_planning_sheet(
        session=session,
        excel_path=excel_path,
        sheet_name=PLAN_REAL_SHEET,
        source_type="real",
        header_row_index=3,
        dates_row_index=2,
        finish_column_name="PER",
        state_column_name=None,
    )

    session.commit()


def import_report_codes(session: Session, excel_path: str) -> None:
    raw_df = pd.read_excel(excel_path, sheet_name=REPORT_CODE_SHEET, header=None)

    header_row_index = find_row_index_by_first_value(raw_df, target_value="Código", column_index=1)
    if header_row_index is None:
        raise ValueError("Could not locate report code table header in sheet '090'.")

    data_start = header_row_index + 1

    for row_index in range(data_start, len(raw_df)):
        code = normalize_text(raw_df.iat[row_index, 1])

        if code is None:
            continue

        if code.upper() == "TOTAL":
            break

        report_code = get_or_create_report_code(session, code)

        report_code.unit_of_measure = normalize_text(raw_df.iat[row_index, 2])
        report_code.denomination = normalize_text(raw_df.iat[row_index, 3])
        report_code.at_unit_hours = parse_float(raw_df.iat[row_index, 4])
        report_code.unit_price = parse_float(raw_df.iat[row_index, 5])

    session.flush()


def import_request_amounts(session: Session, excel_path: str) -> None:
    raw_df = pd.read_excel(excel_path, sheet_name=REQUEST_AMOUNT_SHEET, header=None)

    header_row_index = find_request_amount_header_row_index(raw_df)
    if header_row_index is None:
        raise ValueError("Could not locate request amount table header in sheet 'Importe ST'.")

    header_values = [normalize_header_value(value) for value in raw_df.iloc[header_row_index].tolist()]
    data_df = raw_df.iloc[header_row_index + 1 :].copy()
    data_df.columns = header_values
    data_df = sanitize_dataframe(data_df)
    data_df = data_df.dropna(how="all").reset_index(drop=True)

    required_columns = {"ST", "Importe"}
    actual_columns = set(data_df.columns)
    missing = required_columns - actual_columns
    if missing:
        raise ValueError(
            f"Missing required columns in sheet 'Importe ST': {sorted(missing)}. "
            f"Detected columns: {list(data_df.columns)}"
        )

    for _, row in data_df.iterrows():
        request_id = parse_int(row.get("ST"))
        if request_id is None:
            continue

        amount = parse_float(row.get("Importe"))
        if amount is None:
            continue

        request = session.scalar(
            select(Request).where(Request.id == request_id)
        )
        if request is None:
            continue

        request.amount = amount

    session.flush()


def find_request_amount_header_row_index(raw_df: pd.DataFrame) -> Optional[int]:
    for row_index in range(len(raw_df)):
        row_values = [normalize_header_value(value) for value in raw_df.iloc[row_index].tolist()]
        if "ST" in row_values and "Importe" in row_values:
            return row_index
    return None


def import_planning_sheet(
    session: Session,
    excel_path: str,
    sheet_name: str,
    source_type: str,
    header_row_index: int,
    dates_row_index: int,
    finish_column_name: str,
    state_column_name: Optional[str],
) -> None:
    raw_df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)

    header_values = raw_df.iloc[header_row_index].tolist()
    column_names = [normalize_header_value(v) for v in header_values]
    date_row = raw_df.iloc[dates_row_index].tolist()

    weekly_columns = extract_weekly_columns(header_values, date_row)

    data_df = raw_df.iloc[header_row_index + 1 :].copy()
    data_df.columns = column_names

    stop_reading_items = False
    last_valid_planning_id: Optional[int] = None

    for data_pos, (original_row_index, row) in enumerate(data_df.iterrows()):
        planning_id = normalize_planning_id(row.get("ID"))

        if planning_id is None:
            continue

        functional_area_name = normalize_text(row.get("Ámbito Funcional"))
        requested_assistance = normalize_text(row.get("Asistencia Técnica Solicitada"))
        duration = parse_float(row.get("Duración (s)"))
        start = parse_date(row.get("Inicio"))
        finish = parse_date(row.get(finish_column_name))
        report_code_value = normalize_text(row.get("Código de Informe"))
        responsible_code = normalize_text(row.get("RESP"))

        functional_area = get_or_create_functional_area(session, functional_area_name)

        is_first_row_for_this_id = planning_id != last_valid_planning_id

        if is_first_row_for_this_id:
            if planning_id == STOP_PLANNING_ID:
                stop_reading_items = True

            planning_item = get_or_create_planning_item(
                session=session,
                planning_id=planning_id,
                functional_area=functional_area,
                requested_assistance=requested_assistance,
                duration=duration,
                start=start,
                finish=finish,
            )

            last_valid_planning_id = planning_id
            continue

        detail_row = is_detail_row(
            report_code=report_code_value,
            responsible_code=responsible_code,
            contract_assigned=row.get("Contrato Asignado"),
            hours_per_week=row.get("h/semana"),
        )

        if not detail_row:
            last_valid_planning_id = planning_id
            continue

        planning_item = session.scalar(
            select(PlanningItem).where(PlanningItem.planning_id == planning_id)
        )

        if planning_item is None:
            planning_item = get_or_create_planning_item(
                session=session,
                planning_id=planning_id,
                functional_area=functional_area,
                requested_assistance=requested_assistance,
                duration=duration,
                start=start,
                finish=finish,
            )

        responsible = get_or_create_responsible(session, responsible_code)
        report_code = None
        if report_code_value is not None:
            report_code = get_or_create_report_code(session, report_code_value)

        planning_line = PlanningLine(
            planning_item_id=planning_item.id,
            source_type=source_type,
            functional_area_id=functional_area.id if functional_area else None,
            requested_assistance=requested_assistance or planning_item.requested_assistance,
            report_code=report_code.code if report_code else None,
            responsible_id=responsible.id if responsible else None,
            contract_assigned=normalize_text(row.get("Contrato Asignado")),
            hours_per_week=parse_float(row.get("h/semana")),
            state=normalize_text(row.get(state_column_name)) if state_column_name else None,
            start=start,
            finish=finish,
            excel_row_number=int(original_row_index) + 1,
        )

        session.add(planning_line)
        session.flush()

        insert_weekly_time_values(
            session=session,
            planning_line=planning_line,
            raw_row=raw_df.iloc[original_row_index],
            weekly_columns=weekly_columns,
        )

        if planning_id == STOP_PLANNING_ID:
            next_data_pos = data_pos + 1
            if next_data_pos >= len(data_df):
                break

            next_original_row_index = data_df.index[next_data_pos]
            next_row = data_df.loc[next_original_row_index]
            next_planning_id = normalize_planning_id(next_row.get("ID"))

            if next_planning_id is None:
                break

        last_valid_planning_id = planning_id

    session.flush()


def find_row_index_by_first_value(
    df: pd.DataFrame,
    target_value: str,
    column_index: int,
) -> Optional[int]:
    for i in range(len(df)):
        value = normalize_text(df.iat[i, column_index])
        if value == target_value:
            return i
    return None


def extract_weekly_columns(header_values: list[object], date_row_values: list[object]) -> list[WeeklyColumn]:
    weekly_columns: list[WeeklyColumn] = []

    for column_index, header_value in enumerate(header_values):
        week_index = parse_week_index(header_value)
        if week_index is None:
            continue

        week_date = parse_date(date_row_values[column_index])
        if week_date is None:
            continue

        weekly_columns.append(
            WeeklyColumn(
                column_index=column_index,
                week_index=week_index,
                year=week_date.year,
                month=week_date.month,
            )
        )

    return weekly_columns


def insert_weekly_time_values(
    session: Session,
    planning_line: PlanningLine,
    raw_row: pd.Series,
    weekly_columns: list[WeeklyColumn],
) -> None:
    for weekly_column in weekly_columns:
        value = parse_float(raw_row.iat[weekly_column.column_index])
        if value is None or value == 0:
            continue

        time_value = PlanningTimeValue(
            planning_line_id=planning_line.id,
            week_index=weekly_column.week_index,
            year=weekly_column.year,
            month=weekly_column.month,
            hours=value,
        )
        session.add(time_value)


def is_detail_row(
    report_code: Optional[str],
    responsible_code: Optional[str],
    contract_assigned: object,
    hours_per_week: object,
) -> bool:
    if report_code is not None:
        return True
    if responsible_code is not None:
        return True
    if normalize_text(contract_assigned) is not None:
        return True
    if parse_float(hours_per_week) is not None:
        return True
    return False


def get_or_create_functional_area(session: Session, name: Optional[str]) -> Optional[FunctionalArea]:
    if name is None:
        return None

    existing = session.scalar(select(FunctionalArea).where(FunctionalArea.name == name))
    if existing is not None:
        return existing

    obj = FunctionalArea(name=name)
    session.add(obj)
    session.flush()
    return obj


def get_or_create_responsible(session: Session, code: Optional[str]) -> Optional[Responsible]:
    if code is None:
        return None

    existing = session.scalar(select(Responsible).where(Responsible.code == code))
    if existing is not None:
        return existing

    obj = Responsible(code=code)
    session.add(obj)
    session.flush()
    return obj


def get_or_create_report_code(session: Session, code: str) -> ReportCode:
    existing = session.scalar(select(ReportCode).where(ReportCode.code == code))
    if existing is not None:
        return existing

    obj = ReportCode(code=code)
    session.add(obj)
    session.flush()
    return obj


def get_request_by_planning_id(session: Session, planning_id: int) -> Optional[Request]:
    return session.scalar(
        select(Request).where(Request.id == planning_id)
    )


def get_or_create_planning_item(
    session: Session,
    planning_id: int,
    functional_area: Optional[FunctionalArea],
    requested_assistance: Optional[str],
    duration: Optional[float],
    start,
    finish,
) -> PlanningItem:
    existing = session.scalar(
        select(PlanningItem).where(PlanningItem.planning_id == planning_id)
    )

    matching_request = get_request_by_planning_id(session, planning_id)

    if existing is not None:
        if existing.request_id is None and matching_request is not None:
            existing.request_id = matching_request.id

        if existing.functional_area_id is None and functional_area is not None:
            existing.functional_area_id = functional_area.id

        if existing.requested_assistance is None and requested_assistance is not None:
            existing.requested_assistance = requested_assistance

        if existing.duration is None and duration is not None:
            existing.duration = duration

        if existing.start is None and start is not None:
            existing.start = start

        if existing.finish is None and finish is not None:
            existing.finish = finish

        session.flush()
        return existing

    obj = PlanningItem(
        planning_id=planning_id,
        request_id=matching_request.id if matching_request else None,
        functional_area_id=functional_area.id if functional_area else None,
        requested_assistance=requested_assistance,
        duration=duration,
        start=start,
        finish=finish,
    )
    session.add(obj)
    session.flush()
    return obj


def normalize_planning_id(value: object) -> Optional[int]:
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


def normalize_header_value(value: object) -> Optional[str]:
    if pd.isna(value):
        return None
    return str(value).strip()


def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_header_value(col) for col in df.columns]
    return df


def normalize_text(value: object) -> Optional[str]:
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    return text


def parse_float(value: object) -> Optional[float]:
    if pd.isna(value):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        text = str(value).strip().replace(",", ".")
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None


def parse_int(value: object) -> Optional[int]:
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


def parse_date(value: object):
    if pd.isna(value):
        return None

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None

    return parsed.date()


def parse_week_index(value: object) -> Optional[int]:
    if pd.isna(value):
        return None

    if isinstance(value, (int, float)) and not pd.isna(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None

    if text.isdigit():
        return int(text)

    return None