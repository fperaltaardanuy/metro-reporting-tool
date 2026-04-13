from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from sqlalchemy import distinct, func, or_
from sqlalchemy.orm import Session

from app.db.models import PlanningLine, PlanningTimeValue, Request, RequestStatus, ReportCode


@dataclass(frozen=True)
class ReportCodeComplianceValue:
    report_code: str
    estimated_hours: float
    real_hours: float
    value: float | str


class MonthlyIndicatorService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def calculate_in17_people_in_execution(
        self,
        year: int,
        month: int,
    ) -> int:
        """
        IN17-CALS-IR
        Count distinct responsible_id values in real planning lines
        that have time values in the selected month.
        """
        result = (
            self.session.query(func.count(distinct(PlanningLine.responsible_id)))
            .join(
                PlanningTimeValue,
                PlanningTimeValue.planning_line_id == PlanningLine.id,
            )
            .filter(PlanningLine.source_type == "real")
            .filter(PlanningLine.responsible_id.isnot(None))
            .filter(PlanningTimeValue.year == year)
            .filter(PlanningTimeValue.month == month)
            .scalar()
        )

        return int(result or 0)

    def calculate_in18_profiles_in_execution(
        self,
        year: int,
        month: int,
    ) -> int:
        """
        IN18-CALS-IR
        Count distinct report_code values in real planning lines
        that have time values in the selected month.
        """
        result = (
            self.session.query(func.count(distinct(PlanningLine.report_code)))
            .join(
                PlanningTimeValue,
                PlanningTimeValue.planning_line_id == PlanningLine.id,
            )
            .filter(PlanningLine.source_type == "real")
            .filter(PlanningLine.report_code.isnot(None))
            .filter(PlanningLine.report_code != "")
            .filter(PlanningTimeValue.year == year)
            .filter(PlanningTimeValue.month == month)
            .scalar()
        )

        return int(result or 0)

    def calculate_in03_planning_compliance_by_report_code(
        self,
        year: int,
        month: int,
        report_codes: Sequence[str],
    ) -> list[ReportCodeComplianceValue]:
        """
        IN03-EFEC-IL

        For each report_code:
        - if estimated == 0 and real == 0 -> "-"
        - if estimated == 0 and real > 0 -> "error"
        - if estimated > 0 -> (real / estimated) * 100
        """
        normalized_report_codes = [
            report_code.strip()
            for report_code in report_codes
            if report_code is not None and report_code.strip()
        ]

        if not normalized_report_codes:
            return []

        estimated_hours_by_code = self._get_hours_by_report_code(
            year=year,
            month=month,
            source_type="estimated",
            report_codes=normalized_report_codes,
        )

        real_hours_by_code = self._get_hours_by_report_code(
            year=year,
            month=month,
            source_type="real",
            report_codes=normalized_report_codes,
        )

        results: list[ReportCodeComplianceValue] = []

        for report_code in normalized_report_codes:
            estimated_hours = float(estimated_hours_by_code.get(report_code, 0.0) or 0.0)
            real_hours = float(real_hours_by_code.get(report_code, 0.0) or 0.0)

            if estimated_hours == 0 and real_hours == 0:
                value: float | str = "-"
            elif estimated_hours == 0 and real_hours > 0:
                value = "error"
            else:
                value = (real_hours / estimated_hours) * 100

            results.append(
                ReportCodeComplianceValue(
                    report_code=report_code,
                    estimated_hours=estimated_hours,
                    real_hours=real_hours,
                    value=value,
                )
            )

        return results

    def _get_hours_by_report_code(
        self,
        year: int,
        month: int,
        source_type: str,
        report_codes: Sequence[str],
    ) -> dict[str, float]:
        rows = (
            self.session.query(
                PlanningLine.report_code,
                func.sum(PlanningTimeValue.hours),
            )
            .join(
                PlanningTimeValue,
                PlanningTimeValue.planning_line_id == PlanningLine.id,
            )
            .filter(PlanningLine.source_type == source_type)
            .filter(PlanningLine.report_code.isnot(None))
            .filter(PlanningLine.report_code != "")
            .filter(PlanningLine.report_code.in_(report_codes))
            .filter(PlanningTimeValue.year == year)
            .filter(PlanningTimeValue.month == month)
            .group_by(PlanningLine.report_code)
            .all()
        )

        return {
            str(report_code): float(total_hours or 0.0)
            for report_code, total_hours in rows
            if report_code is not None
        }
    from sqlalchemy import or_

    def calculate_in19_fte_in_execution(
        self,
        year: int,
        month: int,
    ) -> float:
        """
        IN19-CALS-IA

        FTEs in execution for the selected month:
        real hours excluding report code IDATGENGES01,
        divided by the contractual monthly FTE hours.
        """
        monthly_fte_hours = (1792 / 12) * 7

        total_hours = (
            self.session.query(func.sum(PlanningTimeValue.hours))
            .select_from(PlanningLine)
            .join(
                PlanningTimeValue,
                PlanningTimeValue.planning_line_id == PlanningLine.id,
            )
            .filter(PlanningLine.source_type == "real")
            .filter(
                or_(
                    PlanningLine.report_code.is_(None),
                    PlanningLine.report_code != "IDATGENGES01",
                )
            )
            .filter(PlanningTimeValue.year == year)
            .filter(PlanningTimeValue.month == month)
            .scalar()
        )

        net_hours = float(total_hours or 0.0)
        return net_hours / monthly_fte_hours

    def calculate_in21_open_requests(self) -> int:
        """
        IN21-EFIC-IA

        Count requests whose status is exactly 'En Curso'.
        No month filter is applied.
        """
        result = (
            self.session.query(func.count(Request.id))
            .join(
                RequestStatus,
                Request.request_status_id == RequestStatus.id,
            )
            .filter(RequestStatus.name == "En Curso")
            .scalar()
        )

        return int(result or 0)


    def calculate_in25_cancelled_requests(self) -> int:
        """
        IN25-EFIC-IP

        Count requests whose status is exactly 'Cancelada'.
        No month filter is applied.
        """
        result = (
            self.session.query(func.count(Request.id))
            .join(
                RequestStatus,
                Request.request_status_id == RequestStatus.id,
            )
            .filter(RequestStatus.name == "Cancelada")
            .scalar()
        )

        return int(result or 0)

    def calculate_in28_service_director_dedication(
        self,
        year: int,
        month: int,
    ) -> float | str:
        """
        IN28-EFIC-IA

        Dedication percentage of the Service Director for the selected month:
        real hours for report code IDATGENGES01 divided by its contractual at_unit_hours.
        """
        report_code = "IDATGENGES01"

        total_hours = (
            self.session.query(func.sum(PlanningTimeValue.hours))
            .select_from(PlanningLine)
            .join(
                PlanningTimeValue,
                PlanningTimeValue.planning_line_id == PlanningLine.id,
            )
            .filter(PlanningLine.source_type == "real")
            .filter(PlanningLine.report_code == report_code)
            .filter(PlanningTimeValue.year == year)
            .filter(PlanningTimeValue.month == month)
            .scalar()
        )

        at_unit_hours = (
            self.session.query(ReportCode.at_unit_hours)
            .filter(ReportCode.code == report_code)
            .scalar()
        )

        real_hours = float(total_hours or 0.0)

        if at_unit_hours is None or float(at_unit_hours) == 0:
            return "error"

        return (real_hours / float(at_unit_hours)) * 100