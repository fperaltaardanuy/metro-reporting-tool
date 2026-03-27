from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from sqlalchemy import distinct, func
from sqlalchemy.orm import Session

from app.db.models import PlanningLine, PlanningTimeValue


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