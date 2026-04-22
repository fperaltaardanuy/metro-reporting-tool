from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from sqlalchemy import distinct, func, case, or_
from sqlalchemy.orm import Session

from app.db.models import PlanningLine, PlanningTimeValue, Request, RequestStatus, ReportCode, ChangeRequest, WorkStatus, ApprovalStatus
from datetime import date
import calendar

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
        that have time values in the selected month, applying these rules:

        1. IDATGENAGN02 and IDATGENAGD01 are considered the same profile.
        2. IDATGENGES01 is excluded from the metric.
        """
        normalized_report_code = case(
            (
                PlanningLine.report_code.in_(["IDATGENAGN02", "IDATGENAGD01"]),
                "IDATGENAGN02",
            ),
            else_=PlanningLine.report_code,
        )

        result = (
            self.session.query(func.count(distinct(normalized_report_code)))
            .select_from(PlanningLine)
            .join(
                PlanningTimeValue,
                PlanningTimeValue.planning_line_id == PlanningLine.id,
            )
            .filter(PlanningLine.source_type == "real")
            .filter(PlanningLine.report_code.isnot(None))
            .filter(PlanningLine.report_code != "")
            .filter(PlanningLine.report_code != "IDATGENGES01")
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

        Count requests considered open with this business rule:
        - approval_status.name == 'MdM Aprobada'
        - request_status.name is not 'Cancelada', 'Cerrada' or 'Rechazada'
        - NULL request_status is accepted as open
        """
        result = (
            self.session.query(func.count(Request.id))
            .join(
                ApprovalStatus,
                Request.approval_status_id == ApprovalStatus.id,
            )
            .outerjoin(
                RequestStatus,
                Request.request_status_id == RequestStatus.id,
            )
            .filter(ApprovalStatus.name == "MdM Aprobada")
            .filter(
                or_(
                    RequestStatus.name.is_(None),
                    ~RequestStatus.name.in_(["Cancelada", "Cerrada", "Rechazada"]),
                )
            )
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
    
    def calculate_in20_new_requests(
        self,
        year: int,
        month: int,
    ) -> int:
        """
        IN20-EFIC-II

        Count requests whose request_date falls within the selected month.
        """
        result = (
            self.session.query(func.count(Request.id))
            .filter(Request.request_date.isnot(None))
            .filter(func.strftime("%Y", Request.request_date) == str(year))
            .filter(func.strftime("%m", Request.request_date) == f"{month:02d}")
            .scalar()
        )

        return int(result or 0)


    def calculate_in23_closed_requests(
        self,
        year: int,
        month: int,
    ) -> int:
        """
        IN23-EFIC-IA

        Count requests whose current status is 'Cerrada' and whose
        request_status_date falls within the selected month.
        """
        result = (
            self.session.query(func.count(Request.id))
            .join(
                RequestStatus,
                Request.request_status_id == RequestStatus.id,
            )
            .filter(RequestStatus.name == "Cerrada")
            .filter(Request.request_status_date.isnot(None))
            .filter(func.strftime("%Y", Request.request_status_date) == str(year))
            .filter(func.strftime("%m", Request.request_status_date) == f"{month:02d}")
            .scalar()
        )

        return int(result or 0)


    def calculate_in26_requests_in_progress_percentage(self) -> float | str:
        """
        IN26-EFIC-IR

        Percentage of requests currently in status 'En Curso'
        relative to open requests (IN21-EFIC-IA).

        Returned in 0..100 scale.
        """
        numerator = (
            self.session.query(func.count(Request.id))
            .join(
                RequestStatus,
                Request.request_status_id == RequestStatus.id,
            )
            .filter(RequestStatus.name == "En Curso")
            .scalar()
        )

        denominator = self.calculate_in21_open_requests()

        numerator_value = int(numerator or 0)

        if denominator == 0:
            if numerator_value == 0:
                return "-"
            return "error"

        return (numerator_value / denominator) * 100
    
    def calculate_in22_delivered_requests(
        self,
        year: int,
        month: int,
    ) -> int:
        """
        IN22-EFIC-IA

        Count requests whose work status is 'Entregado' and whose
        work_status_date falls within the selected month.
        """
        result = (
            self.session.query(func.count(Request.id))
            .join(
                WorkStatus,
                Request.work_status_id == WorkStatus.id,
            )
            .filter(WorkStatus.name == "Entregado")
            .filter(Request.work_status_date.isnot(None))
            .filter(func.strftime("%Y", Request.work_status_date) == str(year))
            .filter(func.strftime("%m", Request.work_status_date) == f"{month:02d}")
            .scalar()
        )

        return int(result or 0)


    def calculate_in27_delivered_requests_percentage(
        self,
        year: int,
        month: int,
    ) -> float | str:
        """
        IN27-EFIC-II

        Percentage of delivered requests in the selected month
        relative to open requests (IN21-EFIC-IA).

        Returned in 0..100 scale.
        """
        numerator = self.calculate_in22_delivered_requests(year, month)
        denominator = self.calculate_in21_open_requests()

        if denominator == 0:
            if numerator == 0:
                return "-"
            return "error"

        return (numerator / denominator) * 100


    def calculate_in24_change_requests_total(
        self,
        year: int,
        month: int,
    ) -> int:
        """
        IN24-EFIC-IA

        Count change requests whose request_date is between
        the start of the selected year and the end of the selected month.
        """
        start_of_year = date(year, 1, 1)
        last_day = calendar.monthrange(year, month)[1]
        month_end = date(year, month, last_day)

        result = (
            self.session.query(func.count(ChangeRequest.id))
            .filter(ChangeRequest.request_date.isnot(None))
            .filter(ChangeRequest.request_date >= start_of_year)
            .filter(ChangeRequest.request_date <= month_end)
            .scalar()
        )

        return int(result or 0)
    
    def calculate_in08_change_requests_triggering_new_request_percentage(
        self,
        year: int,
        month: int,
    ) -> float | str:
        """
        IN08-EFEC-IP

        Percentage of change requests in the selected YTD period
        that trigger a new work request.

        Period:
        from January 1st of the selected year
        to the last day of the selected month.

        Returned in 0..100 scale.
        """
        start_of_year = date(year, 1, 1)
        last_day = calendar.monthrange(year, month)[1]
        month_end = date(year, month, last_day)

        total_change_requests = (
            self.session.query(func.count(ChangeRequest.id))
            .filter(ChangeRequest.request_date.isnot(None))
            .filter(ChangeRequest.request_date >= start_of_year)
            .filter(ChangeRequest.request_date <= month_end)
            .scalar()
        )

        created_work_request_count = (
            self.session.query(func.count(ChangeRequest.id))
            .filter(ChangeRequest.request_date.isnot(None))
            .filter(ChangeRequest.request_date >= start_of_year)
            .filter(ChangeRequest.request_date <= month_end)
            .filter(ChangeRequest.created_work_request_flag.is_(True))
            .scalar()
        )

        denominator = int(total_change_requests or 0)
        numerator = int(created_work_request_count or 0)

        if denominator == 0:
            if numerator == 0:
                return "-"
            return "error"

        return (numerator / denominator) * 100
    
    def calculate_in07_modified_requests_percentage(
        self,
        year: int,
        month: int,
    ) -> float | str:
        """
        IN07-EFEC-IP

        Percentage of change requests in the selected YTD period
        with modified_work_request_flag = True, relative to:

        - requests closed in the selected YTD period
        - plus requests currently in progress whose request_status_date
        is less than or equal to the end of the selected month

        Returned in 0..100 scale.
        """
        start_of_year = date(year, 1, 1)
        last_day = calendar.monthrange(year, month)[1]
        month_end = date(year, month, last_day)

        modified_change_requests = (
            self.session.query(func.count(ChangeRequest.id))
            .filter(ChangeRequest.request_date.isnot(None))
            .filter(ChangeRequest.request_date >= start_of_year)
            .filter(ChangeRequest.request_date <= month_end)
            .filter(ChangeRequest.modified_work_request_flag.is_(True))
            .scalar()
        )

        closed_requests_ytd = (
            self.session.query(func.count(Request.id))
            .join(
                RequestStatus,
                Request.request_status_id == RequestStatus.id,
            )
            .filter(RequestStatus.name == "Cerrada")
            .filter(Request.request_status_date.isnot(None))
            .filter(Request.request_status_date >= start_of_year)
            .filter(Request.request_status_date <= month_end)
            .scalar()
        )

        in_progress_requests = (
            self.session.query(func.count(Request.id))
            .join(
                RequestStatus,
                Request.request_status_id == RequestStatus.id,
            )
            .filter(RequestStatus.name == "En Curso")
            .filter(Request.request_status_date.isnot(None))
            .filter(Request.request_status_date <= month_end)
            .scalar()
        )

        numerator = int(modified_change_requests or 0)
        denominator = int(closed_requests_ytd or 0) + int(in_progress_requests or 0)

        if denominator == 0:
            if numerator == 0:
                return "-"
            return "error"

        return (numerator / denominator) * 100
    
    def calculate_in02_budget_compliance_percentage(
        self,
        year: int,
        month: int,
    ) -> float | str:
        """
        IN02-EFEC-IL

        Percentage of actual monthly cost relative to estimated monthly cost,
        excluding report code IDATGENGES01.

        Returned in 0..100 scale.
        """
        estimated_cost = self._calculate_monthly_planning_cost(
            year=year,
            month=month,
            source_type="estimated",
        )

        if estimated_cost == "error":
            return "error"

        real_cost = self._calculate_monthly_planning_cost(
            year=year,
            month=month,
            source_type="real",
        )

        if real_cost == "error":
            return "error"

        estimated_cost_value = float(estimated_cost or 0.0)
        real_cost_value = float(real_cost or 0.0)

        if estimated_cost_value == 0:
            if real_cost_value == 0:
                return "-"
            return "error"

        return (real_cost_value / estimated_cost_value) * 100


    def _calculate_monthly_planning_cost(
        self,
        year: int,
        month: int,
        source_type: str,
    ) -> float | str:
        rows = (
            self.session.query(
                PlanningLine.report_code,
                func.sum(PlanningTimeValue.hours),
                ReportCode.unit_price,
                ReportCode.at_unit_hours,
            )
            .select_from(PlanningLine)
            .join(
                PlanningTimeValue,
                PlanningTimeValue.planning_line_id == PlanningLine.id,
            )
            .join(
                ReportCode,
                ReportCode.code == PlanningLine.report_code,
            )
            .filter(PlanningLine.source_type == source_type)
            .filter(PlanningLine.report_code.isnot(None))
            .filter(PlanningLine.report_code != "")
            .filter(PlanningLine.report_code != "IDATGENGES01")
            .filter(PlanningTimeValue.year == year)
            .filter(PlanningTimeValue.month == month)
            .group_by(
                PlanningLine.report_code,
                ReportCode.unit_price,
                ReportCode.at_unit_hours,
            )
            .all()
        )

        total_cost = 0.0

        for report_code, total_hours, unit_price, at_unit_hours in rows:
            hours = float(total_hours or 0.0)

            if hours == 0:
                continue

            if unit_price is None or at_unit_hours is None or float(at_unit_hours) == 0:
                return "error"

            hourly_cost = float(unit_price) / float(at_unit_hours)
            total_cost += hours * hourly_cost

        return total_cost
