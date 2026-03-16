from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# =========================
# Association tables
# =========================

class RequestSystem(Base):
    __tablename__ = "request_systems"

    request_id: Mapped[int] = mapped_column(
        ForeignKey("requests.id", ondelete="CASCADE"),
        primary_key=True,
    )
    system_id: Mapped[int] = mapped_column(
        ForeignKey("systems.id", ondelete="CASCADE"),
        primary_key=True,
    )


class RequestInterestGroupActivityType(Base):
    __tablename__ = "request_interest_group_activity_types"

    request_id: Mapped[int] = mapped_column(
        ForeignKey("requests.id", ondelete="CASCADE"),
        primary_key=True,
    )
    activity_type_id: Mapped[int] = mapped_column(
        ForeignKey("interest_group_activity_types.id", ondelete="CASCADE"),
        primary_key=True,
    )


# =========================
# Shared catalog tables
# =========================

class FunctionalArea(Base):
    __tablename__ = "functional_areas"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    requests: Mapped[List["Request"]] = relationship(back_populates="functional_area")
    planning_items: Mapped[List["PlanningItem"]] = relationship(back_populates="functional_area")
    planning_lines: Mapped[List["PlanningLine"]] = relationship(back_populates="functional_area")


# =========================
# Request / ST catalog tables
# =========================

class Requester(Base):
    __tablename__ = "requesters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    requests: Mapped[List["Request"]] = relationship(back_populates="requester")


class Priority(Base):
    __tablename__ = "priorities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)

    requests: Mapped[List["Request"]] = relationship(back_populates="priority")


class OutOfHoursType(Base):
    __tablename__ = "out_of_hours_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    requests: Mapped[List["Request"]] = relationship(back_populates="out_of_hours_type")


class InvestmentType(Base):
    __tablename__ = "investment_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    requests: Mapped[List["Request"]] = relationship(back_populates="investment_type")


class ApprovalStatus(Base):
    __tablename__ = "approval_statuses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(150), unique=True, nullable=False)

    requests: Mapped[List["Request"]] = relationship(
        back_populates="approval_status",
        foreign_keys="Request.approval_status_id",
    )


class RequestStatus(Base):
    __tablename__ = "request_statuses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(150), unique=True, nullable=False)

    requests: Mapped[List["Request"]] = relationship(
        back_populates="request_status",
        foreign_keys="Request.request_status_id",
    )


class ClosureResult(Base):
    __tablename__ = "closure_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)

    requests: Mapped[List["Request"]] = relationship(back_populates="closure_result")


class ServiceActivityType(Base):
    __tablename__ = "service_activity_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    requests: Mapped[List["Request"]] = relationship(back_populates="service_activity_type")


class WorkStatus(Base):
    __tablename__ = "work_statuses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)

    requests: Mapped[List["Request"]] = relationship(back_populates="work_status")


class System(Base):
    __tablename__ = "systems"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    requests: Mapped[List["Request"]] = relationship(
        secondary="request_systems",
        back_populates="systems",
    )


class InterestGroupActivityType(Base):
    __tablename__ = "interest_group_activity_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    requests: Mapped[List["Request"]] = relationship(
        secondary="request_interest_group_activity_types",
        back_populates="interest_group_activity_types",
    )


class Person(Base):
    __tablename__ = "people"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    responsible_requests: Mapped[List["Request"]] = relationship(
        back_populates="ardanuy_responsible",
        foreign_keys="Request.ardanuy_responsible_id",
    )

    modified_requests: Mapped[List["Request"]] = relationship(
        back_populates="last_modified_by",
        foreign_keys="Request.last_modified_by_id",
    )


# =========================
# Main ST / requests table
# =========================

class Request(Base):
    __tablename__ = "requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    requester_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("requesters.id"),
        nullable=True,
    )
    functional_area_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("functional_areas.id"),
        nullable=True,
    )
    priority_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("priorities.id"),
        nullable=True,
    )
    out_of_hours_type_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("out_of_hours_types.id"),
        nullable=True,
    )
    investment_type_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("investment_types.id"),
        nullable=True,
    )
    approval_status_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("approval_statuses.id"),
        nullable=True,
    )
    request_status_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("request_statuses.id"),
        nullable=True,
    )
    closure_result_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("closure_results.id"),
        nullable=True,
    )
    ardanuy_responsible_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("people.id"),
        nullable=True,
    )
    last_modified_by_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("people.id"),
        nullable=True,
    )
    service_activity_type_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("service_activity_types.id"),
        nullable=True,
    )
    work_status_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("work_statuses.id"),
        nullable=True,
    )

    request_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    requested_assistance: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    needs_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    planned_start_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    planned_end_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    days_per_week_raw: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    hours_per_day_raw: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    weeks_per_month_raw: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    out_of_hours_percentage: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    work_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    comments: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    confidential_documentation: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    sc_code: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    purchase_order: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    duration_days: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    approval_status_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    modified_request_flag: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    reevaluated_request_flag: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    survey_responded_flag: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    request_status_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    comment_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    request_status_comments: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    actual_start_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    actual_end_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    last_modified_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    work_status_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    stc_number: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    stc_approval_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    element_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    source_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    requester: Mapped[Optional[Requester]] = relationship(back_populates="requests")
    functional_area: Mapped[Optional[FunctionalArea]] = relationship(back_populates="requests")
    priority: Mapped[Optional[Priority]] = relationship(back_populates="requests")
    out_of_hours_type: Mapped[Optional[OutOfHoursType]] = relationship(back_populates="requests")
    investment_type: Mapped[Optional[InvestmentType]] = relationship(back_populates="requests")
    approval_status: Mapped[Optional[ApprovalStatus]] = relationship(
        back_populates="requests",
        foreign_keys=[approval_status_id],
    )
    request_status: Mapped[Optional[RequestStatus]] = relationship(
        back_populates="requests",
        foreign_keys=[request_status_id],
    )
    closure_result: Mapped[Optional[ClosureResult]] = relationship(back_populates="requests")
    ardanuy_responsible: Mapped[Optional[Person]] = relationship(
        back_populates="responsible_requests",
        foreign_keys=[ardanuy_responsible_id],
    )
    last_modified_by: Mapped[Optional[Person]] = relationship(
        back_populates="modified_requests",
        foreign_keys=[last_modified_by_id],
    )
    service_activity_type: Mapped[Optional[ServiceActivityType]] = relationship(back_populates="requests")
    work_status: Mapped[Optional[WorkStatus]] = relationship(back_populates="requests")

    systems: Mapped[List[System]] = relationship(
        secondary="request_systems",
        back_populates="requests",
    )

    interest_group_activity_types: Mapped[List[InterestGroupActivityType]] = relationship(
        secondary="request_interest_group_activity_types",
        back_populates="requests",
    )

    planning_items: Mapped[List["PlanningItem"]] = relationship(
        back_populates="request"
    )


# =========================
# Planning / strategic planning models
# =========================

class Responsible(Base):
    __tablename__ = "responsibles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)

    planning_lines: Mapped[List["PlanningLine"]] = relationship(back_populates="responsible")


class PlanningItem(Base):
    __tablename__ = "planning_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # ID from the planning sheets
    planning_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)

    request_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("requests.id"),
        nullable=True,
    )

    functional_area_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("functional_areas.id"),
        nullable=True,
    )

    requested_assistance: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duration: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    start: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    finish: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    request: Mapped[Optional["Request"]] = relationship(
        back_populates="planning_items"
    )

    functional_area: Mapped[Optional[FunctionalArea]] = relationship(
        back_populates="planning_items"
    )

    planning_lines: Mapped[List["PlanningLine"]] = relationship(
        back_populates="planning_item",
        cascade="all, delete-orphan",
    )


class ReportCode(Base):
    __tablename__ = "report_codes"

    code: Mapped[str] = mapped_column(String(100), primary_key=True)
    unit_of_measure: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    denomination: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    at_unit_hours: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    unit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    planning_lines: Mapped[List["PlanningLine"]] = relationship(back_populates="report_code_ref")


class PlanningLine(Base):
    __tablename__ = "planning_lines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    planning_item_id: Mapped[int] = mapped_column(
        ForeignKey("planning_items.id", ondelete="CASCADE"),
        nullable=False,
    )

    source_type: Mapped[str] = mapped_column(String(20), nullable=False)

    functional_area_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("functional_areas.id"),
        nullable=True,
    )

    requested_assistance: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    report_code: Mapped[Optional[str]] = mapped_column(
        ForeignKey("report_codes.code"),
        nullable=True,
    )

    responsible_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("responsibles.id"),
        nullable=True,
    )

    contract_assigned: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    hours_per_week: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    start: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    finish: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    excel_row_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    planning_item: Mapped["PlanningItem"] = relationship(back_populates="planning_lines")
    functional_area: Mapped[Optional[FunctionalArea]] = relationship(
        back_populates="planning_lines"
    )
    responsible: Mapped[Optional["Responsible"]] = relationship(back_populates="planning_lines")
    report_code_ref: Mapped[Optional["ReportCode"]] = relationship(back_populates="planning_lines")

    time_values: Mapped[List["PlanningTimeValue"]] = relationship(
        back_populates="planning_line",
        cascade="all, delete-orphan",
    )


class PlanningTimeValue(Base):
    __tablename__ = "planning_time_values"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    planning_line_id: Mapped[int] = mapped_column(
        ForeignKey("planning_lines.id", ondelete="CASCADE"),
        nullable=False,
    )

    week_index: Mapped[int] = mapped_column(Integer, nullable=False)

    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    month: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    hours: Mapped[float] = mapped_column(Float, nullable=False)

    planning_line: Mapped["PlanningLine"] = relationship(back_populates="time_values")
