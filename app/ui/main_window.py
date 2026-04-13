from __future__ import annotations

import tkinter as tk
from datetime import date
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from app.db.init_db import recreate_database
from app.db.session import SessionLocal
from app.importers.change_requests_importer import import_change_requests_excel
from app.importers.planning_importer import import_planning_excel
from app.importers.solicitudes_importer import import_solicitudes_excel
from app.services.monthly_indicator_service import MonthlyIndicatorService
from app.services.monthly_template_writer import MonthlyTemplateWriter


class MainWindow:
    MONTH_OPTIONS = [
        ("01", "Enero"),
        ("02", "Febrero"),
        ("03", "Marzo"),
        ("04", "Abril"),
        ("05", "Mayo"),
        ("06", "Junio"),
        ("07", "Julio"),
        ("08", "Agosto"),
        ("09", "Septiembre"),
        ("10", "Octubre"),
        ("11", "Noviembre"),
        ("12", "Diciembre"),
    ]
    MONTH_NAMES = [name for _, name in MONTH_OPTIONS]
    MONTH_NAME_TO_NUMBER = {name: int(code) for code, name in MONTH_OPTIONS}

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Metro Reporting Tool")
        self.root.geometry("840x620")
        self.root.minsize(780, 560)

        today = date.today()
        default_month_name = self.MONTH_OPTIONS[today.month - 1][1]

        self.requests_file_var = tk.StringVar()
        self.change_requests_file_var = tk.StringVar()
        self.planning_file_var = tk.StringVar()
        self.output_file_var = tk.StringVar()
        self.report_month_var = tk.StringVar(value=default_month_name)
        self.report_year_var = tk.StringVar(value=str(today.year))
        self.status_var = tk.StringVar(value="Selecciona los ficheros de entrada.")

        self._build_ui()

    def _build_ui(self) -> None:
        main_frame = ttk.Frame(self.root, padding=16)
        main_frame.pack(fill="both", expand=True)

        title_label = ttk.Label(
            main_frame,
            text="Metro Reporting Tool",
            font=("Segoe UI", 16, "bold"),
        )
        title_label.pack(anchor="w", pady=(0, 16))

        description_label = ttk.Label(
            main_frame,
            text=(
                "Selecciona los Excel de entrada, procesa los datos para regenerar la base de datos "
                "y después trabaja sobre el fichero Excel de salida/base para generar el reporte mensual."
            ),
            wraplength=780,
            justify="left",
        )
        description_label.pack(anchor="w", pady=(0, 20))

        files_frame = ttk.LabelFrame(main_frame, text="Ficheros de entrada", padding=12)
        files_frame.pack(fill="x", pady=(0, 16))

        ttk.Label(files_frame, text="Excel de solicitudes (ST):").grid(
            row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 8)
        )
        ttk.Entry(
            files_frame,
            textvariable=self.requests_file_var,
            width=80,
        ).grid(row=0, column=1, sticky="ew", pady=(0, 8))
        ttk.Button(
            files_frame,
            text="Seleccionar...",
            command=self._select_requests_file,
        ).grid(row=0, column=2, padx=(8, 0), pady=(0, 8))

        ttk.Label(files_frame, text="Excel de solicitudes de cambio:").grid(
            row=1, column=0, sticky="w", padx=(0, 8), pady=(0, 8)
        )
        ttk.Entry(
            files_frame,
            textvariable=self.change_requests_file_var,
            width=80,
        ).grid(row=1, column=1, sticky="ew", pady=(0, 8))
        ttk.Button(
            files_frame,
            text="Seleccionar...",
            command=self._select_change_requests_file,
        ).grid(row=1, column=2, padx=(8, 0), pady=(0, 8))

        ttk.Label(files_frame, text="Excel de planificación:").grid(
            row=2, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Entry(
            files_frame,
            textvariable=self.planning_file_var,
            width=80,
        ).grid(row=2, column=1, sticky="ew")
        ttk.Button(
            files_frame,
            text="Seleccionar...",
            command=self._select_planning_file,
        ).grid(row=2, column=2, padx=(8, 0))

        files_frame.columnconfigure(1, weight=1)

        process_frame = ttk.Frame(main_frame)
        process_frame.pack(fill="x", pady=(0, 16))

        self.process_button = ttk.Button(
            process_frame,
            text="Procesar ficheros",
            command=self._process_files,
        )
        self.process_button.pack(side="left")

        output_file_frame = ttk.LabelFrame(main_frame, text="Fichero de salida / plantilla", padding=12)
        output_file_frame.pack(fill="x", pady=(0, 16))

        ttk.Label(output_file_frame, text="Excel base de reporte:").grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Entry(
            output_file_frame,
            textvariable=self.output_file_var,
            width=80,
        ).grid(row=0, column=1, sticky="ew")
        ttk.Button(
            output_file_frame,
            text="Seleccionar...",
            command=self._select_output_file,
        ).grid(row=0, column=2, padx=(8, 0))

        output_file_frame.columnconfigure(1, weight=1)

        report_period_frame = ttk.LabelFrame(main_frame, text="Periodo de reporte", padding=12)
        report_period_frame.pack(fill="x", pady=(0, 16))

        ttk.Label(report_period_frame, text="Mes:").grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )

        month_combobox = ttk.Combobox(
            report_period_frame,
            textvariable=self.report_month_var,
            values=self.MONTH_NAMES,
            width=14,
            state="readonly",
        )
        month_combobox.grid(row=0, column=1, sticky="w", padx=(0, 20))

        ttk.Label(report_period_frame, text="Año:").grid(
            row=0, column=2, sticky="w", padx=(0, 8)
        )

        ttk.Spinbox(
            report_period_frame,
            from_=2020,
            to=2100,
            textvariable=self.report_year_var,
            width=8,
        ).grid(row=0, column=3, sticky="w")

        help_label = ttk.Label(
            report_period_frame,
            text=(
                "Este periodo se usará para identificar la columna mensual a actualizar "
                "o crear en el Excel de salida."
            ),
            wraplength=720,
            justify="left",
        )
        help_label.grid(row=1, column=0, columnspan=4, sticky="w", pady=(10, 0))

        output_actions_frame = ttk.LabelFrame(main_frame, text="Generación de reportes", padding=12)
        output_actions_frame.pack(fill="x", pady=(0, 16))

        self.generate_monthly_button = ttk.Button(
            output_actions_frame,
            text="Generar mensual",
            command=self._generate_monthly,
        )
        self.generate_monthly_button.pack(side="left")

        status_frame = ttk.LabelFrame(main_frame, text="Estado", padding=12)
        status_frame.pack(fill="both", expand=True)

        status_label = ttk.Label(
            status_frame,
            textvariable=self.status_var,
            wraplength=760,
            justify="left",
        )
        status_label.pack(anchor="w", pady=(0, 12))

        self.log_text = tk.Text(
            status_frame,
            height=14,
            wrap="word",
            state="disabled",
        )
        self.log_text.pack(fill="both", expand=True)

    def _select_requests_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Selecciona el Excel de solicitudes",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.requests_file_var.set(path)

    def _select_change_requests_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Selecciona el Excel de solicitudes de cambio",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.change_requests_file_var.set(path)

    def _select_planning_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Selecciona el Excel de planificación",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.planning_file_var.set(path)

    def _select_output_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Selecciona el Excel base de salida",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.output_file_var.set(path)

    def _process_files(self) -> None:
        requests_path = self.requests_file_var.get().strip()
        change_requests_path = self.change_requests_file_var.get().strip()
        planning_path = self.planning_file_var.get().strip()

        if not requests_path:
            messagebox.showwarning("Falta fichero", "Selecciona el Excel de solicitudes.")
            return

        if not change_requests_path:
            messagebox.showwarning(
                "Falta fichero",
                "Selecciona el Excel de solicitudes de cambio.",
            )
            return

        if not planning_path:
            messagebox.showwarning("Falta fichero", "Selecciona el Excel de planificación.")
            return

        if not Path(requests_path).exists():
            messagebox.showerror("Fichero no encontrado", f"No existe:\n{requests_path}")
            return

        if not Path(change_requests_path).exists():
            messagebox.showerror("Fichero no encontrado", f"No existe:\n{change_requests_path}")
            return

        if not Path(planning_path).exists():
            messagebox.showerror("Fichero no encontrado", f"No existe:\n{planning_path}")
            return

        self._set_busy_state(True)
        self._append_log("Regenerando base de datos...")

        session = None
        try:
            recreate_database()
            self._append_log("Base de datos recreada correctamente.")

            session = SessionLocal()

            self._append_log("Importando Excel de solicitudes...")
            import_solicitudes_excel(session, requests_path)
            self._append_log("Excel de solicitudes importado correctamente.")

            self._append_log("Importando Excel de solicitudes de cambio...")
            import_change_requests_excel(session, change_requests_path)
            self._append_log("Excel de solicitudes de cambio importado correctamente.")

            self._append_log("Importando Excel de planificación...")
            import_planning_excel(session, planning_path)
            self._append_log("Excel de planificación importado correctamente.")

            self.status_var.set("Proceso completado correctamente.")
            messagebox.showinfo("Proceso completado", "Los ficheros se han procesado correctamente.")

        except Exception as ex:
            if session is not None:
                session.rollback()

            self.status_var.set("Se ha producido un error durante el procesamiento.")
            self._append_log(f"ERROR: {ex}")
            messagebox.showerror("Error", f"Se ha producido un error:\n\n{ex}")

        finally:
            if session is not None:
                session.close()

            self._set_busy_state(False)

    def _generate_monthly(self) -> None:
        output_path = self.output_file_var.get().strip()

        if not output_path:
            messagebox.showwarning("Falta fichero", "Selecciona el Excel base de salida.")
            return

        if not Path(output_path).exists():
            messagebox.showerror("Fichero no encontrado", f"No existe:\n{output_path}")
            return

        try:
            report_month = self._get_selected_report_month()
        except ValueError as ex:
            messagebox.showerror("Periodo no válido", str(ex))
            return

        session = None

        try:
            self._set_busy_state(True)
            self.status_var.set("Generando reporte mensual...")

            self._append_log(
                f"Generando reporte mensual para {report_month.strftime('%m-%Y')}..."
            )

            session = SessionLocal()
            indicator_service = MonthlyIndicatorService(session)
            writer = MonthlyTemplateWriter()

            self._append_log("Leyendo report codes del bloque IN03 desde la plantilla...")
            report_codes = writer.get_in03_report_codes(output_path)
            self._append_log(
                "Report codes IN03 detectados: " + ", ".join(report_codes)
            )

            self._append_log("Calculando IN03-EFEC-IL...")
            in03_values = indicator_service.calculate_in03_planning_compliance_by_report_code(
                year=report_month.year,
                month=report_month.month,
                report_codes=report_codes,
            )

            self._append_log("Calculando IN17-CALS-IR...")
            in17_value = indicator_service.calculate_in17_people_in_execution(
                year=report_month.year,
                month=report_month.month,
            )
            self._append_log(f"IN17-CALS-IR = {in17_value}")

            self._append_log("Calculando IN18-CALS-IR...")
            in18_value = indicator_service.calculate_in18_profiles_in_execution(
                year=report_month.year,
                month=report_month.month,
            )
            self._append_log(f"IN18-CALS-IR = {in18_value}")

            self._append_log("Calculando IN19-CALS-IA...")
            in19_value = indicator_service.calculate_in19_fte_in_execution(
                year=report_month.year,
                month=report_month.month,
            )
            self._append_log(f"IN19-CALS-IA = {in19_value}")

            self._append_log("Calculando IN28-EFIC-IA...")
            in28_value = indicator_service.calculate_in28_service_director_dedication(
                year=report_month.year,
                month=report_month.month,
            )
            self._append_log(f"IN28-EFIC-IA = {in28_value}")

            indicator_values = {
                "IN17-CALS-IR": in17_value,
                "IN18-CALS-IR": in18_value,
                "IN19-CALS-IA": in19_value,
                "IN21-EFIC-IA": indicator_service.calculate_in21_open_requests(),
                "IN25-EFIC-IP": indicator_service.calculate_in25_cancelled_requests(),
                "IN28-EFIC-IA": in28_value,
            }

            self._append_log("Escribiendo resultados en la plantilla Excel...")
            result = writer.write_monthly_report(
                workbook_path=output_path,
                report_month=report_month,
                indicator_values=indicator_values,
                in03_values=in03_values,
            )

            self.status_var.set("Reporte mensual generado correctamente.")
            self._append_log(
                f"Reporte mensual generado correctamente en la hoja '{result.sheet_name}', "
                f"columna {result.month_column} ({result.month_label})."
            )

            if result.written_indicator_ids:
                self._append_log(
                    "Indicadores escritos: " + ", ".join(result.written_indicator_ids)
                )

            if result.written_report_codes:
                self._append_log(
                    "Report codes escritos en IN03: " + ", ".join(result.written_report_codes)
                )

            if result.missing_indicator_ids:
                self._append_log(
                    "Indicadores no encontrados en plantilla: "
                    + ", ".join(result.missing_indicator_ids)
                )

            if result.missing_report_codes:
                self._append_log(
                    "Report codes del bloque IN03 sin valor calculado: "
                    + ", ".join(result.missing_report_codes)
                )

            messagebox.showinfo(
                "Reporte mensual generado",
                (
                    "El reporte mensual se ha generado correctamente.\n\n"
                    f"Periodo: {result.month_label}\n"
                    f"Hoja: {result.sheet_name}"
                ),
            )

        except Exception as ex:
            self.status_var.set("Se ha producido un error durante la generación mensual.")
            self._append_log(f"ERROR: {ex}")
            messagebox.showerror("Error", f"Se ha producido un error:\n\n{ex}")

        finally:
            if session is not None:
                session.close()

            self._set_busy_state(False)

    def _get_selected_report_month(self) -> date:
        month_name = self.report_month_var.get().strip()
        year_text = self.report_year_var.get().strip()

        if not month_name:
            raise ValueError("Selecciona un mes de reporte.")

        if month_name not in self.MONTH_NAME_TO_NUMBER:
            raise ValueError("El mes de reporte no es válido.")

        if not year_text:
            raise ValueError("Selecciona un año de reporte.")

        try:
            year = int(year_text)
        except ValueError as ex:
            raise ValueError("El año de reporte no es válido.") from ex

        if year < 2000 or year > 2100:
            raise ValueError("El año de reporte debe estar entre 2000 y 2100.")

        month = self.MONTH_NAME_TO_NUMBER[month_name]
        return date(year, month, 1)

    def _set_busy_state(self, is_busy: bool) -> None:
        if is_busy:
            self.process_button.config(state="disabled")
            self.generate_monthly_button.config(state="disabled")
            self.status_var.set("Procesando ficheros...")
            self.root.config(cursor="watch")
        else:
            self.process_button.config(state="normal")
            self.generate_monthly_button.config(state="normal")
            self.root.config(cursor="")

        self.root.update_idletasks()

    def _append_log(self, message: str) -> None:
        self.log_text.config(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.config(state="disabled")
        self.root.update_idletasks()