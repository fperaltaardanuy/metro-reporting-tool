from __future__ import annotations

import json
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
    CONFIG_PATH = Path(__file__).resolve().parents[2] / "app_config.json"
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
        self.root.geometry("1040x760")
        self.root.minsize(900, 680)

        self.config = self._load_config()
        self.show_developer_actions = bool(
            self.config.get("show_developer_actions", False)
        )

        today = date.today()
        default_month_name = self.MONTH_OPTIONS[today.month - 1][1]

        self.file_status_vars: list[tk.StringVar] = []
        self.file_buttons: list[ttk.Button] = []
        self.action_buttons: list[ttk.Button] = []

        self.requests_file_var = tk.StringVar()
        self.change_requests_file_var = tk.StringVar()
        self.planning_file_var = tk.StringVar()
        self.output_file_var = tk.StringVar()
        self.report_month_var = tk.StringVar(value=default_month_name)
        self.report_year_var = tk.StringVar(value=str(today.year))
        self.status_var = tk.StringVar(
            value="Preparado. Selecciona los ficheros de entrada."
        )

        self._build_ui()

    def _build_ui(self) -> None:
        self._configure_styles()

        main_frame = ttk.Frame(self.root, style="App.TFrame", padding=20)
        main_frame.pack(fill="both", expand=True)

        header_frame = ttk.Frame(main_frame, style="App.TFrame")
        header_frame.pack(fill="x", pady=(0, 18))
        header_frame.columnconfigure(0, weight=1)

        title_label = ttk.Label(
            header_frame,
            text="Metro Reporting Tool",
            style="Title.TLabel",
        )
        title_label.grid(row=0, column=0, sticky="w")

        description_label = ttk.Label(
            header_frame,
            text=(
                "Importa los Excel de entrada, actualiza la base SQLite y genera los indicadores "
                "mensuales sobre la plantilla de salida."
            ),
            style="Subtitle.TLabel",
        )
        description_label.grid(row=1, column=0, sticky="w", pady=(6, 0))

        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill="both", expand=True)

        data_tab = ttk.Frame(notebook, style="App.TFrame", padding=(0, 14, 0, 0))
        charts_tab = ttk.Frame(notebook, style="App.TFrame", padding=(0, 14, 0, 0))
        notebook.add(data_tab, text="Datos y reportes")
        notebook.add(charts_tab, text="Soporte para gráficas")

        content_frame = ttk.Frame(data_tab, style="App.TFrame")
        content_frame.pack(fill="both", expand=True)
        content_frame.columnconfigure(0, weight=3, uniform="content")
        content_frame.columnconfigure(1, weight=2, uniform="content")
        content_frame.rowconfigure(0, weight=1)

        workflow_frame = ttk.Frame(content_frame, style="App.TFrame")
        workflow_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 14))
        workflow_frame.columnconfigure(0, weight=1)

        status_frame = ttk.Frame(content_frame, style="App.TFrame")
        status_frame.grid(row=0, column=1, sticky="nsew")
        status_frame.columnconfigure(0, weight=1)
        status_frame.rowconfigure(1, weight=1)

        files_frame = ttk.LabelFrame(
            workflow_frame,
            text="1. Ficheros de entrada",
            padding=(16, 12, 16, 16),
        )
        files_frame.grid(row=0, column=0, sticky="ew", pady=(0, 14))
        files_frame.columnconfigure(1, weight=1)

        self._add_file_picker_row(
            parent=files_frame,
            row=0,
            title="Solicitudes ST",
            description="Excel principal de solicitudes.",
            variable=self.requests_file_var,
            command=self._select_requests_file,
        )
        self._add_file_picker_row(
            parent=files_frame,
            row=1,
            title="Solicitudes de cambio",
            description="Excel con cambios asociados a solicitudes.",
            variable=self.change_requests_file_var,
            command=self._select_change_requests_file,
        )
        self._add_file_picker_row(
            parent=files_frame,
            row=2,
            title="Planificación",
            description="Excel de planificación y recursos.",
            variable=self.planning_file_var,
            command=self._select_planning_file,
        )

        process_frame = ttk.Frame(workflow_frame, style="Card.TFrame", padding=16)
        process_frame.grid(row=1, column=0, sticky="ew", pady=(0, 14))
        process_frame.columnconfigure(0, weight=1)

        process_text_frame = ttk.Frame(process_frame, style="Card.TFrame")
        process_text_frame.grid(row=0, column=0, sticky="ew", padx=(0, 12))
        ttk.Label(
            process_text_frame,
            text="Actualizar base de datos",
            style="SectionTitle.TLabel",
        ).pack(anchor="w")
        ttk.Label(
            process_text_frame,
            text="Recrea metro_requests.db e importa los tres Excel seleccionados.",
            style="Hint.TLabel",
        ).pack(anchor="w", pady=(4, 0))

        self.process_button = ttk.Button(
            process_frame,
            text="Procesar ficheros",
            command=self._process_files,
            style="Accent.TButton",
        )
        self.process_button.grid(row=0, column=1, sticky="e")
        self.action_buttons.append(self.process_button)

        output_file_frame = ttk.LabelFrame(
            workflow_frame,
            text="2. Plantilla de salida",
            padding=(16, 12, 16, 16),
        )
        output_file_frame.grid(row=2, column=0, sticky="ew", pady=(0, 14))
        output_file_frame.columnconfigure(1, weight=1)

        self._add_file_picker_row(
            parent=output_file_frame,
            row=0,
            title="Excel base de reporte",
            description="Plantilla que se actualizará con los indicadores calculados.",
            variable=self.output_file_var,
            command=self._select_output_file,
        )

        report_frame = ttk.LabelFrame(
            workflow_frame,
            text="3. Periodo y generación",
            padding=(16, 12, 16, 16),
        )
        report_frame.grid(row=3, column=0, sticky="ew")
        report_frame.columnconfigure(0, weight=1)

        period_controls_frame = ttk.Frame(report_frame)
        period_controls_frame.grid(row=0, column=0, sticky="ew", pady=(0, 14))
        period_controls_frame.columnconfigure(4, weight=1)

        ttk.Label(period_controls_frame, text="Mes", style="FieldLabel.TLabel").grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )

        month_combobox = ttk.Combobox(
            period_controls_frame,
            textvariable=self.report_month_var,
            values=self.MONTH_NAMES,
            width=16,
            state="readonly",
        )
        month_combobox.grid(row=0, column=1, sticky="w", padx=(0, 20))

        ttk.Label(period_controls_frame, text="Año", style="FieldLabel.TLabel").grid(
            row=0, column=2, sticky="w", padx=(0, 8)
        )

        ttk.Spinbox(
            period_controls_frame,
            from_=2020,
            to=2100,
            textvariable=self.report_year_var,
            width=8,
        ).grid(row=0, column=3, sticky="w")

        output_actions_frame = ttk.Frame(report_frame)
        output_actions_frame.grid(row=1, column=0, sticky="ew")
        output_actions_frame.columnconfigure(2, weight=1)

        self.generate_monthly_button = ttk.Button(
            output_actions_frame,
            text="Generar mensual",
            command=self._generate_monthly,
            style="Accent.TButton",
        )
        self.generate_monthly_button.grid(row=0, column=0, sticky="w")
        self.action_buttons.append(self.generate_monthly_button)

        if self.show_developer_actions:
            self.generate_all_monthly_button = ttk.Button(
                output_actions_frame,
                text="Dev: generar mensuales 2024→mes",
                command=self._generate_monthly_range_from_2024,
            )
            self.generate_all_monthly_button.grid(
                row=0, column=1, sticky="w", padx=(8, 0)
            )
            self.action_buttons.append(self.generate_all_monthly_button)

        status_card = ttk.LabelFrame(
            status_frame, text="Estado", padding=(16, 12, 16, 16)
        )
        status_card.grid(row=0, column=0, sticky="ew", pady=(0, 14))
        status_card.columnconfigure(0, weight=1)

        status_label = ttk.Label(
            status_card,
            textvariable=self.status_var,
            wraplength=360,
            justify="left",
            style="Status.TLabel",
        )
        status_label.grid(row=0, column=0, sticky="ew")

        self.progress_bar = ttk.Progressbar(
            status_card,
            mode="indeterminate",
            length=240,
        )
        self.progress_bar.grid(row=1, column=0, sticky="ew", pady=(12, 0))

        log_frame = ttk.LabelFrame(
            status_frame, text="Registro", padding=(12, 10, 12, 12)
        )
        log_frame.grid(row=1, column=0, sticky="nsew")
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(
            log_frame,
            height=18,
            wrap="word",
            state="disabled",
            background="#fbfdff",
            foreground="#172033",
            borderwidth=0,
            padx=10,
            pady=10,
            font=("Consolas", 9),
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")

        log_scrollbar = ttk.Scrollbar(
            log_frame,
            orient="vertical",
            command=self.log_text.yview,
        )
        log_scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scrollbar.set)

        clear_log_button = ttk.Button(
            log_frame,
            text="Limpiar registro",
            command=self._clear_log,
        )
        clear_log_button.grid(row=1, column=0, sticky="w", pady=(10, 0))

        for variable in (
            self.requests_file_var,
            self.change_requests_file_var,
            self.planning_file_var,
            self.output_file_var,
        ):
            variable.trace_add("write", self._refresh_file_statuses)

        self._refresh_file_statuses()
        self._build_charts_tab(charts_tab)

    def _configure_styles(self) -> None:
        style = ttk.Style(self.root)

        if "clam" in style.theme_names():
            style.theme_use("clam")

        self.root.configure(background="#f5f7fb")

        style.configure("TFrame", background="#ffffff")
        style.configure("App.TFrame", background="#f5f7fb")
        style.configure("Card.TFrame", background="#ffffff", relief="flat")
        style.configure(
            "Title.TLabel",
            background="#f5f7fb",
            foreground="#172033",
            font=("Segoe UI", 20, "bold"),
        )
        style.configure(
            "Subtitle.TLabel",
            background="#f5f7fb",
            foreground="#5c667a",
            font=("Segoe UI", 10),
        )
        style.configure(
            "SectionTitle.TLabel",
            background="#ffffff",
            foreground="#172033",
            font=("Segoe UI", 11, "bold"),
        )
        style.configure(
            "FieldLabel.TLabel",
            background="#ffffff",
            foreground="#334155",
            font=("Segoe UI", 9, "bold"),
        )
        style.configure(
            "Hint.TLabel",
            background="#ffffff",
            foreground="#64748b",
            font=("Segoe UI", 9),
        )
        style.configure(
            "Status.TLabel",
            background="#ffffff",
            foreground="#172033",
            font=("Segoe UI", 10, "bold"),
        )
        style.configure(
            "Badge.TLabel",
            background="#ffffff",
            foreground="#475569",
            font=("Segoe UI", 9),
        )
        style.configure(
            "TLabelframe", background="#ffffff", bordercolor="#d7dee8", relief="solid"
        )
        style.configure(
            "TLabelframe.Label", foreground="#172033", font=("Segoe UI", 10, "bold")
        )
        style.configure("TEntry", padding=6)
        style.configure("TButton", padding=(12, 7))
        style.configure("Accent.TButton", padding=(14, 8), font=("Segoe UI", 9, "bold"))

    def _build_charts_tab(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)

        placeholder_frame = ttk.LabelFrame(
            parent,
            text="Soporte para gráficas",
            padding=(18, 16, 18, 18),
        )
        placeholder_frame.grid(row=0, column=0, sticky="new")
        placeholder_frame.columnconfigure(0, weight=1)

        ttk.Label(
            placeholder_frame,
            text="Generación de datos para gráficas",
            style="SectionTitle.TLabel",
        ).grid(row=0, column=0, sticky="w")

        ttk.Label(
            placeholder_frame,
            text=(
                "Este apartado queda reservado para exportar datasets preparados "
                "que después se usarán en herramientas específicas de visualización."
            ),
            style="Hint.TLabel",
            wraplength=760,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(8, 0))

        ttk.Label(
            placeholder_frame,
            text=(
                "Próximos usos previstos: CSVs por indicador y periodo, agregados "
                "desde SQLite y ficheros compatibles con PowerBI."
            ),
            style="Hint.TLabel",
            wraplength=760,
            justify="left",
        ).grid(row=2, column=0, sticky="w", pady=(6, 0))

    def _load_config(self) -> dict[str, object]:
        if not self.CONFIG_PATH.exists():
            return {}

        try:
            with self.CONFIG_PATH.open(encoding="utf-8") as config_file:
                config = json.load(config_file)
        except json.JSONDecodeError as ex:
            messagebox.showwarning(
                "Configuración no válida",
                (
                    f"No se ha podido leer {self.CONFIG_PATH.name}. "
                    f"Se usará la configuración por defecto.\n\n{ex}"
                ),
            )
            return {}

        if not isinstance(config, dict):
            messagebox.showwarning(
                "Configuración no válida",
                (
                    f"{self.CONFIG_PATH.name} debe contener un objeto JSON. "
                    "Se usará la configuración por defecto."
                ),
            )
            return {}

        return config

    def _add_file_picker_row(
        self,
        parent: ttk.LabelFrame,
        row: int,
        title: str,
        description: str,
        variable: tk.StringVar,
        command,
    ) -> None:
        text_frame = ttk.Frame(parent)
        text_frame.grid(row=row, column=0, sticky="nw", padx=(0, 12), pady=(8, 8))

        ttk.Label(text_frame, text=title, style="FieldLabel.TLabel").pack(anchor="w")
        ttk.Label(
            text_frame, text=description, style="Hint.TLabel", wraplength=180
        ).pack(anchor="w", pady=(3, 0))

        entry = ttk.Entry(parent, textvariable=variable)
        entry.grid(row=row, column=1, sticky="ew", pady=(8, 8))

        button = ttk.Button(parent, text="Seleccionar", command=command)
        button.grid(row=row, column=2, sticky="e", padx=(10, 0), pady=(8, 8))
        self.file_buttons.append(button)

        status_var = tk.StringVar(value="Pendiente")
        ttk.Label(parent, textvariable=status_var, style="Badge.TLabel").grid(
            row=row,
            column=3,
            sticky="e",
            padx=(10, 0),
            pady=(8, 8),
        )
        self.file_status_vars.append(status_var)

    def _refresh_file_statuses(self, *_args) -> None:
        variables = (
            self.requests_file_var,
            self.change_requests_file_var,
            self.planning_file_var,
            self.output_file_var,
        )

        for variable, status_var in zip(variables, self.file_status_vars):
            path = variable.get().strip()
            if not path:
                status_var.set("Pendiente")
            elif Path(path).exists():
                status_var.set("Listo")
            else:
                status_var.set("No encontrado")

    def _clear_log(self) -> None:
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.config(state="disabled")

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
            messagebox.showwarning(
                "Falta fichero", "Selecciona el Excel de solicitudes."
            )
            return

        if not change_requests_path:
            messagebox.showwarning(
                "Falta fichero",
                "Selecciona el Excel de solicitudes de cambio.",
            )
            return

        if not planning_path:
            messagebox.showwarning(
                "Falta fichero", "Selecciona el Excel de planificación."
            )
            return

        if not Path(requests_path).exists():
            messagebox.showerror(
                "Fichero no encontrado", f"No existe:\n{requests_path}"
            )
            return

        if not Path(change_requests_path).exists():
            messagebox.showerror(
                "Fichero no encontrado", f"No existe:\n{change_requests_path}"
            )
            return

        if not Path(planning_path).exists():
            messagebox.showerror(
                "Fichero no encontrado", f"No existe:\n{planning_path}"
            )
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
            messagebox.showinfo(
                "Proceso completado", "Los ficheros se han procesado correctamente."
            )

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
            messagebox.showwarning(
                "Falta fichero", "Selecciona el Excel base de salida."
            )
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

            session = SessionLocal()
            self._generate_monthly_for_period(
                session=session,
                output_path=output_path,
                report_month=report_month,
            )

            self.status_var.set("Reporte mensual generado correctamente.")
            messagebox.showinfo(
                "Reporte mensual generado",
                f"Se ha generado correctamente el mensual de {report_month.strftime('%m-%Y')}.",
            )

        except Exception as ex:
            self.status_var.set(
                "Se ha producido un error durante la generación mensual."
            )
            self._append_log(f"ERROR: {ex}")
            messagebox.showerror("Error", f"Se ha producido un error:\n\n{ex}")

        finally:
            if session is not None:
                session.close()

            self._set_busy_state(False)

    def _generate_monthly_range_from_2024(self) -> None:
        output_path = self.output_file_var.get().strip()

        if not output_path:
            messagebox.showwarning(
                "Falta fichero", "Selecciona el Excel base de salida."
            )
            return

        if not Path(output_path).exists():
            messagebox.showerror("Fichero no encontrado", f"No existe:\n{output_path}")
            return

        try:
            end_month = self._get_selected_report_month()
        except ValueError as ex:
            messagebox.showerror("Periodo no válido", str(ex))
            return

        start_month = date(2024, 1, 1)

        if end_month < start_month:
            messagebox.showerror(
                "Periodo no válido",
                "El mes elegido debe ser enero de 2024 o posterior para esta generación masiva.",
            )
            return

        session = None

        try:
            self._set_busy_state(True)
            self.status_var.set("Generando mensuales 2024→mes...")

            session = SessionLocal()

            months = list(self._iterate_months(start_month, end_month))
            total = len(months)

            self._append_log(
                f"Generando mensuales desde {start_month.strftime('%m-%Y')} "
                f"hasta {end_month.strftime('%m-%Y')}..."
            )

            for index, report_month in enumerate(months, start=1):
                self.status_var.set(
                    f"Generando mensuales 2024→mes... ({index}/{total}) {report_month.strftime('%m-%Y')}"
                )
                self._generate_monthly_for_period(
                    session=session,
                    output_path=output_path,
                    report_month=report_month,
                )

            self.status_var.set("Generación masiva mensual completada.")
            messagebox.showinfo(
                "Generación completada",
                (
                    "Se han generado todos los mensuales desde 01-2024 "
                    f"hasta {end_month.strftime('%m-%Y')}."
                ),
            )

        except Exception as ex:
            self.status_var.set(
                "Se ha producido un error durante la generación masiva."
            )
            self._append_log(f"ERROR: {ex}")
            messagebox.showerror("Error", f"Se ha producido un error:\n\n{ex}")

        finally:
            if session is not None:
                session.close()

            self._set_busy_state(False)

    def _generate_monthly_for_period(
        self,
        session,
        output_path: str,
        report_month: date,
    ) -> None:
        self._append_log(
            f"Generando reporte mensual para {report_month.strftime('%m-%Y')}..."
        )

        indicator_service = MonthlyIndicatorService(session)
        writer = MonthlyTemplateWriter()

        self._append_log("Leyendo report codes del bloque IN03 desde la plantilla...")
        report_codes = writer.get_in03_report_codes(output_path)
        self._append_log("Report codes IN03 detectados: " + ", ".join(report_codes))

        self._append_log("Calculando IN03-EFEC-IL...")
        in03_values = (
            indicator_service.calculate_in03_planning_compliance_by_report_code(
                year=report_month.year,
                month=report_month.month,
                report_codes=report_codes,
            )
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

        self._append_log("Calculando IN20-EFIC-II...")
        in20_value = indicator_service.calculate_in20_new_requests(
            year=report_month.year,
            month=report_month.month,
        )
        self._append_log(f"IN20-EFIC-II = {in20_value}")

        self._append_log("Calculando IN23-EFIC-IA...")
        in23_value = indicator_service.calculate_in23_closed_requests(
            year=report_month.year,
            month=report_month.month,
        )
        self._append_log(f"IN23-EFIC-IA = {in23_value}")

        self._append_log("Calculando IN21-EFIC-IA...")
        in21_value = indicator_service.calculate_in21_open_requests()
        self._append_log(f"IN21-EFIC-IA = {in21_value}")

        self._append_log("Calculando IN26-EFIC-IR...")
        in26_value = indicator_service.calculate_in26_requests_in_progress_percentage()
        self._append_log(f"IN26-EFIC-IR = {in26_value}")

        self._append_log("Calculando IN22-EFIC-IA...")
        in22_value = indicator_service.calculate_in22_delivered_requests(
            year=report_month.year,
            month=report_month.month,
        )
        self._append_log(f"IN22-EFIC-IA = {in22_value}")

        self._append_log("Calculando IN24-EFIC-IA...")
        in24_value = indicator_service.calculate_in24_change_requests_total(
            year=report_month.year,
            month=report_month.month,
        )
        self._append_log(f"IN24-EFIC-IA = {in24_value}")

        self._append_log("Calculando IN25-EFIC-IP...")
        in25_value = indicator_service.calculate_in25_cancelled_requests()
        self._append_log(f"IN25-EFIC-IP = {in25_value}")

        self._append_log("Calculando IN27-EFIC-II...")
        in27_value = indicator_service.calculate_in27_delivered_requests_percentage(
            year=report_month.year,
            month=report_month.month,
        )
        self._append_log(f"IN27-EFIC-II = {in27_value}")

        self._append_log("Calculando IN08-EFEC-IP...")
        in08_value = indicator_service.calculate_in08_change_requests_triggering_new_request_percentage(
            year=report_month.year,
            month=report_month.month,
        )
        self._append_log(f"IN08-EFEC-IP = {in08_value}")

        self._append_log("Calculando IN07-EFEC-IP...")
        in07_value = indicator_service.calculate_in07_modified_requests_percentage(
            year=report_month.year,
            month=report_month.month,
        )
        self._append_log(f"IN07-EFEC-IP = {in07_value}")

        self._append_log("Calculando IN02-EFEC-IL...")
        in02_value = indicator_service.calculate_in02_budget_compliance_percentage(
            year=report_month.year,
            month=report_month.month,
        )
        self._append_log(f"IN02-EFEC-IL = {in02_value}")

        self._append_log("Calculando IN06-EFEC-IL...")
        in06_value = indicator_service.calculate_in06_finished_requests_with_budget_deviation_percentage(
            year=report_month.year,
            month=report_month.month,
        )
        self._append_log(f"IN06-EFEC-IL = {in06_value}")

        self._append_log("Calculando IN10-EFEC-IL...")
        in10_value = (
            indicator_service.calculate_in10_average_budget_deviation_percentage(
                year=report_month.year,
                month=report_month.month,
            )
        )
        self._append_log(f"IN10-EFEC-IL = {in10_value}")

        self._append_log("Calculando IN11-EFEC-IA...")
        in11_value = (
            indicator_service.calculate_in11_monthly_budget_deviation_percentage(
                year=report_month.year,
                month=report_month.month,
            )
        )
        self._append_log(f"IN11-EFEC-IA = {in11_value}")

        self._append_log("Calculando IN01-EFEC-IL...")
        in01_value = (
            indicator_service.calculate_in01_budget_planning_compliance_percentage(
                year=report_month.year,
                month=report_month.month,
            )
        )
        self._append_log(f"IN01-EFEC-IL = {in01_value}")

        self._append_log("Calculando IN05-EFEC-IL...")
        in05_value = indicator_service.calculate_in05_finished_requests_with_schedule_deviation_percentage(
            year=report_month.year,
            month=report_month.month,
        )
        self._append_log(f"IN05-EFEC-IL = {in05_value}")

        self._append_log("Calculando IN12-EFEC-IL...")
        in12_value = (
            indicator_service.calculate_in12_average_schedule_deviation_percentage(
                year=report_month.year,
                month=report_month.month,
            )
        )
        self._append_log(f"IN12-EFEC-IL = {in12_value}")

        self._append_log("Calculando IN04-EFEC-IP...")
        in04_value = indicator_service.calculate_in04_evaluated_within_48h_percentage(
            year=report_month.year,
            month=report_month.month,
        )
        self._append_log(f"IN04-EFEC-IP = {in04_value}")

        self._append_log("Calculando IN13-CALS-IR...")
        in13_value = (
            indicator_service.calculate_in13_requests_with_profile_deviation_percentage(
                year=report_month.year,
                month=report_month.month,
            )
        )
        self._append_log(f"IN13-CALS-IR = {in13_value}")

        self._append_log("Calculando IN14-CALS-IR...")
        in14_value = (
            indicator_service.calculate_in14_requests_with_specific_profiles_percentage(
                year=report_month.year,
                month=report_month.month,
            )
        )
        self._append_log(f"IN14-CALS-IR = {in14_value}")

        indicator_values = {
            "IN01-EFEC-IL": in01_value,
            "IN02-EFEC-IL": in02_value,
            "IN04-EFEC-IP": in04_value,
            "IN05-EFEC-IL": in05_value,
            "IN06-EFEC-IL": in06_value,
            "IN07-EFEC-IP": in07_value,
            "IN08-EFEC-IP": in08_value,
            "IN10-EFEC-IL": in10_value,
            "IN11-EFEC-IA": in11_value,
            "IN12-EFEC-IL": in12_value,
            "IN13-CALS-IR": in13_value,
            "IN14-CALS-IR": in14_value,
            "IN17-CALS-IR": in17_value,
            "IN18-CALS-IR": in18_value,
            "IN19-CALS-IA": in19_value,
            "IN20-EFIC-II": in20_value,
            "IN21-EFIC-IA": in21_value,
            "IN22-EFIC-IA": in22_value,
            "IN23-EFIC-IA": in23_value,
            "IN24-EFIC-IA": in24_value,
            "IN25-EFIC-IP": in25_value,
            "IN26-EFIC-IR": in26_value,
            "IN27-EFIC-II": in27_value,
            "IN28-EFIC-IA": in28_value,
        }

        self._append_log("Escribiendo resultados en la plantilla Excel...")
        result = writer.write_monthly_report(
            workbook_path=output_path,
            report_month=report_month,
            indicator_values=indicator_values,
            in03_values=in03_values,
        )

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
                "Report codes escritos en IN03: "
                + ", ".join(result.written_report_codes)
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

    def _iterate_months(self, start_month: date, end_month: date):
        current = start_month

        while current <= end_month:
            yield current

            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)

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
        button_state = "disabled" if is_busy else "normal"

        if is_busy:
            self.status_var.set("Procesando...")
            self.root.config(cursor="watch")
            self.progress_bar.start(12)
        else:
            self.root.config(cursor="")
            self.progress_bar.stop()

        for button in self.action_buttons + self.file_buttons:
            button.config(state=button_state)

        self.root.update_idletasks()

    def _append_log(self, message: str) -> None:
        self.log_text.config(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.config(state="disabled")
        self.root.update_idletasks()
