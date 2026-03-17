from __future__ import annotations

import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from app.db.init_db import recreate_database
from app.db.session import SessionLocal
from app.importers.planning_importer import import_planning_excel
from app.importers.solicitudes_importer import import_solicitudes_excel


class MainWindow:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Metro Reporting Tool")
        self.root.geometry("760x420")
        self.root.minsize(700, 380)

        self.requests_file_var = tk.StringVar()
        self.planning_file_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Selecciona los dos ficheros de entrada.")

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
                "Selecciona el Excel de solicitudes (ST) y el Excel de planificación. "
                "Después procesa ambos ficheros para regenerar la base de datos."
            ),
            wraplength=700,
            justify="left",
        )
        description_label.pack(anchor="w", pady=(0, 20))

        files_frame = ttk.LabelFrame(main_frame, text="Ficheros de entrada", padding=12)
        files_frame.pack(fill="x", pady=(0, 16))

        # Requests / ST file
        ttk.Label(files_frame, text="Excel de solicitudes (ST):").grid(
            row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 8)
        )
        ttk.Entry(
            files_frame,
            textvariable=self.requests_file_var,
            width=72,
        ).grid(row=0, column=1, sticky="ew", pady=(0, 8))
        ttk.Button(
            files_frame,
            text="Seleccionar...",
            command=self._select_requests_file,
        ).grid(row=0, column=2, padx=(8, 0), pady=(0, 8))

        # Planning file
        ttk.Label(files_frame, text="Excel de planificación:").grid(
            row=1, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Entry(
            files_frame,
            textvariable=self.planning_file_var,
            width=72,
        ).grid(row=1, column=1, sticky="ew")
        ttk.Button(
            files_frame,
            text="Seleccionar...",
            command=self._select_planning_file,
        ).grid(row=1, column=2, padx=(8, 0))

        files_frame.columnconfigure(1, weight=1)

        actions_frame = ttk.Frame(main_frame)
        actions_frame.pack(fill="x", pady=(0, 16))

        self.process_button = ttk.Button(
            actions_frame,
            text="Procesar ficheros",
            command=self._process_files,
        )
        self.process_button.pack(side="left")

        self.generate_output_button = ttk.Button(
            actions_frame,
            text="Generar output",
            command=self._generate_output_placeholder,
        )
        self.generate_output_button.pack(side="left", padx=(8, 0))

        status_frame = ttk.LabelFrame(main_frame, text="Estado", padding=12)
        status_frame.pack(fill="both", expand=True)

        status_label = ttk.Label(
            status_frame,
            textvariable=self.status_var,
            wraplength=680,
            justify="left",
        )
        status_label.pack(anchor="w", pady=(0, 12))

        self.log_text = tk.Text(
            status_frame,
            height=12,
            wrap="word",
            state="disabled",
        )
        self.log_text.pack(fill="both", expand=True)

    def _select_requests_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Selecciona el Excel de solicitudes",
            filetypes=[
                ("Excel files", "*.xlsx *.xls"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self.requests_file_var.set(path)

    def _select_planning_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Selecciona el Excel de planificación",
            filetypes=[
                ("Excel files", "*.xlsx *.xls"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self.planning_file_var.set(path)

    def _process_files(self) -> None:
        requests_path = self.requests_file_var.get().strip()
        planning_path = self.planning_file_var.get().strip()

        if not requests_path:
            messagebox.showwarning("Falta fichero", "Selecciona el Excel de solicitudes.")
            return

        if not planning_path:
            messagebox.showwarning("Falta fichero", "Selecciona el Excel de planificación.")
            return

        if not Path(requests_path).exists():
            messagebox.showerror("Fichero no encontrado", f"No existe:\n{requests_path}")
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

    def _generate_output_placeholder(self) -> None:
        messagebox.showinfo(
            "Pendiente",
            "La generación de output todavía no está implementada.",
        )

    def _set_busy_state(self, is_busy: bool) -> None:
        if is_busy:
            self.process_button.config(state="disabled")
            self.generate_output_button.config(state="disabled")
            self.status_var.set("Procesando ficheros...")
            self.root.config(cursor="watch")
        else:
            self.process_button.config(state="normal")
            self.generate_output_button.config(state="normal")
            self.root.config(cursor="")

        self.root.update_idletasks()

    def _append_log(self, message: str) -> None:
        self.log_text.config(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.config(state="disabled")
        self.root.update_idletasks()