from __future__ import annotations

import os
import queue
import threading
import tkinter as tk
from tkinter import filedialog, ttk

from racing_form_etl.api.ingest import ingest_api_day
from racing_form_etl.config import get_secret_status, save_dotenv
from racing_form_etl.model.predict import generate_picks
from racing_form_etl.model.train import train_model


class APITab(ttk.Frame):
    def __init__(self, master: tk.Misc):
        super().__init__(master)
        self.msg_queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self.cancel_event = threading.Event()
        self._build_ui()
        self.after(150, self._poll)
        self._refresh_credential_state()

    def _build_ui(self) -> None:
        self.columnconfigure(1, weight=1)
        self.date_var = tk.StringVar()
        self.country_var = tk.StringVar(value="AU")
        self.db_var = tk.StringVar(value="output/racing.sqlite")
        self.outdir_var = tk.StringVar(value="output")

        self.username_var = tk.StringVar(value=os.getenv("THERACINGAPI_USERNAME", ""))
        self.password_var = tk.StringVar(value=os.getenv("THERACINGAPI_PASSWORD", ""))
        self.api_key_var = tk.StringVar(value=os.getenv("THERACINGAPI_API_KEY", ""))
        self.save_env_var = tk.BooleanVar(value=False)

        row = 0
        for label, var in [("Date (YYYY-MM-DD)", self.date_var), ("Countries (CSV)", self.country_var), ("DB Path", self.db_var), ("Outdir", self.outdir_var)]:
            ttk.Label(self, text=label).grid(row=row, column=0, sticky="w", padx=6, pady=4)
            ttk.Entry(self, textvariable=var).grid(row=row, column=1, sticky="ew", padx=6, pady=4)
            row += 1

        ttk.Button(self, text="Browse DB", command=self._pick_db).grid(row=2, column=2, padx=6)
        ttk.Button(self, text="Browse Outdir", command=self._pick_outdir).grid(row=3, column=2, padx=6)

        settings = ttk.LabelFrame(self, text="API Settings")
        settings.grid(row=row, column=0, columnspan=3, sticky="ew", padx=6, pady=6)
        settings.columnconfigure(1, weight=1)
        for i, (label, var, show) in enumerate([
            ("Username", self.username_var, None),
            ("Password", self.password_var, "*"),
            ("API Key (optional)", self.api_key_var, None),
        ]):
            ttk.Label(settings, text=label).grid(row=i, column=0, sticky="w", padx=6, pady=4)
            ttk.Entry(settings, textvariable=var, show=show).grid(row=i, column=1, sticky="ew", padx=6, pady=4)

        ttk.Checkbutton(settings, text="Save to local .env", variable=self.save_env_var).grid(row=3, column=0, sticky="w", padx=6, pady=4)
        ttk.Button(settings, text="Apply", command=self._apply_settings).grid(row=3, column=1, sticky="e", padx=6)
        row += 1

        self.status_label = ttk.Label(self, text="")
        self.status_label.grid(row=row, column=0, columnspan=3, sticky="w", padx=6)
        row += 1

        actions = ttk.Frame(self)
        actions.grid(row=row, column=0, columnspan=3, sticky="ew", padx=6, pady=6)
        self.ingest_btn = ttk.Button(actions, text="Ingest", command=self._start_ingest)
        self.train_btn = ttk.Button(actions, text="Train", command=self._start_train)
        self.picks_btn = ttk.Button(actions, text="Picks", command=self._start_picks)
        self.cancel_btn = ttk.Button(actions, text="Cancel", command=self.cancel_event.set)
        self.ingest_btn.grid(row=0, column=0, padx=4)
        self.train_btn.grid(row=0, column=1, padx=4)
        self.picks_btn.grid(row=0, column=2, padx=4)
        self.cancel_btn.grid(row=0, column=3, padx=4)
        row += 1

        self.progress = ttk.Progressbar(self, mode="determinate", maximum=100)
        self.progress.grid(row=row, column=0, columnspan=3, sticky="ew", padx=6, pady=6)
        row += 1

        self.log = tk.Text(self, height=14)
        self.log.grid(row=row, column=0, columnspan=3, sticky="nsew", padx=6, pady=6)
        self.rowconfigure(row, weight=1)

    def _pick_db(self) -> None:
        path = filedialog.asksaveasfilename(defaultextension=".sqlite")
        if path:
            self.db_var.set(path)

    def _pick_outdir(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.outdir_var.set(path)

    def _apply_settings(self) -> None:
        os.environ["THERACINGAPI_USERNAME"] = self.username_var.get().strip()
        os.environ["THERACINGAPI_PASSWORD"] = self.password_var.get().strip()
        os.environ["THERACINGAPI_API_KEY"] = self.api_key_var.get().strip()
        if self.save_env_var.get():
            save_dotenv(
                {
                    "THERACINGAPI_USERNAME": self.username_var.get().strip(),
                    "THERACINGAPI_PASSWORD": self.password_var.get().strip(),
                    "THERACINGAPI_API_KEY": self.api_key_var.get().strip(),
                }
            )
        self._refresh_credential_state()
        self._log("Applied API settings (values hidden).")

    def _refresh_credential_state(self) -> None:
        status = get_secret_status()
        ready = status["THERACINGAPI_USERNAME"] and status["THERACINGAPI_PASSWORD"]
        self.ingest_btn.configure(state=("normal" if ready else "disabled"))
        self.status_label.configure(text="Credentials configured." if ready else "Missing API username/password.")

    def _run_bg(self, fn, label: str) -> None:
        self.cancel_event.clear()
        self.progress["value"] = 5
        self._set_buttons(False)

        def worker() -> None:
            try:
                self.msg_queue.put(("log", f"Starting {label}..."))
                result = fn()
                self.msg_queue.put(("progress", "100"))
                self.msg_queue.put(("log", f"Completed {label}: {result}"))
            except Exception as exc:
                self.msg_queue.put(("log", f"Error during {label}: {exc}"))
            finally:
                self.msg_queue.put(("done", ""))

        threading.Thread(target=worker, daemon=True).start()

    def _start_ingest(self) -> None:
        date = self.date_var.get().strip()
        countries = [x.strip() for x in self.country_var.get().split(",") if x.strip()]
        self._run_bg(
            lambda: ingest_api_day(self.db_var.get(), date, countries, cancel_event=self.cancel_event, outdir=self.outdir_var.get()),
            "ingest",
        )

    def _start_train(self) -> None:
        self._run_bg(lambda: train_model(self.db_var.get(), self.outdir_var.get()), "train")

    def _start_picks(self) -> None:
        date = self.date_var.get().strip()
        self._run_bg(
            lambda: generate_picks(self.db_var.get(), date, os.path.join(self.outdir_var.get(), f"picks_{date}.csv"), os.path.join(self.outdir_var.get(), "model.pkl")),
            "picks",
        )

    def _set_buttons(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        self.train_btn.configure(state=state)
        self.picks_btn.configure(state=state)
        self.cancel_btn.configure(state=("normal" if not enabled else "disabled"))
        self._refresh_credential_state()

    def _poll(self) -> None:
        while True:
            try:
                msg_type, payload = self.msg_queue.get_nowait()
            except queue.Empty:
                break
            if msg_type == "log":
                self._log(payload)
            elif msg_type == "progress":
                self.progress["value"] = float(payload)
            elif msg_type == "done":
                self._set_buttons(True)
                self.progress["value"] = 100
        self.after(150, self._poll)

    def _log(self, message: str) -> None:
        self.log.insert("end", message + "\n")
        self.log.see("end")
