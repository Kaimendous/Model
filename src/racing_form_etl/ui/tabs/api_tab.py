from __future__ import annotations

import os
import queue
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, ttk

from racing_form_etl.api.ingest import ingest_api_day
from racing_form_etl.api.the_racing_api_client import TheRacingAPIClient
from racing_form_etl.config import get_secret_status, save_dotenv
from racing_form_etl.model.predict import generate_picks
from racing_form_etl.model.train import train_model

MIN_TRAIN_RACES = 2
MIN_TRAIN_RUNNERS = 6


def training_readiness(db_path: str) -> tuple[bool, str]:
    if not Path(db_path).exists():
        return False, "Ingest data first: DB file does not exist yet."
    try:
        with sqlite3.connect(db_path) as conn:
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            required = {"api_races", "api_runners", "api_results"}
            if not required.issubset(tables):
                return False, "Ingest data first: API tables are missing."
            races, runners = conn.execute(
                """
                SELECT
                    COUNT(DISTINCT rs.race_id),
                    COUNT(DISTINCT r.runner_id)
                FROM api_results rs
                JOIN api_runners r ON r.race_id = rs.race_id
                WHERE rs.winner_runner_id IS NOT NULL
                """
            ).fetchone()
            if races < MIN_TRAIN_RACES or runners < MIN_TRAIN_RUNNERS:
                return (
                    False,
                    f"Need >= {MIN_TRAIN_RACES} resulted races and >= {MIN_TRAIN_RUNNERS} runners (currently {races}/{runners}).",
                )
    except sqlite3.Error:
        return False, "Unable to read DB for training readiness."
    return True, "Training data looks ready."


class APITab(ttk.Frame):
    def __init__(self, master: tk.Misc):
        super().__init__(master, padding=10)
        self.msg_queue: queue.Queue[tuple[str, dict[str, str]]] = queue.Queue()
        self.cancel_event = threading.Event()
        self.capabilities: dict[str, object] = {"auth_ok": False, "can_racecards": False, "plan_message": "Unknown"}
        self._build_ui()
        self.after(150, self._poll)
        self._refresh_credential_state()

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(5, weight=1)

        self.date_var = tk.StringVar()
        self.region_var = tk.StringVar(value="gb,ire")
        self.db_var = tk.StringVar(value="output/racing.sqlite")
        self.outdir_var = tk.StringVar(value="output")
        self.mode_var = tk.StringVar(value="auto")

        self.username_var = tk.StringVar(value=os.getenv("THERACINGAPI_USERNAME", ""))
        self.password_var = tk.StringVar(value=os.getenv("THERACINGAPI_PASSWORD", ""))
        self.api_key_var = tk.StringVar(value=os.getenv("THERACINGAPI_API_KEY", ""))
        self.save_env_var = tk.BooleanVar(value=False)
        self.saved_var = tk.StringVar(value="")

        self.plan_var = tk.StringVar(value="Plan: unknown")
        strip = ttk.Frame(self)
        strip.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        strip.columnconfigure(1, weight=1)
        ttk.Label(strip, text="Capabilities / Plan", style="Accent.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Label(strip, textvariable=self.plan_var).grid(row=0, column=1, sticky="w")

        form = ttk.LabelFrame(self, text="Ingest Setup", padding=8)
        form.grid(row=1, column=0, sticky="ew")
        for col in (1,):
            form.columnconfigure(col, weight=1)

        ttk.Label(form, text="Date (YYYY-MM-DD)").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(form, textvariable=self.date_var).grid(row=0, column=1, sticky="ew", padx=4, pady=4)

        ttk.Label(form, text="Mode").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        mode_wrap = ttk.Frame(form)
        mode_wrap.grid(row=1, column=1, sticky="w", padx=4, pady=4)
        ttk.Radiobutton(mode_wrap, text="Auto", variable=self.mode_var, value="auto", command=self._refresh_buttons).grid(row=0, column=0, padx=(0, 8))
        ttk.Radiobutton(mode_wrap, text="Manual", variable=self.mode_var, value="manual", command=self._refresh_buttons).grid(row=0, column=1)

        ttk.Label(form, text="Regions (CSV)").grid(row=2, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(form, textvariable=self.region_var).grid(row=2, column=1, sticky="ew", padx=4, pady=4)
        ttk.Label(form, text="Examples: gb, ire, hk (lowercase)").grid(row=3, column=1, sticky="w", padx=4)

        quick = ttk.Frame(form)
        quick.grid(row=2, column=2, rowspan=2, sticky="n", padx=4)
        ttk.Button(quick, text="GB", command=lambda: self.region_var.set("gb")).grid(row=0, column=0, padx=2)
        ttk.Button(quick, text="IRE", command=lambda: self.region_var.set("ire")).grid(row=0, column=1, padx=2)
        ttk.Button(quick, text="HK", command=lambda: self.region_var.set("hk")).grid(row=0, column=2, padx=2)

        ttk.Label(form, text="DB Path").grid(row=4, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(form, textvariable=self.db_var).grid(row=4, column=1, sticky="ew", padx=4, pady=4)
        ttk.Button(form, text="Browse", command=self._pick_db).grid(row=4, column=2, padx=4)

        ttk.Label(form, text="Outdir").grid(row=5, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(form, textvariable=self.outdir_var).grid(row=5, column=1, sticky="ew", padx=4, pady=4)
        ttk.Button(form, text="Browse", command=self._pick_outdir).grid(row=5, column=2, padx=4)

        settings = ttk.LabelFrame(self, text="API Settings", padding=8)
        settings.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        settings.columnconfigure(1, weight=1)
        for i, (label, var, show) in enumerate([
            ("Username", self.username_var, None),
            ("Password", self.password_var, "*"),
            ("API Key (optional)", self.api_key_var, None),
        ]):
            ttk.Label(settings, text=label).grid(row=i, column=0, sticky="w", padx=4, pady=3)
            entry = ttk.Entry(settings, textvariable=var, show=show)
            entry.grid(row=i, column=1, sticky="ew", padx=4, pady=3)
            entry.bind("<Return>", lambda _e: self._apply_settings())

        control = ttk.Frame(settings)
        control.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(4, 0))
        control.columnconfigure(4, weight=1)
        ttk.Checkbutton(control, text="Save to local .env", variable=self.save_env_var).grid(row=0, column=0, sticky="w")
        ttk.Button(control, text="Apply", command=self._apply_settings).grid(row=0, column=1, padx=4)
        ttk.Button(control, text="Test Connection", command=self._test_connection).grid(row=0, column=2, padx=4)
        ttk.Button(control, text="Detect Plan / Capabilities", command=self._detect_capabilities).grid(row=0, column=3, padx=4)
        ttk.Label(control, textvariable=self.saved_var).grid(row=0, column=4, sticky="e")

        action_row = ttk.Frame(self)
        action_row.grid(row=3, column=0, sticky="ew", pady=8)
        for i in range(6):
            action_row.columnconfigure(i, weight=(1 if i == 5 else 0))
        self.discover_btn = ttk.Button(action_row, text="Discover Available Racecards", command=self._discover)
        self.ingest_btn = ttk.Button(action_row, text="Ingest", command=self._start_ingest)
        self.train_btn = ttk.Button(action_row, text="Train", command=self._start_train)
        self.picks_btn = ttk.Button(action_row, text="Picks", command=self._start_picks)
        self.cancel_btn = ttk.Button(action_row, text="Cancel", command=self.cancel_event.set)
        self.discover_btn.grid(row=0, column=0, padx=4)
        self.ingest_btn.grid(row=0, column=1, padx=4)
        self.train_btn.grid(row=0, column=2, padx=4)
        self.picks_btn.grid(row=0, column=3, padx=4)
        self.cancel_btn.grid(row=0, column=4, padx=4)

        self.inline_status = tk.StringVar(value="Apply -> Test Connection -> Discover -> Ingest -> Train -> Picks")
        ttk.Label(action_row, textvariable=self.inline_status).grid(row=0, column=5, sticky="e")

        self.progress_step = tk.StringVar(value="Idle")
        ttk.Label(self, textvariable=self.progress_step).grid(row=4, column=0, sticky="w", padx=2)
        self.progress = ttk.Progressbar(self, mode="determinate", maximum=100)
        self.progress.grid(row=4, column=0, sticky="ew", padx=(140, 2))

        content = ttk.Panedwindow(self, orient="vertical")
        content.grid(row=5, column=0, sticky="nsew")

        discovered = ttk.LabelFrame(content, text="Discovered racecards", padding=6)
        discovered.columnconfigure(0, weight=1)
        self.discovered_tree = ttk.Treeview(discovered, columns=("region", "status"), show="headings", height=5)
        self.discovered_tree.heading("region", text="Region")
        self.discovered_tree.heading("status", text="Status")
        self.discovered_tree.column("region", width=120, anchor="w")
        self.discovered_tree.column("status", width=300, anchor="w")
        self.discovered_tree.grid(row=0, column=0, sticky="nsew")
        content.add(discovered, weight=1)

        logs = ttk.LabelFrame(content, text="Logs", padding=6)
        logs.columnconfigure(0, weight=1)
        logs.rowconfigure(0, weight=1)
        self.log = tk.Text(logs, height=12, wrap="word")
        self.log.grid(row=0, column=0, sticky="nsew")
        y_scroll = ttk.Scrollbar(logs, orient="vertical", command=self.log.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        self.log.configure(yscrollcommand=y_scroll.set)
        self.log.tag_configure("INFO", foreground="#2b5fb3")
        self.log.tag_configure("WARN", foreground="#a06a00")
        self.log.tag_configure("ERROR", foreground="#c62828")
        ttk.Button(logs, text="Copy Logs", command=self._copy_logs).grid(row=1, column=0, sticky="e", pady=(6, 0))
        content.add(logs, weight=2)

    def _client(self) -> TheRacingAPIClient:
        return TheRacingAPIClient(
            username=self.username_var.get().strip(),
            password=self.password_var.get().strip(),
            api_key=self.api_key_var.get().strip() or None,
        )

    def _regions(self) -> list[str]:
        return [x.strip().lower() for x in self.region_var.get().split(",") if x.strip()]

    def _pick_db(self) -> None:
        path = filedialog.asksaveasfilename(defaultextension=".sqlite")
        if path:
            self.db_var.set(path)
            self._refresh_buttons()

    def _pick_outdir(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.outdir_var.set(path)
            self._refresh_buttons()

    def _apply_settings(self) -> None:
        os.environ["THERACINGAPI_USERNAME"] = self.username_var.get().strip()
        os.environ["THERACINGAPI_PASSWORD"] = self.password_var.get().strip()
        os.environ["THERACINGAPI_API_KEY"] = self.api_key_var.get().strip()
        self.saved_var.set("")
        if self.save_env_var.get():
            save_dotenv(
                {
                    "THERACINGAPI_USERNAME": self.username_var.get().strip(),
                    "THERACINGAPI_PASSWORD": self.password_var.get().strip(),
                    "THERACINGAPI_API_KEY": self.api_key_var.get().strip(),
                }
            )
            self.saved_var.set("Saved to .env")
        self._refresh_credential_state()
        self._log("INFO", "Applied API settings (values hidden).")

    def _refresh_credential_state(self) -> None:
        status = get_secret_status()
        ready = status["THERACINGAPI_USERNAME"] and status["THERACINGAPI_PASSWORD"]
        if not ready:
            self.inline_status.set("Missing API username/password.")
        self._refresh_buttons()

    def _refresh_buttons(self) -> None:
        creds_ok = bool(self.username_var.get().strip() and self.password_var.get().strip())
        date_ok = bool(self.date_var.get().strip())
        regions = self._regions()

        train_ok, train_msg = training_readiness(self.db_var.get().strip())
        model_exists = Path(self.outdir_var.get().strip() or "output").joinpath("model.pkl").exists()

        ingest_allowed = creds_ok and date_ok and bool(regions)
        if self.mode_var.get() == "auto" and not self.capabilities.get("can_racecards"):
            ingest_allowed = False
        self.ingest_btn.configure(state=("normal" if ingest_allowed else "disabled"))
        self.discover_btn.configure(state=("normal" if creds_ok and date_ok else "disabled"))

        self.train_btn.configure(state=("normal" if train_ok else "disabled"))
        self.picks_btn.configure(state=("normal" if model_exists else "disabled"))
        self.cancel_btn.configure(state="disabled")

        if not creds_ok:
            self.inline_status.set("Apply credentials, then test connection.")
        elif not self.capabilities.get("auth_ok"):
            self.inline_status.set("Use Test Connection and Detect Plan before ingest.")
        elif not ingest_allowed:
            self.inline_status.set("Discover available racecards for this plan/date.")
        elif not train_ok:
            self.inline_status.set(train_msg)
        elif not model_exists:
            self.inline_status.set("Train to create output/model.pkl before Picks.")
        else:
            self.inline_status.set("Ready: Ingest -> Train -> Picks")

    def _test_connection(self) -> None:
        self._run_bg(self._worker_test_connection, "test connection")

    def _detect_capabilities(self) -> None:
        self._run_bg(self._worker_detect_capabilities, "detect capabilities")

    def _discover(self) -> None:
        self._run_bg(self._worker_discover, "discover racecards")

    def _worker_test_connection(self) -> str:
        caps = self._client().probe_capabilities(self.date_var.get().strip(), self._regions()[:1] or ["gb"], cancel_event=self.cancel_event)
        self.msg_queue.put(("caps", {"caps": caps}))
        if caps["auth_ok"]:
            return "Connection successful"
        raise RuntimeError(str(caps["plan_message"]))

    def _worker_detect_capabilities(self) -> str:
        caps = self._client().probe_capabilities(self.date_var.get().strip(), self._regions() or ["gb", "ire", "hk"], cancel_event=self.cancel_event)
        self.msg_queue.put(("caps", {"caps": caps}))
        return str(caps.get("plan_message") or "Capabilities updated")

    def _worker_discover(self) -> str:
        client = self._client()
        date = self.date_var.get().strip()
        rows = []
        for region in self._regions() or ["gb", "ire", "hk"]:
            if self.cancel_event.is_set():
                raise RuntimeError("Discovery cancelled")
            try:
                client.fetch_daily_racecard_summaries(date, [region], cancel_event=self.cancel_event)
                rows.append((region, "available"))
            except Exception as exc:
                rows.append((region, f"blocked: {str(exc)[:80]}"))
        self.msg_queue.put(("discover", {"rows": rows}))
        return f"Found {sum(1 for _r, s in rows if s == 'available')} available regions"

    def _run_bg(self, fn, label: str) -> None:
        self.cancel_event.clear()
        self.progress["value"] = 5
        self.progress_step.set(f"Running: {label}")
        self._set_busy(True)

        def worker() -> None:
            try:
                self.msg_queue.put(("log", {"level": "INFO", "message": f"Starting {label}..."}))
                result = fn()
                self.msg_queue.put(("progress", {"step": "Done", "pct": "100"}))
                self.msg_queue.put(("log", {"level": "INFO", "message": f"Completed {label}: {result}"}))
            except Exception as exc:
                self.msg_queue.put(("log", {"level": "ERROR", "message": f"Error during {label}: {exc}"}))
            finally:
                self.msg_queue.put(("done", {}))

        threading.Thread(target=worker, daemon=True).start()

    def _start_ingest(self) -> None:
        date = self.date_var.get().strip()
        regions = self._regions()

        def _ingest() -> dict[str, object]:
            chosen_regions = regions
            if self.mode_var.get() == "auto":
                caps = self._client().probe_capabilities(date, regions, cancel_event=self.cancel_event)
                self.msg_queue.put(("caps", {"caps": caps}))
                chosen_regions = list(caps.get("available_regions") or [])
                if not chosen_regions:
                    raise RuntimeError(str(caps.get("plan_message") or "No available racecards for selected regions."))
                self.msg_queue.put(("log", {"level": "INFO", "message": f"Auto mode selected regions: {', '.join(chosen_regions)}"}))

            return ingest_api_day(
                self.db_var.get(),
                date,
                chosen_regions,
                cancel_event=self.cancel_event,
                outdir=self.outdir_var.get(),
                progress_cb=lambda step, pct: self.msg_queue.put(("progress", {"step": step, "pct": str(pct)})),
                minimal_payload=True,
            )

        self._run_bg(_ingest, "ingest")

    def _start_train(self) -> None:
        if self.train_btn.cget("state") == "disabled":
            self._log("WARN", self.inline_status.get())
            return
        self._run_bg(lambda: train_model(self.db_var.get(), self.outdir_var.get()), "train")

    def _start_picks(self) -> None:
        if self.picks_btn.cget("state") == "disabled":
            self._log("WARN", "Model file missing. Train first to generate picks.")
            return
        date = self.date_var.get().strip()
        self._run_bg(
            lambda: generate_picks(
                self.db_var.get(),
                date,
                os.path.join(self.outdir_var.get(), f"picks_{date}.csv"),
                os.path.join(self.outdir_var.get(), "model.pkl"),
            ),
            "picks",
        )

    def _set_busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        self.discover_btn.configure(state=state)
        self.ingest_btn.configure(state=state)
        self.train_btn.configure(state=state)
        self.picks_btn.configure(state=state)
        self.cancel_btn.configure(state=("normal" if busy else "disabled"))

    def _poll(self) -> None:
        while True:
            try:
                msg_type, payload = self.msg_queue.get_nowait()
            except queue.Empty:
                break
            if msg_type == "log":
                self._log(payload.get("level", "INFO"), payload.get("message", ""))
            elif msg_type == "progress":
                self.progress["value"] = float(payload.get("pct", "0"))
                self.progress_step.set(payload.get("step", "Running"))
            elif msg_type == "discover":
                for item in self.discovered_tree.get_children():
                    self.discovered_tree.delete(item)
                for region, status in payload.get("rows", []):
                    self.discovered_tree.insert("", "end", values=(region, status))
            elif msg_type == "caps":
                self.capabilities = payload.get("caps", {})
                plan_msg = str(self.capabilities.get("plan_message") or "Unknown")
                self.plan_var.set(f"Plan: {plan_msg}")
            elif msg_type == "done":
                self._set_busy(False)
                self._refresh_buttons()
                self.progress_step.set("Idle")
        self.after(150, self._poll)

    def _copy_logs(self) -> None:
        text = self.log.get("1.0", "end").strip()
        self.clipboard_clear()
        self.clipboard_append(text)
        self._log("INFO", "Logs copied to clipboard.")

    def _log(self, level: str, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        safe = message.replace(self.password_var.get().strip(), "***") if self.password_var.get().strip() else message
        line = f"[{timestamp}] [{level}] {safe}\n"
        self.log.insert("end", line, level)
        self.log.see("end")
