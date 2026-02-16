from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from racing_form_etl.config import load_dotenv
from racing_form_etl.ui.tabs.api_tab import APITab


def create_app() -> tk.Tk:
    load_dotenv()
    root = tk.Tk()
    root.title("Racing Form ETL")
    root.geometry("1040x760")

    style = ttk.Style(root)
    if "vista" in style.theme_names():
        style.theme_use("vista")
    elif "clam" in style.theme_names():
        style.theme_use("clam")
    style.configure("Accent.TLabel", font=("Segoe UI", 9, "bold"))

    notebook = ttk.Notebook(root)
    notebook.pack(fill="both", expand=True, padx=8, pady=8)

    api_tab = APITab(notebook)
    notebook.add(api_tab, text="API")
    return root


def main() -> int:
    root = create_app()
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
