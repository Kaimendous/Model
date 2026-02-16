from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from racing_form_etl.config import load_dotenv
from racing_form_etl.ui.tabs.api_tab import APITab


def create_app() -> tk.Tk:
    load_dotenv()
    root = tk.Tk()
    root.title("Racing Form ETL")
    root.geometry("980x700")

    notebook = ttk.Notebook(root)
    notebook.pack(fill="both", expand=True)

    api_tab = APITab(notebook)
    notebook.add(api_tab, text="API")
    return root


def main() -> int:
    root = create_app()
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
