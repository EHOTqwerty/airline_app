import tkinter as tk
from tkinter import ttk, messagebox
import threading
from datetime import datetime, timedelta, date
import os
import webbrowser
import csv
from pathlib import Path
from sqlalchemy import text

from app.db.connection import get_engine
from app.config.eu_codes import EU_COUNTRY_CODES
from app.config.eu_countries import EU_COUNTRIES
from app.config.top_airports import TOP_AIRPORTS

from app.etl.import_airports_ourairports import run as etl_import_airports
from app.etl.etl_weather_country import run as etl_weather
from app.etl.build_weather_risk_daily import run as etl_risk
from app.etl.generate_operations import run as etl_ops
from app.etl.apply_weather_impact import run as etl_impact
from app.etl.fetch_offers_with_fallback import run as etl_offers

from app.reports.report_weather_risk import main as rep_weather_risk
from app.reports.report_operations_vs_risk import main as rep_ops_vs_risk
from app.reports.report_prices_vs_risk import main as rep_prices_vs_risk


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Airline DB App (final) - UE + pogoda + ceny")
        self.geometry("1160x720")
        self.minsize(980, 640)

        today = date.today()
        self.start_var = tk.StringVar(value=(today + timedelta(days=7)).strftime("%Y-%m-%d"))
        self.end_var = tk.StringVar(value=(today + timedelta(days=14)).strftime("%Y-%m-%d"))

        self.country_var = tk.StringVar()
        self.dest_country_var = tk.StringVar()

        self.report_var = tk.StringVar(value="Pogoda: risk (daily)")
        self.report_outputs = {}

        self.reports = {
            "Pogoda: risk (daily)": rep_weather_risk,
            "Operacje vs ryzyko (forecast)": rep_ops_vs_risk,
            "Ceny vs ryzyko (forecast)": rep_prices_vs_risk,
        }

        self._build_ui()
        self._load_eu_countries()
        self.log("Gotowe. Kolejność: Import airports -> Pogoda -> Risk -> Operacje -> Ceny -> Raporty")

    def _build_ui(self):
        root = ttk.Frame(self, padding=10)
        root.pack(fill="both", expand=True)

        actions = ttk.LabelFrame(root, text="Akcje", padding=10)
        actions.pack(fill="x")

        for i in range(20):
            actions.columnconfigure(i, weight=0)
        actions.columnconfigure(3, weight=1)
        actions.columnconfigure(5, weight=1)
        actions.columnconfigure(9, weight=1)

        r = 0
        ttk.Button(actions, text="Test DB", command=self.test_db).grid(row=r, column=0, padx=5, pady=3, sticky="w")
        ttk.Button(actions, text="Import airports (UE)", command=self.import_airports).grid(row=r, column=1, padx=5, pady=3, sticky="w")

        ttk.Label(actions, text="Kraj (wylot):").grid(row=r, column=2, padx=(12,5), pady=3, sticky="w")
        self.country_cb = ttk.Combobox(actions, textvariable=self.country_var, state="readonly", width=28)
        self.country_cb.grid(row=r, column=3, padx=5, pady=3, sticky="w")

        ttk.Label(actions, text="Kraj docelowy:").grid(row=r, column=4, padx=(12,5), pady=3, sticky="w")
        self.dest_country_cb = ttk.Combobox(actions, textvariable=self.dest_country_var, state="readonly", width=28)
        self.dest_country_cb.grid(row=r, column=5, padx=5, pady=3, sticky="w")

        r = 1
        ttk.Label(actions, text="Start:").grid(row=r, column=0, padx=5, pady=3, sticky="w")
        ttk.Entry(actions, textvariable=self.start_var, width=12).grid(row=r, column=1, padx=5, pady=3, sticky="w")
        ttk.Label(actions, text="End:").grid(row=r, column=2, padx=5, pady=3, sticky="w")
        ttk.Entry(actions, textvariable=self.end_var, width=12).grid(row=r, column=3, padx=5, pady=3, sticky="w")

        ttk.Button(actions, text="Pobierz pogodę", command=self.fetch_weather).grid(row=r, column=4, padx=(12,5), pady=3, sticky="w")
        ttk.Button(actions, text="Build risk (daily)", command=self.build_risk).grid(row=r, column=5, padx=5, pady=3, sticky="w")
        ttk.Button(actions, text="Generuj operacje", command=self.generate_ops).grid(row=r, column=6, padx=5, pady=3, sticky="w")
        ttk.Button(actions, text="Apply weather impact", command=self.apply_impact).grid(row=r, column=7, padx=5, pady=3, sticky="w")
        ttk.Button(actions, text="Pobierz ceny (Amadeus→fallback)", command=self.fetch_prices).grid(row=r, column=8, padx=(12,5), pady=3, sticky="w")

        r = 2
        ttk.Label(actions, text="Raport:").grid(row=r, column=0, padx=5, pady=3, sticky="w")
        self.report_cb = ttk.Combobox(actions, textvariable=self.report_var, state="readonly", width=36)
        self.report_cb["values"] = list(self.reports.keys())
        self.report_cb.grid(row=r, column=1, padx=5, pady=3, sticky="w")

        ttk.Button(actions, text="Generuj", command=self.generate_report).grid(row=r, column=2, padx=(12,5), pady=3, sticky="w")
        ttk.Button(actions, text="Otwórz HTML", command=lambda: self.open_report("html")).grid(row=r, column=3, padx=5, pady=3, sticky="w")
        ttk.Button(actions, text="Otwórz CSV", command=lambda: self.open_report("csv")).grid(row=r, column=4, padx=5, pady=3, sticky="w")
        ttk.Button(actions, text="Otwórz PNG", command=lambda: self.open_report("png")).grid(row=r, column=5, padx=5, pady=3, sticky="w")

        mid = ttk.Frame(root)
        mid.pack(fill="both", expand=True, pady=10)

        stats = ttk.LabelFrame(mid, text="Statystyki", padding=10)
        stats.pack(side="left", fill="both", expand=True, padx=(0,10))
        self.stats_text = tk.Text(stats, height=12)
        self.stats_text.pack(fill="both", expand=True)

        logs = ttk.LabelFrame(mid, text="Log", padding=10)
        logs.pack(side="right", fill="both", expand=True)
        self.log_text = tk.Text(logs, height=12)
        self.log_text.pack(fill="both", expand=True)

        prev = ttk.LabelFrame(root, text="Podgląd CSV", padding=10)
        prev.pack(fill="both", expand=True)
        self.table = ttk.Treeview(prev, show="headings", height=8)
        self.table.pack(fill="both", expand=True)

    def log(self, msg: str):
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")

    def _load_eu_countries(self):
        labels = [f"{EU_COUNTRIES[c]} ({c})" for c in EU_COUNTRY_CODES]
        labels.sort()
        self.country_cb["values"] = labels
        self.dest_country_cb["values"] = labels
        self.country_var.set("Polska (PL)" if "Polska (PL)" in labels else labels[0])
        self.dest_country_var.set("Niemcy (DE)" if "Niemcy (DE)" in labels else labels[0])

    def cc(self, label: str) -> str:
        if "(" in label and ")" in label:
            return label.split("(")[-1].split(")")[0].strip().upper()
        return label.strip().upper()

    def dates_list(self):
        try:
            s = datetime.strptime(self.start_var.get().strip(), "%Y-%m-%d").date()
            e = datetime.strptime(self.end_var.get().strip(), "%Y-%m-%d").date()
            if e < s:
                return []
            out = []
            cur = s
            while cur <= e:
                out.append(cur.strftime("%Y-%m-%d"))
                cur += timedelta(days=1)
            return out
        except Exception:
            return []

    def test_db(self):
        try:
            engine = get_engine()
            with engine.connect() as conn:
                v = conn.execute(text("SELECT VERSION()")).scalar()
            self.log(f"DB OK: {v}")
            self.refresh_stats()
        except Exception as e:
            messagebox.showerror("DB error", str(e))

    def refresh_stats(self):
        try:
            engine = get_engine()
            with engine.connect() as conn:
                airports = conn.execute(text("SELECT COUNT(*) FROM airports")).scalar()
                wh = conn.execute(text("SELECT COUNT(*) FROM weather_hourly")).scalar()
                wr = conn.execute(text("SELECT COUNT(*) FROM weather_risk_daily")).scalar()
                flights = conn.execute(text("SELECT COUNT(*) FROM flights")).scalar()
                tickets = conn.execute(text("SELECT COUNT(*) FROM tickets")).scalar()
                offers = conn.execute(text("SELECT COUNT(*) FROM amadeus_flight_offers")).scalar()

            self.stats_text.delete("1.0", "end")
            self.stats_text.insert("end", f"airports: {airports}\nweather_hourly: {wh}\nweather_risk_daily: {wr}\n")
            self.stats_text.insert("end", f"flights: {flights}\ntickets: {tickets}\namadeus_flight_offers: {offers}\n")
        except Exception as e:
            self.log(f"STATS ERROR: {e}")

    def import_airports(self):
        def job():
            try:
                self.log("Import airports (UE) ...")
                msg = etl_import_airports()
                self.log(msg)
                self.refresh_stats()
            except Exception as e:
                messagebox.showerror("Import error", str(e))
        threading.Thread(target=job, daemon=True).start()

    def fetch_weather(self):
        cc = self.cc(self.country_var.get())
        start = self.start_var.get().strip()
        end = self.end_var.get().strip()

        def job():
            try:
                self.log(f"Weather ETL: {cc} {start}..{end}")
                msg = etl_weather(cc, start, end)
                self.log(msg)
                self.refresh_stats()
            except Exception as e:
                messagebox.showerror("Weather error", str(e))
        threading.Thread(target=job, daemon=True).start()

    def build_risk(self):
        cc = self.cc(self.country_var.get())
        def job():
            try:
                self.log(f"Build risk: {cc}")
                self.log(etl_risk(cc))
                self.refresh_stats()
            except Exception as e:
                messagebox.showerror("Risk error", str(e))
        threading.Thread(target=job, daemon=True).start()

    def generate_ops(self):
        cc = self.cc(self.country_var.get())
        start = self.start_var.get().strip()
        end = self.end_var.get().strip()
        def job():
            try:
                self.log(f"Generate ops: {cc} {start}..{end}")
                self.log(etl_ops(cc, start, end, flights_per_day=6))
                self.refresh_stats()
            except Exception as e:
                messagebox.showerror("Ops error", str(e))
        threading.Thread(target=job, daemon=True).start()

    def apply_impact(self):
        cc = self.cc(self.country_var.get())
        def job():
            try:
                self.log(f"Apply impact: {cc}")
                self.log(etl_impact(cc))
                self.refresh_stats()
            except Exception as e:
                messagebox.showerror("Impact error", str(e))
        threading.Thread(target=job, daemon=True).start()

    def fetch_prices(self):
        origin_cc = self.cc(self.country_var.get())
        dest_cc = self.cc(self.dest_country_var.get())
        if origin_cc == dest_cc:
            messagebox.showwarning("Uwaga", "Wybierz inny kraj docelowy.")
            return
        dates = self.dates_list()
        if not dates:
            messagebox.showwarning("Daty", "Podaj poprawne daty.")
            return

        origins = TOP_AIRPORTS.get(origin_cc, [])[:2]
        dests = TOP_AIRPORTS.get(dest_cc, [])[:2]
        if not origins or not dests:
            messagebox.showwarning("Lotniska", "Brak TOP lotnisk dla wybranych krajów.")
            return

        def job():
            try:
                self.log(f"Offers ETL: {origin_cc}->{dest_cc} dates={len(dates)} routes={len(origins)*len(dests)}")
                fb = 0
                ok = 0
                for d in dates:
                    for o in origins:
                        for ds in dests:
                            msg = etl_offers(o, ds, d, adults=1, fallback_n=10)
                            self.log(msg)
                            if msg.startswith("OK:"):
                                ok += 1
                            elif msg.startswith("FALLBACK:"):
                                fb += 1
                self.log(f"Offers done: ok={ok}, fallback={fb}")
                self.refresh_stats()
            except Exception as e:
                messagebox.showerror("Offers error", str(e))
        threading.Thread(target=job, daemon=True).start()

    def generate_report(self):
        cc = self.cc(self.country_var.get())
        name = self.report_var.get()
        fn = self.reports.get(name)
        if not fn:
            return

        def job():
            try:
                self.log(f"Report: {name} {cc}")
                csv_path, png_path, html_path = fn(cc)
                self.report_outputs[name] = {"csv": csv_path, "png": png_path, "html": html_path}
                if csv_path:
                    self.show_csv(csv_path)
                self.log("OK report")
            except Exception as e:
                messagebox.showerror("Report error", str(e))
        threading.Thread(target=job, daemon=True).start()

    def open_report(self, kind: str):
        name = self.report_var.get()
        paths = self.report_outputs.get(name, {})
        p = paths.get(kind, "")
        if not p:
            messagebox.showinfo("Brak", "Najpierw wygeneruj raport.")
            return
        P = Path(p)
        if not P.exists():
            messagebox.showerror("Brak pliku", str(P))
            return
        if P.suffix.lower() in [".html",".htm"]:
            webbrowser.open(P.resolve().as_uri())
        else:
            os.startfile(P.resolve())  # type: ignore[attr-defined]

    def show_csv(self, path: str, max_rows: int = 200):
        p = Path(path)
        if not p.exists():
            return
        self.table.delete(*self.table.get_children())

        with p.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return
            self.table["columns"] = header
            for h in header:
                self.table.heading(h, text=h)
                self.table.column(h, width=130, stretch=True)
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break
                self.table.insert("", "end", values=row)


if __name__ == "__main__":
    App().mainloop()
