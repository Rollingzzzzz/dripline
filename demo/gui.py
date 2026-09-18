"""One-click demo launcher and results viewer (stdlib tkinter, no extras).

    python demo/gui.py

A single window: one button runs the whole benchmark matrix (server and
client containers via docker compose, same as demo/run.py), the log pane
streams progress, and when the run finishes the tables fill in — budget
compliance (the 1x vs Nx story), latency, and the raw-throughput verdict
("who is faster"), computed from the same JSONs the HTML report uses.
The HTML report button opens demo/out/report.html for the shareable view.
"""

from __future__ import annotations

import json
import queue
import subprocess
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import scrolledtext, ttk

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "demo" / "out"
ENGINE_LABEL = {"dripline": "dripline", "aiolimiter": "aiolimiter"}


class DemoGui:
    def __init__(self) -> None:
        self.root = tk.Tk()
        self.root.title("dripline demo — run & compare")
        self.root.geometry("880x640")
        self.lines: queue.Queue[str] = queue.Queue()
        self.worker: threading.Thread | None = None

        top = tk.Frame(self.root)
        top.pack(fill="x", padx=10, pady=8)
        self.run_btn = tk.Button(top, text="RUN BENCHMARK", font=("Segoe UI", 12, "bold"),
                                 bg="#1a56db", fg="white", padx=18, pady=6,
                                 command=self.start_run)
        self.run_btn.pack(side="left")
        self.report_btn = tk.Button(top, text="Open HTML report", padx=10,
                                    command=self.open_report)
        self.report_btn.pack(side="left", padx=8)
        self.status = tk.Label(top, text="idle — 6 runs, ~7 minutes",
                               fg="#6b7280")
        self.status.pack(side="left", padx=10)

        self.log = scrolledtext.ScrolledText(self.root, height=12, width=110,
                                             font=("Consolas", 9), state="disabled")
        self.log.pack(fill="both", expand=True, padx=10)

        self.budget = self._table(["engine", "workers", "admitted", "ceiling",
                                   "ratio", "retry-after"])
        self.speed = self._table(["engine", "achieved req/s", "p50 ms", "p99 ms"])
        self.verdict = tk.Label(self.root, text="", font=("Segoe UI", 11, "bold"),
                                wraplength=840, justify="left")
        self.verdict.pack(fill="x", padx=10, pady=6)
        self._poll()

    # -- plumbing ----------------------------------------------------------- #

    def _table(self, headers: list[str]) -> ttk.Treeview:
        frame = tk.Frame(self.root)
        frame.pack(fill="x", padx=10, pady=4)
        view = ttk.Treeview(frame, columns=headers, show="headings", height=4)
        for h in headers:
            view.heading(h, text=h)
            view.column(h, width=110, anchor="e")
        view.column("engine", anchor="w")
        view.pack(fill="x")
        return view

    def log_line(self, line: str) -> None:
        self.log.configure(state="normal")
        self.log.insert("end", line.rstrip() + "\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    def _poll(self) -> None:
        try:
            while True:
                self.log_line(self.lines.get_nowait())
        except queue.Empty:
            pass
        self.root.after(200, self._poll)

    # -- run ---------------------------------------------------------------- #

    def start_run(self) -> None:
        if self.worker and self.worker.is_alive():
            return
        self.run_btn.configure(state="disabled")
        self.status.configure(text="running…", fg="#1a56db")
        self.worker = threading.Thread(target=self._run, daemon=True)
        self.worker.start()

    def _run(self) -> None:
        proc = subprocess.Popen(
            [sys.executable, str(ROOT / "demo" / "run.py")],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", cwd=str(ROOT))
        assert proc.stdout is not None
        for line in proc.stdout:
            self.lines.put(line)
        code = proc.wait()
        self.lines.put(f"=== run.py exited with {code}\n")
        self.root.after(0, self._render_results)

    # -- results ------------------------------------------------------------ #

    def _render_results(self) -> None:
        self.run_btn.configure(state="normal")
        runs = sorted(OUT_DIR.glob("run-*.json"))
        if not runs:
            self.status.configure(text="no results found", fg="#e02424")
            return
        for item in self.budget.get_children():
            self.budget.delete(item)
        for item in self.speed.get_children():
            self.speed.delete(item)
        speed_rps: dict[str, float] = {}
        budget_ratios: dict[str, float] = {}
        for path in runs:
            r = json.loads(path.read_text(encoding="utf-8"))
            t = r["totals"]
            if "speed" in path.stem:
                speed_rps[r["engine"]] = t["sent"] / r["duration_s"]
                lat = r.get("latency", {})
                self.speed.insert("", "end", values=(
                    ENGINE_LABEL.get(r["engine"], r["engine"]),
                    f"{t['sent'] / r['duration_s']:,.0f}",
                    lat.get("p50_ms", "—"), lat.get("p99_ms", "—")))
                continue
            ratio = t["admitted"] / r["configured_ceiling"]
            if r["workers"] == "4" or r["workers"] == 4:
                budget_ratios[r["engine"]] = ratio
            self.budget.insert("", "end", values=(
                ENGINE_LABEL.get(r["engine"], r["engine"]), r["workers"],
                f"{t['admitted']:,}", f"{r['configured_ceiling']:,.0f}",
                f"{ratio:.2f}x",
                f"{r['keys_with_retry_after']}/{r['key_count']}"))
        parts = []
        if len(budget_ratios) == 2:
            d, a = budget_ratios.get("dripline"), budget_ratios.get("aiolimiter")
            parts.append(f"BUDGET (4 workers): dripline {d:.2f}x vs "
                         f"aiolimiter {a:.2f}x of the configured limit")
        if len(speed_rps) == 2:
            fast = max(speed_rps, key=speed_rps.get)
            slow = min(speed_rps, key=speed_rps.get)
            margin = (speed_rps[fast] / speed_rps[slow] - 1) * 100
            if margin >= 5:
                parts.append(f"SPEED: {fast} is {margin:.0f}% faster "
                             f"({speed_rps[fast]:,.0f} vs {speed_rps[slow]:,.0f} req/s)")
            else:
                parts.append(f"SPEED: tie within noise "
                             f"({speed_rps['dripline']:,.0f} vs "
                             f"{speed_rps['aiolimiter']:,.0f} req/s — both saturate "
                             "the HTTP stack; the decision-layer gap is in "
                             "bench/results)")
        self.verdict.configure(text="  ·  ".join(parts) or "no comparable runs")
        self.status.configure(text="done — results below, HTML report for sharing",
                              fg="#0e9f6e")

    def open_report(self) -> None:
        path = OUT_DIR / "report.html"
        if path.exists():
            import webbrowser
            webbrowser.open(path.as_uri())


if __name__ == "__main__":
    DemoGui().root.mainloop()
