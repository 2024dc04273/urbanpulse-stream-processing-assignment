#!/usr/bin/env python3
"""
Build the combined submission DOCX from the three task reports.

Steps:
  1. Render the Task A Mermaid diagram to docs/diagrams/architecture.png using
     the vendored mermaid.min.js + headless Chrome (a screenshot).
  2. Assemble a Markdown source that concatenates Tasks A/B/C, replacing the
     Mermaid code fence with the rendered image and inserting page breaks.
  3. Convert to .docx with pandoc.

Requires: pandoc (`brew install pandoc`), Google Chrome, and the vendored
scripts/mermaid.min.js (see build_pdf.py docstring for the download URL).

Usage:  python3 scripts/build_docx.py
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from shutil import which

CHROME_CANDIDATES = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "google-chrome", "chromium", "chromium-browser",
]


def find(cmds):
    for c in cmds:
        if os.path.isabs(c) and os.path.exists(c):
            return c
        if which(c):
            return c
    return None


def render_diagram(root: str, chrome: str) -> None:
    here = os.path.join(root, "scripts")
    mermaid_js = open(os.path.join(here, "mermaid.min.js"), encoding="utf-8").read()
    md = open(os.path.join(root, "docs", "task_a_architecture.md"), encoding="utf-8").read()
    graph = re.search(r"```mermaid\n(.*?)```", md, re.S).group(1).strip()
    html = (f"<!doctype html><html><head><meta charset='utf-8'><script>{mermaid_js}</script>"
            "<style>html,body{margin:0;padding:16px;background:#fff}"
            ".mermaid svg{height:auto}</style></head><body>"
            f"<div class='mermaid'>{graph}</div>"
            "<script>mermaid.initialize({startOnLoad:false,theme:'default',"
            "flowchart:{useMaxWidth:false,htmlLabels:true}});"
            "mermaid.run().then(()=>document.title='READY');</script></body></html>")
    html_path = os.path.join(root, "docs", "_diagram.html")
    open(html_path, "w", encoding="utf-8").write(html)
    out = os.path.join(root, "docs", "diagrams", "architecture.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    subprocess.run([chrome, "--headless=new", "--disable-gpu", "--no-sandbox",
                    "--hide-scrollbars", "--force-device-scale-factor=2",
                    "--window-size=2760,1120", "--virtual-time-budget=10000",
                    "--default-background-color=FFFFFFFF",
                    f"--screenshot={out}", f"file://{html_path}"],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.remove(html_path)
    print(f"  ✓ diagram → {out}")


def build_source(root: str) -> str:
    """Regenerate both submission Markdown and its DOCX-specific source."""
    docs = os.path.join(root, "docs")
    title = (
        "# UrbanPulse — Combined Report (Tasks A, B, C)\n\n"
        "**DSE ZG556 / CC ZG556 — Stream Processing & Analytics**\n\n"
        "Situated Learning Assignment · Domain 3: Smart Cities & Urban Infrastructure · 75 marks\n\n"
        "Real-Time Urban Operations Intelligence Platform for MetroConnect. Every result "
        "shown was produced by running the accompanying code against the Docker Compose stack.\n"
    )
    markdown_page_break = "\n\n\\newpage\n\n"
    docx_page_break = ('\n\n```{=openxml}\n'
                       '<w:p><w:r><w:br w:type="page"/></w:r></w:p>\n'
                       '```\n\n')
    a = open(os.path.join(docs, "task_a_architecture.md"), encoding="utf-8").read()
    b = open(os.path.join(docs, "task_b_kafka.md"), encoding="utf-8").read()
    c = open(os.path.join(docs, "task_c_flink_spark.md"), encoding="utf-8").read()

    # The PDF builder consumes this common Markdown source and renders Mermaid
    # natively; keep it synchronised with the three canonical task reports.
    combined = markdown_page_break.join([title, a, b, c])
    combined_path = os.path.join(docs, "UrbanPulse_Combined_Report.md")
    open(combined_path, "w", encoding="utf-8").write(combined)

    # Pandoc needs a raster architecture diagram for deterministic DOCX output.
    a = re.sub(r"```mermaid\n.*?```",
               "![UrbanPulse Architecture — four streams → Kafka → speed & batch layers "
               "→ polyglot storage → serving](diagrams/architecture.png)",
               a, count=1, flags=re.S)
    src = os.path.join(docs, "_docx_source.md")
    open(src, "w", encoding="utf-8").write(docx_page_break.join([title, a, b, c]))
    return src


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    chrome = find(CHROME_CANDIDATES) or sys.exit("Chrome/Chromium not found.")
    pandoc = find(["pandoc"]) or sys.exit("pandoc not found — `brew install pandoc`.")
    print("Building DOCX…")
    render_diagram(root, chrome)
    src = build_source(root)
    out = os.path.join(root, "docs", "UrbanPulse_Report.docx")
    subprocess.run([pandoc, os.path.basename(src), "-o", os.path.basename(out)],
                   cwd=os.path.join(root, "docs"), check=True)
    os.remove(src)
    print(f"  ✓ wrote {out} ({os.path.getsize(out)/1024:.0f} KB)")


if __name__ == "__main__":
    main()
