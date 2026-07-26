#!/usr/bin/env python3
"""
Build the combined submission PDF from docs/UrbanPulse_Combined_Report.md.

Renders Markdown (via marked) and the Task A Mermaid diagram (via mermaid) into a
styled, print-ready HTML page, then uses headless Google Chrome to export a PDF.
Both JS libs are inlined so rendering needs no network at print time.

Usage:
    python3 scripts/build_pdf.py \
        [--md docs/UrbanPulse_Combined_Report.md] \
        [--out docs/UrbanPulse_Report.pdf] \
        [--lib-dir <dir with marked.min.js + mermaid.min.js>]

If the libs aren't found locally, download them once:
    curl -fsSL https://cdn.jsdelivr.net/npm/marked@12/marked.min.js  -o marked.min.js
    curl -fsSL https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js -o mermaid.min.js
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

CHROME_CANDIDATES = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "google-chrome", "chromium", "chromium-browser",
]

HTML_TEMPLATE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>UrbanPulse Combined Report</title>
<script>{marked}</script>
<script>{mermaid}</script>
<style>
  @page {{ size: A4; margin: 16mm 15mm; }}
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, "Segoe UI", Helvetica, Arial, sans-serif;
         font-size: 10.5pt; line-height: 1.5; color: #1a1a1a; max-width: 100%; }}
  h1 {{ font-size: 20pt; border-bottom: 2px solid #2b6cb0; padding-bottom: 4px; color:#1a365d; }}
  h2 {{ font-size: 15pt; margin-top: 1.4em; color:#2c5282; border-bottom:1px solid #cbd5e0; padding-bottom:2px; }}
  h3 {{ font-size: 12.5pt; color:#2d3748; }}
  h4 {{ font-size: 11pt; color:#2d3748; }}
  code {{ font-family: "SF Mono", Menlo, Consolas, monospace; font-size: 9pt;
          background:#f1f5f9; padding:1px 4px; border-radius:3px; }}
  pre {{ background:#f7fafc; border:1px solid #e2e8f0; border-radius:6px; padding:10px;
         font-size:7.9pt; line-height:1.35;
         white-space: pre-wrap; overflow-wrap: anywhere; word-break: break-word; }}
  pre code {{ background:none; padding:0; white-space: inherit; }}
  table {{ border-collapse: collapse; width:100%; font-size:8.4pt; margin:10px 0;
           table-layout: fixed; page-break-inside: avoid; }}
  th, td {{ border:1px solid #cbd5e0; padding:4px 6px; text-align:left; vertical-align:top;
            overflow-wrap: anywhere; word-break: break-word; }}
  th {{ background:#edf2f7; }}
  tr:nth-child(even) td {{ background:#f9fafb; }}
  blockquote {{ border-left:3px solid #90cdf4; margin:10px 0; padding:2px 12px;
                color:#4a5568; background:#f7fafc; }}
  .mermaid {{ text-align:center; margin:14px 0; page-break-inside: avoid; }}
  .mermaid svg {{ max-width:100%; height:auto; }}
  .pagebreak {{ page-break-after: always; }}
  a {{ color:#2b6cb0; text-decoration:none; }}
  img {{ max-width:100%; }}
</style></head>
<body>
<script type="text/markdown" id="src">
{markdown}
</script>
<div id="content">rendering…</div>
<script>
  (async () => {{
    let md = document.getElementById('src').textContent;
    md = md.replace(/\\\\newpage/g, '\\n\\n<div class="pagebreak"></div>\\n\\n');
    marked.setOptions({{ gfm: true, breaks: false }});
    document.getElementById('content').innerHTML = marked.parse(md);
    // Convert fenced ```mermaid blocks into mermaid containers.
    document.querySelectorAll('code.language-mermaid').forEach(el => {{
      const div = document.createElement('div');
      div.className = 'mermaid';
      div.textContent = el.textContent;
      el.closest('pre').replaceWith(div);
    }});
    mermaid.initialize({{ startOnLoad: false, theme: 'default',
                          flowchart: {{ useMaxWidth: true, htmlLabels: true }} }});
    try {{ await mermaid.run(); }} catch (e) {{ console.error('mermaid', e); }}
    document.title = 'READY';   // signal for the render wait
  }})();
</script>
</body></html>
"""


def find_chrome() -> str:
    for c in CHROME_CANDIDATES:
        if os.path.isabs(c) and os.path.exists(c):
            return c
        from shutil import which
        if which(c):
            return c
    sys.exit("Google Chrome / Chromium not found — install it or edit CHROME_CANDIDATES.")


def main() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)
    ap = argparse.ArgumentParser()
    ap.add_argument("--md", default=os.path.join(root, "docs", "UrbanPulse_Combined_Report.md"))
    ap.add_argument("--out", default=os.path.join(root, "docs", "UrbanPulse_Report.pdf"))
    ap.add_argument("--lib-dir", default=here)
    args = ap.parse_args()

    def read_lib(name: str) -> str:
        for d in (args.lib_dir, here, root):
            p = os.path.join(d, name)
            if os.path.exists(p):
                return open(p, encoding="utf-8").read()
        sys.exit(f"{name} not found — download it (see this script's docstring).")

    markdown = open(args.md, encoding="utf-8").read()
    html = HTML_TEMPLATE.format(marked=read_lib("marked.min.js"),
                                mermaid=read_lib("mermaid.min.js"),
                                markdown=markdown)
    html_path = os.path.join(root, "docs", "report.html")
    with open(html_path, "w", encoding="utf-8") as fh:
        fh.write(html)

    chrome = find_chrome()
    cmd = [chrome, "--headless=new", "--disable-gpu", "--no-sandbox",
           "--no-pdf-header-footer", "--run-all-compositor-stages-before-draw",
           "--virtual-time-budget=25000",
           f"--print-to-pdf={args.out}", f"file://{html_path}"]
    print(f"Rendering PDF with Chrome…\n  {os.path.basename(args.md)} → {args.out}")
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    size = os.path.getsize(args.out)
    print(f"✓ wrote {args.out} ({size/1024:.0f} KB)")


if __name__ == "__main__":
    main()
