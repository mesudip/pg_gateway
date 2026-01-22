#!/usr/bin/env python3
"""
Generate a tabular HTML report from pytest-benchmark JSON.

Usage:
    python scripts/generate_benchmark_report.py reports/benchmark.json reports/benchmark_table.html

This script parses the benchmark JSON produced by pytest-benchmark and
creates a simple, self-contained HTML file with a table of key statistics.
It detects connection type (gateway vs primary) from parametrized test names.
"""

import json
import sys
import os
from collections import defaultdict

CSS = """
body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }
h1 { margin-bottom: 8px; }
.table { border-collapse: collapse; width: 100%; }
.table th, .table td { border: 1px solid #ddd; padding: 8px; }
.table th { background-color: #f4f4f4; text-align: left; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 12px; }
.badge.gateway { background: #e6f4ff; color: #0b6efb; border: 1px solid #bcd8ff; }
.badge.primary { background: #e8f5e9; color: #2e7d32; border: 1px solid #c8e6c9; }
.small { color: #666; font-size: 12px; }
.footer { margin-top: 16px; color: #666; font-size: 12px; }
"""

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>pg_gateway Benchmark Report</title>
<style>{css}</style>
</head>
<body>
<h1>pg_gateway Benchmark Report</h1>
<p class="small">Comparing pg_gateway vs direct primary connection. Lower times are better.</p>
<table class="table">
<thead>
<tr>
  <th>Benchmark</th>
  <th>Connection</th>
  <th>Rounds</th>
  <th>Iterations</th>
    <th>Min (ms)</th>
    <th>Max (ms)</th>
    <th>Mean (ms)</th>
    <th>Median (ms)</th>
    <th>Stddev (ms)</th>
</tr>
</thead>
<tbody>
{rows}
</tbody>
</table>
<div class="footer">Source: {json_path}</div>
</body>
</html>
"""


def detect_connection(name: str) -> str:
    """Infer connection label from parametrized name."""
    # pytest adds parameters like: test_name[param_id]
    # We use id="gateway" or id="primary" in benchmarks
    if "[gateway]" in name:
        return "gateway"
    if "[primary]" in name:
        return "primary"
    # fallback: look for keywords
    lower = name.lower()
    if "gateway" in lower:
        return "gateway"
    if "primary" in lower:
        return "primary"
    return "unknown"


def _ms(val):
    try:
        return round(float(val) * 1000.0, 3)
    except Exception:
        return val


def render_rows(benchmarks):
    rows = []
    for b in benchmarks:
        name = b.get("name") or b.get("fullname")
        stats = b.get("stats", {})
        conn = detect_connection(name)
        badge_class = "badge gateway" if conn == "gateway" else ("badge primary" if conn == "primary" else "badge")
        rows.append(
            f"<tr>"
            f"<td>{name}</td>"
            f"<td><span class='{badge_class}'>{conn}</span></td>"
            f"<td>{stats.get('rounds','')}</td>"
            f"<td>{stats.get('iterations','')}</td>"
            f"<td>{_ms(stats.get('min',''))}</td>"
            f"<td>{_ms(stats.get('max',''))}</td>"
            f"<td>{_ms(stats.get('mean',''))}</td>"
            f"<td>{_ms(stats.get('median',''))}</td>"
            f"<td>{_ms(stats.get('stddev',''))}</td>"
            f"</tr>"
        )
    return "\n".join(rows)


def main():
    if len(sys.argv) < 3:
        print("Usage: generate_benchmark_report.py <input_json> <output_html>")
        sys.exit(1)
    input_json = sys.argv[1]
    output_html = sys.argv[2]

    with open(input_json, "r") as f:
        data = json.load(f)

    benchmarks = data.get("benchmarks", [])
    rows_html = render_rows(benchmarks)

    html = HTML_TEMPLATE.format(css=CSS, rows=rows_html, json_path=os.path.abspath(input_json))

    os.makedirs(os.path.dirname(output_html), exist_ok=True)
    with open(output_html, "w") as f:
        f.write(html)

    print(f"Wrote HTML report to: {output_html}")


if __name__ == "__main__":
    main()
