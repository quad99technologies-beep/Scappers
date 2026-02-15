#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Report Generator Module

Automated report generation for scraper outputs.
Runs AFTER scraping completes - does NOT touch scraping logic.

Usage:
    from core.progress.report_generator import generate_report, generate_summary_report
    
    # Generate a detailed report for a scraper run
    report = generate_report(
        scraper_name="Malaysia",
        output_dir="output/Malaysia",
        format="html"
    )
    
    # Generate a summary across all scrapers
    summary = generate_summary_report(scrapers=["Malaysia", "Argentina", "India"])
"""

import logging
import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from collections import defaultdict

import pandas as pd

logger = logging.getLogger(__name__)

# Try to import jinja2 for HTML reports
try:
    from jinja2 import Template, Environment, BaseLoader
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False
    Template = None


class ReportGenerator:
    """
    Report generator for scraper outputs.
    
    Generates reports in various formats (HTML, JSON, Markdown, text).
    """
    
    # HTML template for reports
    HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }}</title>
    <style>
        :root {
            --primary-color: #2563eb;
            --success-color: #10b981;
            --warning-color: #f59e0b;
            --error-color: #ef4444;
            --bg-color: #f8fafc;
            --card-bg: #ffffff;
            --text-color: #1e293b;
            --border-color: #e2e8f0;
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background-color: var(--bg-color);
            color: var(--text-color);
            line-height: 1.6;
            padding: 2rem;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        
        h1 {
            color: var(--primary-color);
            margin-bottom: 0.5rem;
            font-size: 2rem;
        }
        
        .subtitle {
            color: #64748b;
            margin-bottom: 2rem;
        }
        
        .card {
            background: var(--card-bg);
            border-radius: 0.5rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            padding: 1.5rem;
            margin-bottom: 1.5rem;
        }
        
        .card h2 {
            font-size: 1.25rem;
            margin-bottom: 1rem;
            color: var(--text-color);
            border-bottom: 2px solid var(--primary-color);
            padding-bottom: 0.5rem;
        }
        
        .metrics {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
        }
        
        .metric {
            background: var(--bg-color);
            padding: 1rem;
            border-radius: 0.5rem;
            text-align: center;
        }
        
        .metric-value {
            font-size: 2rem;
            font-weight: bold;
            color: var(--primary-color);
        }
        
        .metric-label {
            color: #64748b;
            font-size: 0.875rem;
        }
        
        .status-ok { color: var(--success-color); }
        .status-warning { color: var(--warning-color); }
        .status-error { color: var(--error-color); }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 1rem;
        }
        
        th, td {
            padding: 0.75rem;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }
        
        th {
            background: var(--bg-color);
            font-weight: 600;
        }
        
        tr:hover {
            background: var(--bg-color);
        }
        
        .badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        
        .badge-success { background: #d1fae5; color: #065f46; }
        .badge-warning { background: #fef3c7; color: #92400e; }
        .badge-error { background: #fee2e2; color: #991b1b; }
        
        .footer {
            margin-top: 2rem;
            text-align: center;
            color: #94a3b8;
            font-size: 0.875rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>{{ title }}</h1>
        <p class="subtitle">Generated: {{ generated_at }}</p>
        
        <div class="metrics">
            {% for metric in metrics %}
            <div class="metric">
                <div class="metric-value {{ metric.style or '' }}">{{ metric.value }}</div>
                <div class="metric-label">{{ metric.label }}</div>
            </div>
            {% endfor %}
        </div>
        
        {% for section in sections %}
        <div class="card">
            <h2>{{ section.title }}</h2>
            {% if section.type == 'table' %}
            <table>
                <thead>
                    <tr>
                        {% for col in section.columns %}
                        <th>{{ col }}</th>
                        {% endfor %}
                    </tr>
                </thead>
                <tbody>
                    {% for row in section.rows %}
                    <tr>
                        {% for col in section.columns %}
                        <td>{{ row[col] }}</td>
                        {% endfor %}
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% elif section.type == 'list' %}
            <ul>
                {% for item in section.items %}
                <li>{{ item }}</li>
                {% endfor %}
            </ul>
            {% elif section.type == 'text' %}
            <p>{{ section.content }}</p>
            {% endif %}
        </div>
        {% endfor %}
        
        <div class="footer">
            <p>Scraper Platform Report | {{ scraper_name }}</p>
        </div>
    </div>
</body>
</html>
    """
    
    def __init__(self, scraper_name: str):
        """
        Initialize report generator.
        
        Args:
            scraper_name: Name of the scraper
        """
        self.scraper_name = scraper_name
        self.data: Dict[str, Any] = {
            "scraper_name": scraper_name,
            "generated_at": datetime.now().isoformat(),
            "metrics": [],
            "sections": [],
        }
    
    def add_metric(self, label: str, value: Any, style: str = None):
        """
        Add a metric to the report.
        
        Args:
            label: Metric label
            value: Metric value
            style: Optional CSS class (status-ok, status-warning, status-error)
        """
        self.data["metrics"].append({
            "label": label,
            "value": value,
            "style": style,
        })
    
    def add_table_section(self, title: str, data: List[Dict], columns: List[str] = None):
        """
        Add a table section to the report.
        
        Args:
            title: Section title
            data: List of dictionaries
            columns: Column names (auto-detect if None)
        """
        if not data:
            return
        
        if columns is None:
            columns = list(data[0].keys())
        
        self.data["sections"].append({
            "title": title,
            "type": "table",
            "columns": columns,
            "rows": data,
        })
    
    def add_list_section(self, title: str, items: List[str]):
        """Add a list section to the report."""
        self.data["sections"].append({
            "title": title,
            "type": "list",
            "items": items,
        })
    
    def add_text_section(self, title: str, content: str):
        """Add a text section to the report."""
        self.data["sections"].append({
            "title": title,
            "type": "text",
            "content": content,
        })
    
    def generate_html(self, output_path: Union[str, Path] = None) -> str:
        """
        Generate HTML report.
        
        Args:
            output_path: Optional path to save the report
        
        Returns:
            HTML content as string
        """
        if JINJA2_AVAILABLE:
            template = Template(self.HTML_TEMPLATE)
            html = template.render(
                title=f"{self.scraper_name} Report",
                generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                scraper_name=self.scraper_name,
                metrics=self.data["metrics"],
                sections=self.data["sections"],
            )
        else:
            # Fallback: simple HTML generation
            html = self._generate_simple_html()
        
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html)
            logger.info(f"HTML report saved to {output_path}")
        
        return html
    
    def _generate_simple_html(self) -> str:
        """Generate simple HTML without Jinja2."""
        html_parts = [
            "<!DOCTYPE html>",
            "<html><head><title>{} Report</title></head>".format(self.scraper_name),
            "<body>",
            "<h1>{} Report</h1>".format(self.scraper_name),
            "<p>Generated: {}</p>".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        ]
        
        # Metrics
        html_parts.append("<h2>Metrics</h2><ul>")
        for metric in self.data["metrics"]:
            html_parts.append(f"<li><strong>{metric['label']}:</strong> {metric['value']}</li>")
        html_parts.append("</ul>")
        
        # Sections
        for section in self.data["sections"]:
            html_parts.append(f"<h2>{section['title']}</h2>")
            if section["type"] == "table":
                html_parts.append("<table border='1'><tr>")
                for col in section["columns"]:
                    html_parts.append(f"<th>{col}</th>")
                html_parts.append("</tr>")
                for row in section["rows"][:100]:  # Limit rows
                    html_parts.append("<tr>")
                    for col in section["columns"]:
                        html_parts.append(f"<td>{row.get(col, '')}</td>")
                    html_parts.append("</tr>")
                html_parts.append("</table>")
            elif section["type"] == "list":
                html_parts.append("<ul>")
                for item in section["items"]:
                    html_parts.append(f"<li>{item}</li>")
                html_parts.append("</ul>")
            elif section["type"] == "text":
                html_parts.append(f"<p>{section['content']}</p>")
        
        html_parts.append("</body></html>")
        return "\n".join(html_parts)
    
    def generate_json(self, output_path: Union[str, Path] = None) -> str:
        """Generate JSON report."""
        json_str = json.dumps(self.data, indent=2, default=str)
        
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(json_str)
            logger.info(f"JSON report saved to {output_path}")
        
        return json_str
    
    def generate_markdown(self, output_path: Union[str, Path] = None) -> str:
        """Generate Markdown report."""
        md_parts = [
            f"# {self.scraper_name} Report",
            f"\n*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n",
        ]
        
        # Metrics
        md_parts.append("## Metrics\n")
        for metric in self.data["metrics"]:
            md_parts.append(f"- **{metric['label']}:** {metric['value']}")
        md_parts.append("")
        
        # Sections
        for section in self.data["sections"]:
            md_parts.append(f"## {section['title']}\n")
            
            if section["type"] == "table":
                # Header
                md_parts.append("| " + " | ".join(section["columns"]) + " |")
                md_parts.append("| " + " | ".join(["---"] * len(section["columns"])) + " |")
                # Rows
                for row in section["rows"][:50]:
                    md_parts.append("| " + " | ".join(str(row.get(col, "")) for col in section["columns"]) + " |")
                if len(section["rows"]) > 50:
                    md_parts.append(f"\n*... and {len(section['rows']) - 50} more rows*")
            
            elif section["type"] == "list":
                for item in section["items"]:
                    md_parts.append(f"- {item}")
            
            elif section["type"] == "text":
                md_parts.append(section["content"])
            
            md_parts.append("")
        
        md_content = "\n".join(md_parts)
        
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(md_content)
            logger.info(f"Markdown report saved to {output_path}")
        
        return md_content
    
    def generate_text(self, output_path: Union[str, Path] = None) -> str:
        """Generate plain text report."""
        lines = [
            "=" * 60,
            f"{self.scraper_name} Report".center(60),
            "=" * 60,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "METRICS",
            "-" * 40,
        ]
        
        for metric in self.data["metrics"]:
            lines.append(f"  {metric['label']}: {metric['value']}")
        
        for section in self.data["sections"]:
            lines.append("")
            lines.append(section["title"].upper())
            lines.append("-" * 40)
            
            if section["type"] == "table":
                # Simple table format
                for row in section["rows"][:20]:
                    lines.append("  " + " | ".join(f"{k}: {v}" for k, v in row.items()))
                if len(section["rows"]) > 20:
                    lines.append(f"  ... and {len(section['rows']) - 20} more rows")
            
            elif section["type"] == "list":
                for item in section["items"]:
                    lines.append(f"  • {item}")
            
            elif section["type"] == "text":
                lines.append(f"  {section['content']}")
        
        lines.append("")
        lines.append("=" * 60)
        
        text_content = "\n".join(lines)
        
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(text_content)
            logger.info(f"Text report saved to {output_path}")
        
        return text_content


def generate_report(
    scraper_name: str,
    output_dir: Union[str, Path],
    format: str = "html",
    output_path: Union[str, Path] = None,
) -> Dict[str, Any]:
    """
    Generate a report for a scraper's output.
    
    Args:
        scraper_name: Name of the scraper
        output_dir: Directory containing output files
        format: Report format - "html", "json", "markdown", "text"
        output_path: Path for the report file (auto-generated if None)
    
    Returns:
        Dict with report info and path
    """
    output_dir = Path(output_dir)
    
    if not output_dir.exists():
        return {"error": f"Output directory not found: {output_dir}"}
    
    # Initialize report generator
    report = ReportGenerator(scraper_name)
    
    # Analyze output files
    csv_files = list(output_dir.glob("*.csv"))
    xlsx_files = list(output_dir.glob("*.xlsx"))
    all_files = csv_files + xlsx_files
    
    # Calculate metrics
    total_records = 0
    total_size = 0
    file_info = []
    
    for file_path in all_files:
        try:
            if file_path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
            
            file_info.append({
                "File": file_path.name,
                "Records": len(df),
                "Columns": len(df.columns),
                "Size (KB)": round(file_path.stat().st_size / 1024, 1),
            })
            total_records += len(df)
            total_size += file_path.stat().st_size
        except Exception as e:
            file_info.append({
                "File": file_path.name,
                "Records": "Error",
                "Columns": "Error",
                "Size (KB)": round(file_path.stat().st_size / 1024, 1),
            })
    
    # Add metrics
    report.add_metric("Total Files", len(all_files))
    report.add_metric("Total Records", f"{total_records:,}")
    report.add_metric("Total Size", f"{total_size / 1024 / 1024:.2f} MB")
    report.add_metric("Status", "Complete", "status-ok")
    
    # Add file details section
    if file_info:
        report.add_table_section("Output Files", file_info)
    
    # Generate report in specified format
    if output_path is None:
        ext = {"html": ".html", "json": ".json", "markdown": ".md", "text": ".txt"}
        output_path = output_dir / f"report_{scraper_name}{ext.get(format, '.html')}"
    
    if format == "html":
        content = report.generate_html(output_path)
    elif format == "json":
        content = report.generate_json(output_path)
    elif format == "markdown":
        content = report.generate_markdown(output_path)
    else:
        content = report.generate_text(output_path)
    
    return {
        "success": True,
        "scraper_name": scraper_name,
        "report_path": str(output_path),
        "format": format,
        "total_records": total_records,
        "total_files": len(all_files),
    }


def generate_summary_report(
    scrapers: List[str] = None,
    output_dir: Union[str, Path] = None,
    format: str = "html",
) -> Dict[str, Any]:
    """
    Generate a summary report across multiple scrapers.
    
    Args:
        scrapers: List of scraper names (auto-detect if None)
        output_dir: Base output directory
        format: Report format
    
    Returns:
        Dict with report info
    """
    # Get base output directory
    if output_dir is None:
        try:
            from core.config.config_manager import ConfigManager
            output_dir = ConfigManager.get_output_dir()
        except:
            output_dir = Path(__file__).parent.parent / "output"
    
    output_dir = Path(output_dir)
    
    # Auto-detect scrapers
    if scrapers is None:
        scrapers = [d.name for d in output_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
    
    # Initialize report
    report = ReportGenerator("Platform Summary")
    
    # Collect data from each scraper
    scraper_stats = []
    total_records = 0
    total_files = 0
    
    for scraper_name in scrapers:
        scraper_dir = output_dir / scraper_name
        if not scraper_dir.exists():
            continue
        
        csv_files = list(scraper_dir.glob("*.csv"))
        xlsx_files = list(scraper_dir.glob("*.xlsx"))
        all_files = csv_files + xlsx_files
        
        records = 0
        for file_path in all_files:
            try:
                if file_path.suffix.lower() == '.csv':
                    df = pd.read_csv(file_path)
                else:
                    df = pd.read_excel(file_path)
                records += len(df)
            except:
                pass
        
        scraper_stats.append({
            "Scraper": scraper_name,
            "Files": len(all_files),
            "Records": f"{records:,}",
            "Status": "✓" if all_files else "○",
        })
        
        total_records += records
        total_files += len(all_files)
    
    # Add metrics
    report.add_metric("Total Scrapers", len(scrapers))
    report.add_metric("Total Files", total_files)
    report.add_metric("Total Records", f"{total_records:,}")
    report.add_metric("Generated", datetime.now().strftime("%Y-%m-%d"))
    
    # Add scraper summary table
    report.add_table_section("Scraper Summary", scraper_stats)
    
    # Generate report
    report_path = output_dir / f"summary_report.{format if format != 'markdown' else 'md'}"
    
    if format == "html":
        report.generate_html(report_path)
    elif format == "json":
        report.generate_json(report_path)
    elif format == "markdown":
        report.generate_markdown(report_path)
    else:
        report.generate_text(report_path)
    
    return {
        "success": True,
        "report_path": str(report_path),
        "scrapers_analyzed": len(scrapers),
        "total_records": total_records,
    }


# CLI interface
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python report_generator.py <scraper_name> <output_dir> [format]")
        print("Example: python report_generator.py Malaysia output/Malaysia html")
        print("\nFormats: html, json, markdown, text")
        sys.exit(1)
    
    scraper_name = sys.argv[1]
    output_dir = sys.argv[2]
    format = sys.argv[3] if len(sys.argv) > 3 else "html"
    
    result = generate_report(scraper_name, output_dir, format)
    
    if result.get("success"):
        print(f"✓ Report generated: {result['report_path']}")
        print(f"  Total records: {result['total_records']:,}")
        print(f"  Total files: {result['total_files']}")
    else:
        print(f"✗ Error: {result.get('error')}")
        sys.exit(1)
