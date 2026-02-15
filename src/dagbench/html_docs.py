"""Generate self-contained HTML documentation pages for DAGBench workflows.

Each workflow gets a single HTML file with:
- Mermaid.js DAG visualization
- Per-task description table
- Provenance section
- Graph statistics
"""
from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Optional

import yaml

from dagbench.schema import WorkflowMetadata, ExtractionMethod


def _load_workflow_data(wf_dir: Path) -> tuple[WorkflowMetadata, dict]:
    """Load metadata and graph data from a workflow directory."""
    meta_path = wf_dir / "metadata.yaml"
    graph_path = wf_dir / "graph.json"

    with open(meta_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    metadata = WorkflowMetadata.model_validate(raw)

    with open(graph_path, "r", encoding="utf-8") as f:
        graph_data = json.load(f)

    return metadata, graph_data


def _mermaid_node_id(name: str) -> str:
    """Convert a task name to a valid Mermaid node ID."""
    return name.replace(" ", "_").replace("-", "_").replace(".", "_")


def _build_mermaid(graph_data: dict) -> str:
    """Build a Mermaid flowchart definition from graph.json data."""
    tg = graph_data.get("task_graph", {})
    tasks = tg.get("tasks", [])
    deps = tg.get("dependencies", [])

    lines = ["graph TD"]

    # Define nodes with costs
    for task in tasks:
        nid = _mermaid_node_id(task["name"])
        cost = task.get("cost", 0)
        label = f'{task["name"]}<br/>cost: {cost:.1f}'
        lines.append(f'    {nid}["{label}"]')

    # Define edges with sizes
    for dep in deps:
        src = _mermaid_node_id(dep["source"])
        tgt = _mermaid_node_id(dep["target"])
        size = dep.get("size", 0)
        if size > 0:
            lines.append(f'    {src} -->|"{size:.0f}"| {tgt}')
        else:
            lines.append(f"    {src} --> {tgt}")

    return "\n".join(lines)


def _task_description(task_name: str, workflow_desc: str, domain_names: list[str]) -> str:
    """Generate a brief human-readable description for a task based on its name and context."""
    name_lower = task_name.lower()
    # Common patterns
    if "source" in name_lower or "ingest" in name_lower:
        return "Data ingestion / source input"
    if "sink" in name_lower or "publish" in name_lower or "output" in name_lower:
        return "Final output / data sink"
    if "parse" in name_lower:
        return "Parse incoming data"
    if "filter" in name_lower:
        return "Filter / validate data"
    if "join" in name_lower or "merge" in name_lower or "fuse" in name_lower or "fusion" in name_lower:
        return "Merge / fuse data streams"
    if "annotate" in name_lower or "enrich" in name_lower:
        return "Enrich data with metadata"
    if "interpolat" in name_lower:
        return "Interpolate missing values"
    if "aggregate" in name_lower or "reduce" in name_lower:
        return "Aggregate / reduce data"
    if "detect" in name_lower:
        return "Detection / anomaly identification"
    if "classif" in name_lower or "predict" in name_lower:
        return "Classification / prediction"
    if "train" in name_lower:
        return "Model training"
    if "preprocess" in name_lower or "pre_process" in name_lower:
        return "Data preprocessing"
    if "transform" in name_lower or "etl" in name_lower:
        return "Data transformation"
    if "encrypt" in name_lower or "decrypt" in name_lower:
        return "Encryption / decryption"
    if "compress" in name_lower or "encode" in name_lower:
        return "Data compression / encoding"
    if "decode" in name_lower or "decompress" in name_lower:
        return "Data decompression / decoding"
    if "schedule" in name_lower or "plan" in name_lower:
        return "Task scheduling / planning"
    if "route" in name_lower or "forward" in name_lower:
        return "Data routing / forwarding"
    if "store" in name_lower or "cache" in name_lower or "write" in name_lower:
        return "Data storage / caching"
    if "read" in name_lower or "load" in name_lower or "fetch" in name_lower:
        return "Data loading / fetching"
    if "map" in name_lower:
        return "Map operation"
    if "virtual" in name_lower:
        return "Virtual node (scheduling placeholder)"
    return f"Processing step in {domain_names[0] if domain_names else 'workflow'} pipeline"


def _extraction_label(method: ExtractionMethod) -> str:
    """Human-readable label for extraction method."""
    labels = {
        ExtractionMethod.PROGRAMMATIC: "Programmatic (from source code/repo)",
        ExtractionMethod.MANUAL_FIGURE: "Manually extracted from paper figure",
        ExtractionMethod.MANUAL_TABLE: "Manually extracted from paper table",
        ExtractionMethod.TRACE_CONVERSION: "Converted from execution trace",
        ExtractionMethod.GENERATED: "Algorithmically generated",
        ExtractionMethod.EXISTING_DATASET: "From existing dataset",
        ExtractionMethod.AI_GENERATED: "AI-generated structure",
    }
    return labels.get(method, method.value)


def generate_html(wf_dir: Path, output_path: Optional[Path] = None) -> Path:
    """Generate an HTML documentation page for a workflow.

    Args:
        wf_dir: Path to the workflow directory (contains metadata.yaml + graph.json)
        output_path: Where to write the HTML file. Defaults to wf_dir/docs.html.

    Returns:
        Path to the generated HTML file.
    """
    metadata, graph_data = _load_workflow_data(wf_dir)
    mermaid_def = _build_mermaid(graph_data)

    tg = graph_data.get("task_graph", {})
    tasks = tg.get("tasks", [])
    network = graph_data.get("network", {})

    domain_names = [d.value for d in metadata.domains]

    # Build task table rows
    task_rows = []
    for task in tasks:
        name = html.escape(task["name"])
        cost = task.get("cost", 0)
        desc = html.escape(_task_description(task["name"], metadata.description, domain_names))
        task_rows.append(f"<tr><td>{name}</td><td>{cost:.2f}</td><td>{desc}</td></tr>")

    # Build provenance section
    p = metadata.provenance
    prov_lines = [f"<strong>Source:</strong> {html.escape(p.source)}"]
    if p.paper_title:
        prov_lines.append(f"<strong>Paper:</strong> {html.escape(p.paper_title)}")
    if p.authors:
        prov_lines.append(f"<strong>Authors:</strong> {html.escape(', '.join(p.authors))}")
    if p.year:
        prov_lines.append(f"<strong>Year:</strong> {p.year}")
    if p.paper_doi:
        doi_url = f"https://doi.org/{p.paper_doi}"
        prov_lines.append(f'<strong>DOI:</strong> <a href="{doi_url}" target="_blank">{html.escape(p.paper_doi)}</a>')
    if p.paper_arxiv:
        arxiv_url = f"https://arxiv.org/abs/{p.paper_arxiv}"
        prov_lines.append(f'<strong>arXiv:</strong> <a href="{arxiv_url}" target="_blank">{html.escape(p.paper_arxiv)}</a>')
    if p.figure_or_table:
        prov_lines.append(f"<strong>Figure/Table:</strong> {html.escape(p.figure_or_table)}")
    if p.repo_url:
        prov_lines.append(f'<strong>Repository:</strong> <a href="{html.escape(p.repo_url)}" target="_blank">{html.escape(p.repo_url)}</a>')
    prov_lines.append(f"<strong>Extraction Method:</strong> {html.escape(_extraction_label(p.extraction_method))}")
    prov_lines.append(f"<strong>Extractor:</strong> {html.escape(p.extractor)}")
    prov_lines.append(f"<strong>Date:</strong> {p.extraction_date}")
    if p.notes:
        prov_lines.append(f"<strong>Notes:</strong> {html.escape(p.notes)}")

    provenance_html = "<br>\n".join(prov_lines)

    # Stats
    s = metadata.graph_stats
    stats_rows = [
        f"<tr><td>Tasks</td><td>{s.num_tasks}</td></tr>",
        f"<tr><td>Edges</td><td>{s.num_edges}</td></tr>",
        f"<tr><td>Depth</td><td>{s.depth}</td></tr>",
        f"<tr><td>Width</td><td>{s.width}</td></tr>",
    ]
    if s.ccr is not None:
        stats_rows.append(f"<tr><td>CCR</td><td>{s.ccr:.4f}</td></tr>")
    if s.parallelism is not None:
        stats_rows.append(f"<tr><td>Parallelism</td><td>{s.parallelism:.4f}</td></tr>")

    # Network info
    net = metadata.network
    net_lines = [f"<strong>Included:</strong> {'Yes' if net.included else 'No'}"]
    if net.topology:
        net_lines.append(f"<strong>Topology:</strong> {net.topology.value}")
    if net.num_nodes_min is not None:
        if net.num_nodes_min == net.num_nodes_max:
            net_lines.append(f"<strong>Nodes:</strong> {net.num_nodes_min}")
        else:
            net_lines.append(f"<strong>Nodes:</strong> {net.num_nodes_min} - {net.num_nodes_max}")
    network_html = "<br>\n".join(net_lines)

    # Tags and domains
    domains_str = ", ".join(domain_names)
    tags_str = ", ".join(metadata.tags) if metadata.tags else "None"

    # Quality issues
    quality_html = ""
    if metadata.quality_issues:
        items = "".join(f"<li>{html.escape(q)}</li>" for q in metadata.quality_issues)
        quality_html = f"""
        <div class="section">
            <h2>Quality Issues</h2>
            <ul>{items}</ul>
        </div>"""

    # Mermaid needs raw definition (not HTML-escaped) inside <pre class="mermaid">
    mermaid_escaped = mermaid_def

    page_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{html.escape(metadata.name)} - DAGBench</title>
<style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background: #f8f9fa; color: #212529; }}
    .container {{ max-width: 1200px; margin: 0 auto; }}
    h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
    h2 {{ color: #34495e; margin-top: 30px; border-bottom: 1px solid #dee2e6; padding-bottom: 5px; }}
    .meta {{ color: #6c757d; margin-bottom: 20px; }}
    .section {{ background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
    .mermaid {{ text-align: center; overflow-x: auto; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #dee2e6; padding: 8px 12px; text-align: left; }}
    th {{ background: #e9ecef; font-weight: 600; }}
    tr:nth-child(even) {{ background: #f8f9fa; }}
    .badge {{ display: inline-block; background: #e9ecef; color: #495057; padding: 2px 8px; border-radius: 4px; font-size: 0.85em; margin-right: 4px; }}
    .badge-method {{ background: #d4edda; color: #155724; }}
    a {{ color: #3498db; }}
    .footer {{ text-align: center; color: #adb5bd; margin-top: 40px; font-size: 0.85em; }}
</style>
</head>
<body>
<div class="container">
    <h1>{html.escape(metadata.name)}</h1>
    <div class="meta">
        <strong>ID:</strong> {html.escape(metadata.id)} &nbsp;|&nbsp;
        <strong>Domains:</strong> {html.escape(domains_str)} &nbsp;|&nbsp;
        <strong>Completeness:</strong> {metadata.completeness.value} &nbsp;|&nbsp;
        <strong>Cost Model:</strong> {metadata.cost_model.value}
    </div>
    <p>{html.escape(metadata.description)}</p>

    <div class="section">
        <h2>DAG Visualization</h2>
        <pre class="mermaid">
{mermaid_escaped}
        </pre>
    </div>

    <div class="section">
        <h2>Tasks ({s.num_tasks})</h2>
        <table>
            <thead><tr><th>Task Name</th><th>Cost</th><th>Description</th></tr></thead>
            <tbody>
                {"".join(task_rows)}
            </tbody>
        </table>
    </div>

    <div class="section">
        <h2>Graph Statistics</h2>
        <table>
            <thead><tr><th>Metric</th><th>Value</th></tr></thead>
            <tbody>
                {"".join(stats_rows)}
            </tbody>
        </table>
    </div>

    <div class="section">
        <h2>Provenance</h2>
        <span class="badge badge-method">{html.escape(_extraction_label(p.extraction_method))}</span>
        <br><br>
        {provenance_html}
    </div>

    <div class="section">
        <h2>Network</h2>
        {network_html}
    </div>
    {quality_html}
    <div class="section">
        <h2>Tags</h2>
        {"".join(f'<span class="badge">{html.escape(t)}</span>' for t in (metadata.tags or []))}
        {'' if metadata.tags else '<em>None</em>'}
    </div>

    <div class="footer">
        Generated by DAGBench &mdash; {html.escape(metadata.id)}
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<script>mermaid.initialize({{startOnLoad: true, theme: 'default', flowchart: {{htmlLabels: true}}}});</script>
</body>
</html>"""

    if output_path is None:
        output_path = wf_dir / "docs.html"

    output_path.write_text(page_html, encoding="utf-8")
    return output_path
