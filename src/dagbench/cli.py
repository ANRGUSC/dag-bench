"""CLI interface for dagbench: list, validate, stats, info, convert."""
import click


@click.group()
@click.version_option(version="0.1.0", prog_name="dagbench")
def main():
    """DAGBench: Task graph benchmark repository for scheduling research."""
    pass


@main.command("list")
@click.option("--domain", "-d", default=None, help="Filter by domain (e.g. iot, synthetic)")
@click.option("--min-tasks", default=None, type=int, help="Minimum number of tasks")
@click.option("--max-tasks", default=None, type=int, help="Maximum number of tasks")
@click.option("--tag", "-t", default=None, help="Filter by tag")
@click.option("--verbose", "-v", is_flag=True, help="Show additional details")
def list_cmd(domain, min_tasks, max_tasks, tag, verbose):
    """List all available workflows."""
    from dagbench.catalog import search as catalog_search, list_workflows

    if domain or min_tasks or max_tasks or tag:
        workflows = catalog_search(
            domain=domain, min_tasks=min_tasks, max_tasks=max_tasks, tag=tag
        )
    else:
        workflows = list_workflows()

    if not workflows:
        click.echo("No workflows found.")
        return

    # Header
    click.echo(f"\n{'ID':<45} {'Tasks':>5} {'Edges':>5} {'Depth':>5} {'Width':>5} {'Name'}")
    click.echo("-" * 110)

    for wf in sorted(workflows, key=lambda w: w.id):
        s = wf.graph_stats
        line = f"{wf.id:<45} {s.num_tasks:>5} {s.num_edges:>5} {s.depth:>5} {s.width:>5} {wf.name}"
        click.echo(line)
        if verbose:
            domains = ", ".join(d.value for d in wf.domains)
            click.echo(f"  Domains: {domains}")
            if wf.tags:
                click.echo(f"  Tags: {', '.join(wf.tags)}")
            click.echo(f"  Completeness: {wf.completeness.value}  Cost model: {wf.cost_model.value}")
            click.echo()

    click.echo(f"\nTotal: {len(workflows)} workflows")


@main.command()
@click.argument("workflow_id", required=False)
def validate(workflow_id):
    """Validate workflows (all or a specific one)."""
    from dagbench.catalog import _discover_workflow_dirs, get_workflow_path
    from dagbench.validate import validate_workflow

    if workflow_id:
        wf_dir = get_workflow_path(workflow_id)
        if wf_dir is None:
            click.echo(f"Workflow '{workflow_id}' not found.", err=True)
            raise SystemExit(1)
        dirs = [wf_dir]
    else:
        dirs = _discover_workflow_dirs()

    if not dirs:
        click.echo("No workflows found.")
        return

    all_ok = True
    for d in dirs:
        result = validate_workflow(d)
        status = click.style("PASS", fg="green") if result.ok else click.style("FAIL", fg="red")
        click.echo(f"  {status} {d.name}")
        for err in result.errors:
            click.echo(f"       ERROR: {err}")
        for warn in result.warnings:
            click.echo(f"       WARN:  {warn}")
        if not result.ok:
            all_ok = False

    click.echo()
    if all_ok:
        click.echo(f"All {len(dirs)} workflows passed validation.")
    else:
        click.echo(f"Some workflows failed validation.")
        raise SystemExit(1)


@main.command()
@click.argument("workflow_id")
def info(workflow_id):
    """Show detailed information about a workflow."""
    from dagbench.loader import load_metadata

    try:
        meta = load_metadata(workflow_id)
    except FileNotFoundError:
        click.echo(f"Workflow '{workflow_id}' not found.", err=True)
        raise SystemExit(1)

    click.echo(f"\n  ID:           {meta.id}")
    click.echo(f"  Name:         {meta.name}")
    click.echo(f"  Description:  {meta.description}")
    click.echo(f"  Domains:      {', '.join(d.value for d in meta.domains)}")
    click.echo(f"  Completeness: {meta.completeness.value}")
    click.echo(f"  Cost Model:   {meta.cost_model.value}")

    s = meta.graph_stats
    click.echo(f"\n  Graph Statistics:")
    click.echo(f"    Tasks:       {s.num_tasks}")
    click.echo(f"    Edges:       {s.num_edges}")
    click.echo(f"    Depth:       {s.depth}")
    click.echo(f"    Width:       {s.width}")
    if s.ccr is not None:
        click.echo(f"    CCR:         {s.ccr:.4f}")
    if s.parallelism is not None:
        click.echo(f"    Parallelism: {s.parallelism:.4f}")

    click.echo(f"\n  Network:")
    click.echo(f"    Included:    {meta.network.included}")
    if meta.network.topology:
        click.echo(f"    Topology:    {meta.network.topology.value}")

    p = meta.provenance
    click.echo(f"\n  Provenance:")
    click.echo(f"    Source:      {p.source}")
    if p.paper_title:
        click.echo(f"    Paper:       {p.paper_title}")
    if p.authors:
        click.echo(f"    Authors:     {', '.join(p.authors)}")
    if p.year:
        click.echo(f"    Year:        {p.year}")
    if p.paper_doi:
        click.echo(f"    DOI:         {p.paper_doi}")
    click.echo(f"    Extraction:  {p.extraction_method.value}")
    click.echo(f"    Date:        {p.extraction_date}")

    click.echo(f"\n  License:")
    click.echo(f"    Source:      {meta.license.source_license}")
    click.echo(f"    DAGBench:    {meta.license.dagbench_license}")

    if meta.tags:
        click.echo(f"\n  Tags: {', '.join(meta.tags)}")
    if meta.quality_issues:
        click.echo(f"\n  Quality Issues:")
        for issue in meta.quality_issues:
            click.echo(f"    - {issue}")
    click.echo()


@main.command()
@click.option("--domain", "-d", default=None, help="Filter by domain")
def stats(domain):
    """Show aggregate statistics across all workflows."""
    from collections import Counter
    from dagbench.catalog import list_workflows, search

    if domain:
        workflows = search(domain=domain)
    else:
        workflows = list_workflows()

    if not workflows:
        click.echo("No workflows found.")
        return

    # Aggregate stats
    domains = Counter()
    completeness = Counter()
    cost_models = Counter()
    total_tasks = 0
    total_edges = 0
    min_tasks = float("inf")
    max_tasks = 0
    depths = []
    widths = []

    for wf in workflows:
        for d in wf.domains:
            domains[d.value] += 1
        completeness[wf.completeness.value] += 1
        cost_models[wf.cost_model.value] += 1
        s = wf.graph_stats
        total_tasks += s.num_tasks
        total_edges += s.num_edges
        min_tasks = min(min_tasks, s.num_tasks)
        max_tasks = max(max_tasks, s.num_tasks)
        depths.append(s.depth)
        widths.append(s.width)

    click.echo(f"\n  DAGBench Statistics")
    click.echo(f"  ==================")
    click.echo(f"\n  Total workflows:   {len(workflows)}")
    click.echo(f"  Total tasks:       {total_tasks}")
    click.echo(f"  Total edges:       {total_edges}")
    click.echo(f"  Tasks range:       {min_tasks} - {max_tasks}")
    click.echo(f"  Avg tasks/workflow: {total_tasks/len(workflows):.1f}")
    click.echo(f"  Avg depth:         {sum(depths)/len(depths):.1f}")
    click.echo(f"  Avg width:         {sum(widths)/len(widths):.1f}")

    click.echo(f"\n  By Domain:")
    for d, count in domains.most_common():
        click.echo(f"    {d:<25} {count:>3}")

    click.echo(f"\n  By Completeness:")
    for c, count in completeness.most_common():
        click.echo(f"    {c:<25} {count:>3}")

    click.echo(f"\n  By Cost Model:")
    for cm, count in cost_models.most_common():
        click.echo(f"    {cm:<25} {count:>3}")
    click.echo()


@main.command()
@click.argument("workflow_id", required=False)
@click.option("--output-dir", "-o", default=None, type=click.Path(), help="Output directory for HTML files")
def docs(workflow_id, output_dir):
    """Generate HTML documentation for workflows."""
    from pathlib import Path
    from dagbench.catalog import _discover_workflow_dirs, get_workflow_path
    from dagbench.html_docs import generate_html

    if workflow_id:
        wf_dir = get_workflow_path(workflow_id)
        if wf_dir is None:
            click.echo(f"Workflow '{workflow_id}' not found.", err=True)
            raise SystemExit(1)
        dirs = [wf_dir]
    else:
        dirs = _discover_workflow_dirs()

    if not dirs:
        click.echo("No workflows found.")
        return

    for d in dirs:
        out = Path(output_dir) / d.name / "docs.html" if output_dir else None
        if out:
            out.parent.mkdir(parents=True, exist_ok=True)
        try:
            result = generate_html(d, out)
            click.echo(f"  {click.style('OK', fg='green')} {d.name} -> {result}")
        except Exception as e:
            click.echo(f"  {click.style('FAIL', fg='red')} {d.name}: {e}")

    click.echo(f"\nGenerated docs for {len(dirs)} workflow(s).")


if __name__ == "__main__":
    main()
