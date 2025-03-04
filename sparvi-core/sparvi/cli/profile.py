"""
CLI module for profile command
"""
import json
import os
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import box

from sparvi.profiler.profile_engine import profile_table

console = Console()


@click.command()
@click.argument("connection_string")
@click.argument("table_name")
@click.option(
    "--output", "-o", help="Output file path (JSON)", type=click.Path(path_type=Path)
)
@click.option(
    "--compare", "-c", help="Compare with previous profile (JSON file)", type=click.Path(exists=True, path_type=Path)
)
@click.option(
    "--include-samples/--no-samples", default=True, help="Include data samples in output"
)
@click.option(
    "--format", "-f", type=click.Choice(["default", "detailed", "minimal"]), default="default",
    help="Output format style"
)
@click.pass_context
def profile(
        ctx: click.Context,
        connection_string: str,
        table_name: str,
        output: Optional[Path],
        compare: Optional[Path],
        include_samples: bool,
        format: str,
) -> None:
    """
    Profile a database table.

    CONNECTION_STRING: Database connection string (e.g., duckdb:///path/to/db.duckdb)
    TABLE_NAME: Name of the table to profile
    """
    verbose = ctx.obj.get("verbose", False)

    console.print(f"[bold blue]Profiling table:[/bold blue] [green]{table_name}[/green]")
    console.print(f"[bold blue]Connection:[/bold blue] {connection_string}")

    # Load previous profile for comparison if specified
    previous_profile = None
    if compare:
        try:
            with open(compare, "r") as f:
                previous_profile = json.load(f)
            console.print(f"[bold blue]Comparing with previous profile:[/bold blue] [green]{compare}[/green]")
        except Exception as e:
            console.print(f"[bold red]Error loading comparison file:[/bold red] {e}")
            return

    # Run the profiler with progress indicator
    with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}[/bold blue]"),
            console=console
    ) as progress:
        task = progress.add_task("Profiling data...", total=None)

        try:
            profile_results = profile_table(
                connection_string,
                table_name,
                previous_profile,
                include_samples=include_samples
            )
            progress.update(task, completed=True, description="Profile completed!")
        except Exception as e:
            progress.update(task, completed=True, description="Profile failed!")
            console.print(f"[bold red]Error profiling table:[/bold red] {e}")
            if verbose:
                console.print_exception()
            return

    # Display summary based on format
    if format == "minimal":
        _print_minimal_summary(profile_results)
    elif format == "detailed":
        _print_detailed_summary(profile_results, verbose)
    else:  # default format
        _print_default_summary(profile_results, verbose)

    # Save to file if output path provided
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        with open(output, "w") as f:
            json.dump(profile_results, f, indent=2)
        console.print(f"\nProfile saved to: [bold green]{output}[/bold green]")


def _print_minimal_summary(profile_results):
    """Print a minimal summary of profile results."""
    console.print(Panel(f"[bold]Profile Summary: {profile_results['table']}[/bold]"))

    # Basic stats
    stats_table = Table(show_header=False, box=box.SIMPLE)
    stats_table.add_column("Metric", style="cyan")
    stats_table.add_column("Value", style="green")

    stats_table.add_row("Timestamp", profile_results['timestamp'])
    stats_table.add_row("Row count", str(profile_results['row_count']))
    stats_table.add_row("Columns", str(len(profile_results.get('completeness', {}))))
    stats_table.add_row("Duplicate rows", str(profile_results.get('duplicate_count', 0)))

    # Add anomaly counts if present
    anomalies = profile_results.get('anomalies', [])
    if anomalies:
        stats_table.add_row("Anomalies detected", f"[bold red]{len(anomalies)}[/bold red]")

    # Add schema shift counts if present
    schema_shifts = profile_results.get('schema_shifts', [])
    if schema_shifts:
        stats_table.add_row("Schema shifts", f"[bold yellow]{len(schema_shifts)}[/bold yellow]")

    console.print(stats_table)


def _print_default_summary(profile_results, verbose=False):
    """Print a standard summary of profile results."""
    console.print(Panel(f"[bold]Profile Summary: {profile_results['table']}[/bold]"))

    # Basic stats table
    stats_table = Table(show_header=False, box=box.SIMPLE)
    stats_table.add_column("Metric", style="cyan")
    stats_table.add_column("Value", style="green")

    stats_table.add_row("Timestamp", profile_results['timestamp'])
    stats_table.add_row("Row count", str(profile_results['row_count']))
    stats_table.add_row("Columns", str(len(profile_results.get('completeness', {}))))
    stats_table.add_row("Duplicate rows", str(profile_results.get('duplicate_count', 0)))

    console.print(stats_table)

    # Column completeness table
    console.print("\n[bold cyan]Column Completeness:[/bold cyan]")

    completeness_table = Table(show_header=True, box=box.SIMPLE)
    completeness_table.add_column("Column", style="cyan")
    completeness_table.add_column("NULL %", justify="right")
    completeness_table.add_column("Distinct %", justify="right")
    completeness_table.add_column("Type", style="dim")

    # Determine column types
    col_types = {}
    for col in profile_results.get('completeness', {}).keys():
        if col in profile_results.get('numeric_stats', {}):
            col_types[col] = "numeric"
        elif col in profile_results.get('date_stats', {}):
            col_types[col] = "date/time"
        elif col in profile_results.get('text_length_stats', {}):
            col_types[col] = "text"
        else:
            col_types[col] = "unknown"

    # Add rows for each column (sorted by null percentage)
    sorted_columns = sorted(
        profile_results.get('completeness', {}).items(),
        key=lambda x: x[1]['null_percentage'],
        reverse=True
    )

    for col, stats in sorted_columns:
        null_style = "green"
        if stats['null_percentage'] > 50:
            null_style = "red bold"
        elif stats['null_percentage'] > 20:
            null_style = "yellow"

        completeness_table.add_row(
            col,
            f"[{null_style}]{stats['null_percentage']}%[/{null_style}]",
            f"{stats['distinct_percentage']}%",
            col_types.get(col, "")
        )

    console.print(completeness_table)

    # Print anomalies if any
    anomalies = profile_results.get('anomalies', [])
    if anomalies:
        console.print(f"\n[bold red]Anomalies Detected ({len(anomalies)}):[/bold red]")
        anomaly_table = Table(show_header=True, box=box.SIMPLE)
        anomaly_table.add_column("Type", style="cyan")
        anomaly_table.add_column("Description")
        anomaly_table.add_column("Severity", justify="right")

        for anomaly in anomalies:
            severity_style = "green"
            if anomaly.get('severity') == 'high':
                severity_style = "red bold"
            elif anomaly.get('severity') == 'medium':
                severity_style = "yellow"

            anomaly_table.add_row(
                anomaly.get('type', 'unknown'),
                anomaly.get('description', ''),
                f"[{severity_style}]{anomaly.get('severity', 'low')}[/{severity_style}]"
            )

        console.print(anomaly_table)

    # Print schema shifts if any
    schema_shifts = profile_results.get('schema_shifts', [])
    if schema_shifts:
        console.print(f"\n[bold yellow]Schema Shifts Detected ({len(schema_shifts)}):[/bold yellow]")
        schema_table = Table(show_header=True, box=box.SIMPLE)
        schema_table.add_column("Type", style="cyan")
        schema_table.add_column("Column")
        schema_table.add_column("Description")

        for shift in schema_shifts:
            schema_table.add_row(
                shift.get('type', 'unknown'),
                shift.get('column', ''),
                shift.get('description', '')
            )

        console.print(schema_table)


def _print_detailed_summary(profile_results, verbose=False):
    """Print a detailed summary of profile results."""
    console.print(Panel(f"[bold]Detailed Profile: {profile_results['table']}[/bold]"))

    # Basic stats table
    stats_table = Table(show_header=False, box=box.SIMPLE)
    stats_table.add_column("Metric", style="cyan")
    stats_table.add_column("Value", style="green")

    stats_table.add_row("Timestamp", profile_results['timestamp'])
    stats_table.add_row("Row count", str(profile_results['row_count']))
    stats_table.add_row("Columns", str(len(profile_results.get('completeness', {}))))
    stats_table.add_row("Duplicate rows", str(profile_results.get('duplicate_count', 0)))

    console.print(stats_table)

    # Column completeness table
    console.print("\n[bold cyan]Column Completeness:[/bold cyan]")

    completeness_table = Table(show_header=True)
    completeness_table.add_column("Column", style="cyan")
    completeness_table.add_column("Nulls", justify="right")
    completeness_table.add_column("NULL %", justify="right")
    completeness_table.add_column("Distinct", justify="right")
    completeness_table.add_column("Distinct %", justify="right")
    completeness_table.add_column("Type", style="dim")

    # Determine column types
    col_types = {}
    for col in profile_results.get('completeness', {}).keys():
        if col in profile_results.get('numeric_stats', {}):
            col_types[col] = "numeric"
        elif col in profile_results.get('date_stats', {}):
            col_types[col] = "date/time"
        elif col in profile_results.get('text_length_stats', {}):
            col_types[col] = "text"
        else:
            col_types[col] = "unknown"

    # Add rows for each column
    for col, stats in profile_results.get('completeness', {}).items():
        null_style = "green"
        if stats['null_percentage'] > 50:
            null_style = "red bold"
        elif stats['null_percentage'] > 20:
            null_style = "yellow"

        completeness_table.add_row(
            col,
            str(stats['nulls']),
            f"[{null_style}]{stats['null_percentage']}%[/{null_style}]",
            str(stats['distinct_count']),
            f"{stats['distinct_percentage']}%",
            col_types.get(col, "")
        )

    console.print(completeness_table)

    # Print numeric stats if any
    numeric_stats = profile_results.get('numeric_stats', {})
    if numeric_stats:
        console.print("\n[bold cyan]Numeric Column Statistics:[/bold cyan]")
        numeric_table = Table(show_header=True)
        numeric_table.add_column("Column", style="cyan")
        numeric_table.add_column("Min", justify="right")
        numeric_table.add_column("Max", justify="right")
        numeric_table.add_column("Average", justify="right")
        numeric_table.add_column("Median", justify="right")
        numeric_table.add_column("StdDev", justify="right")

        for col, stats in numeric_stats.items():
            numeric_table.add_row(
                col,
                str(stats.get('min', 'N/A')),
                str(stats.get('max', 'N/A')),
                f"{stats.get('avg', 'N/A')}" if stats.get('avg') is None else f"{float(stats.get('avg', 0)):.2f}",
                f"{stats.get('median', 'N/A')}" if stats.get(
                    'median') is None else f"{float(stats.get('median', 0)):.2f}",
                f"{stats.get('stdev', 'N/A')}" if stats.get('stdev') is None else f"{float(stats.get('stdev', 0)):.2f}"
            )

        console.print(numeric_table)

    # Print text stats if any
    text_stats = profile_results.get('text_length_stats', {})
    if text_stats:
        console.print("\n[bold cyan]Text Column Statistics:[/bold cyan]")
        text_table = Table(show_header=True)
        text_table.add_column("Column", style="cyan")
        text_table.add_column("Min Length", justify="right")
        text_table.add_column("Max Length", justify="right")
        text_table.add_column("Avg Length", justify="right")

        for col, stats in text_stats.items():
            text_table.add_row(
                col,
                str(stats.get('min_length', 'N/A')),
                str(stats.get('max_length', 'N/A')),
                f"{stats.get('avg_length', 'N/A')}" if stats.get(
                    'avg_length') is None else f"{float(stats.get('avg_length', 0)):.1f}"
            )

        console.print(text_table)

    # Print date stats if any
    date_stats = profile_results.get('date_stats', {})
    if date_stats:
        console.print("\n[bold cyan]Date Column Statistics:[/bold cyan]")
        date_table = Table(show_header=True)
        date_table.add_column("Column", style="cyan")
        date_table.add_column("Min Date")
        date_table.add_column("Max Date")
        date_table.add_column("Distinct Values", justify="right")
        date_table.add_column("Date Range (days)", justify="right")

        for col, stats in date_stats.items():
            date_table.add_row(
                col,
                str(stats.get('min_date', 'N/A')),
                str(stats.get('max_date', 'N/A')),
                str(stats.get('distinct_count', 'N/A')),
                str(stats.get('date_range_days', 'N/A'))
            )

        console.print(date_table)

    # Print outliers if any
    outliers = profile_results.get('outliers', {})
    if outliers and verbose:
        console.print("\n[bold cyan]Outliers Detected:[/bold cyan]")
        for col, values in outliers.items():
            console.print(f"  [bold]{col}[/bold]: {', '.join(str(v) for v in values)}")

    # Print anomalies if any
    anomalies = profile_results.get('anomalies', [])
    if anomalies:
        console.print(f"\n[bold red]Anomalies Detected ({len(anomalies)}):[/bold red]")
        anomaly_table = Table(show_header=True)
        anomaly_table.add_column("Type", style="cyan")
        anomaly_table.add_column("Description")
        anomaly_table.add_column("Column", style="dim")
        anomaly_table.add_column("Severity", justify="right")

        for anomaly in anomalies:
            severity_style = "green"
            if anomaly.get('severity') == 'high':
                severity_style = "red bold"
            elif anomaly.get('severity') == 'medium':
                severity_style = "yellow"

            anomaly_table.add_row(
                anomaly.get('type', 'unknown'),
                anomaly.get('description', ''),
                anomaly.get('column', '-'),
                f"[{severity_style}]{anomaly.get('severity', 'low')}[/{severity_style}]"
            )

        console.print(anomaly_table)

    # Print schema shifts if any
    schema_shifts = profile_results.get('schema_shifts', [])
    if schema_shifts:
        console.print(f"\n[bold yellow]Schema Shifts Detected ({len(schema_shifts)}):[/bold yellow]")
        schema_table = Table(show_header=True)
        schema_table.add_column("Type", style="cyan")
        schema_table.add_column("Column")
        schema_table.add_column("Description")
        schema_table.add_column("Severity", justify="right")

        for shift in schema_shifts:
            severity_style = "green"
            if shift.get('severity') == 'high':
                severity_style = "red bold"
            elif shift.get('severity') == 'medium':
                severity_style = "yellow"

            schema_table.add_row(
                shift.get('type', 'unknown'),
                shift.get('column', ''),
                shift.get('description', ''),
                f"[{severity_style}]{shift.get('severity', 'low')}[/{severity_style}]"
            )

        console.print(schema_table)

    # Print samples if available and verbose
    samples = profile_results.get('samples', [])
    if samples and verbose:
        console.print("\n[bold cyan]Sample Data (first 5 rows):[/bold cyan]")
        if samples:
            # Create sample table with columns from first row
            columns = list(samples[0].keys())
            sample_table = Table(show_header=True)
            for col in columns:
                sample_table.add_column(col)

            # Add sample rows (max 5)
            for i, row in enumerate(samples[:5]):
                sample_table.add_row(*[str(row.get(col, '')) for col in columns])

            console.print(sample_table)

            if len(samples) > 5:
                console.print(f"[dim]...and {len(samples) - 5} more rows (total: {len(samples)})[/dim]")