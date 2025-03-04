"""
CLI module for validate command
"""
import json
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

import click
import yaml
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import box

from sparvi.validations.validator import run_validations, load_rules_from_file
from sparvi.validations.default_validations import get_default_validations

console = Console()


@click.command()
@click.argument("connection_string")
@click.argument("table_name")
@click.option(
    "--rules", "-r",
    help="Path to YAML file with validation rules",
    type=click.Path(exists=True, path_type=Path)
)
@click.option(
    "--output", "-o",
    help="Output file path for results (JSON)",
    type=click.Path(path_type=Path)
)
@click.option(
    "--generate-defaults", "-g",
    is_flag=True,
    help="Generate default validation rules"
)
@click.option(
    "--save-defaults", "-s",
    help="Save generated default rules to file",
    type=click.Path(path_type=Path)
)
@click.option(
    "--fail-on-error", "-f",
    is_flag=True,
    help="Exit with non-zero code if any validation fails"
)
@click.pass_context
def validate(
        ctx: click.Context,
        connection_string: str,
        table_name: str,
        rules: Optional[Path],
        output: Optional[Path],
        generate_defaults: bool,
        save_defaults: Optional[Path],
        fail_on_error: bool,
) -> None:
    """
    Validate a database table against rules.

    CONNECTION_STRING: Database connection string (e.g., duckdb:///path/to/db.duckdb)
    TABLE_NAME: Name of the table to validate
    """
    verbose = ctx.obj.get("verbose", False)
    validation_rules = []

    console.print(f"[bold blue]Validating table:[/bold blue] [green]{table_name}[/green]")
    console.print(f"[bold blue]Connection:[/bold blue] {connection_string}")

    # Generate default rules if requested
    if generate_defaults:
        with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}[/bold blue]"),
                console=console
        ) as progress:
            task = progress.add_task("Generating default validation rules...", total=None)

            try:
                default_rules = get_default_validations(connection_string, table_name)
                validation_rules.extend(default_rules)
                progress.update(task, completed=True, description=f"Generated {len(default_rules)} default rules")

                # Save the default rules if a path was provided
                if save_defaults:
                    save_defaults.parent.mkdir(parents=True, exist_ok=True)
                    with open(save_defaults, "w") as f:
                        yaml.dump({"rules": default_rules}, f, sort_keys=False, default_flow_style=False)
                    console.print(f"Default rules saved to: [bold green]{save_defaults}[/bold green]")

            except Exception as e:
                progress.update(task, completed=True, description="Rule generation failed!")
                console.print(f"[bold red]Error generating default rules:[/bold red] {e}")
                if verbose:
                    console.print_exception()
                return

    # Load rules from file if provided
    if rules:
        try:
            file_rules = load_rules_from_file(rules)
            console.print(f"Loaded [bold]{len(file_rules)}[/bold] rules from: {rules}")
            validation_rules.extend(file_rules)
        except Exception as e:
            console.print(f"[bold red]Error loading rules:[/bold red] {e}")
            if verbose:
                console.print_exception()
            return

    # Check if we have any rules to run
    if not validation_rules:
        console.print("[bold yellow]No validation rules provided.[/bold yellow]")
        console.print("Provide rules with --rules or generate defaults with --generate-defaults")
        return

    # Display a summary of rules to be executed
    _print_rules_summary(validation_rules)

    # Run validations
    with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}[/bold blue]"),
            console=console
    ) as progress:
        task = progress.add_task(f"Running {len(validation_rules)} validations...", total=None)

        try:
            results = run_validations(connection_string, validation_rules)
            progress.update(task, completed=True, description="Validation completed!")

        except Exception as e:
            progress.update(task, completed=True, description="Validation failed!")
            console.print(f"[bold red]Error running validations:[/bold red] {e}")
            if verbose:
                console.print_exception()
            sys.exit(1)

    # Display results
    _display_validation_results(results)

    # Calculate pass/fail counts
    passed = sum(1 for r in results if r.get("is_valid", False))
    failed = len(results) - passed

    # Show summary
    if failed > 0:
        console.print(
            f"\nValidation summary: [bold green]{passed} passed[/bold green], [bold red]{failed} failed[/bold red]")
    else:
        console.print(f"\nValidation summary: [bold green]All {passed} validations passed![/bold green]")

    # Save results to file if output path provided
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        with open(output, "w") as f:
            json.dump({"results": results}, f, indent=2)
        console.print(f"Validation results saved to: [bold green]{output}[/bold green]")

    # Exit with error code if any validation failed and fail-on-error is set
    if failed > 0 and fail_on_error:
        sys.exit(1)


def _print_rules_summary(rules: List[Dict[str, Any]]) -> None:
    """Display a summary of validation rules to be executed."""
    console.print(f"\n[bold]Validation Rules Summary ([green]{len(rules)}[/green] total):[/bold]")

    # Group rules by type
    rule_types = {}
    for rule in rules:
        rule_name = rule.get("name", "Unknown")
        rule_type = rule_name.split('_')[0] if '_' in rule_name else "other"
        rule_types[rule_type] = rule_types.get(rule_type, 0) + 1

    # Display as a small table
    rule_table = Table(show_header=False, box=box.SIMPLE)
    rule_table.add_column("Type", style="cyan")
    rule_table.add_column("Count", justify="right", style="green")

    for rule_type, count in sorted(rule_types.items(), key=lambda x: x[1], reverse=True):
        rule_table.add_row(rule_type, str(count))

    console.print(rule_table)


def _display_validation_results(results: List[Dict[str, Any]]) -> None:
    """Display validation results in a table format."""
    console.print("\n[bold]Validation Results:[/bold]")

    # Create a table for results
    table = Table(show_header=True)
    table.add_column("Rule", style="cyan")
    table.add_column("Status", justify="center")
    table.add_column("Expected", justify="right")
    table.add_column("Actual", justify="right")
    table.add_column("Description")

    for result in results:
        rule_name = result.get("rule_name", "Unknown rule")
        is_valid = result.get("is_valid", False)
        expected = json.dumps(result.get("expected_value", ""))

        # Handle error case separately
        if "error" in result:
            actual = f"[italic red]Error: {result['error']}[/italic red]"
        else:
            actual = json.dumps(result.get("actual_value", ""))

        description = result.get("description", "")

        status_style = "green bold" if is_valid else "red bold"
        status_text = "✓ PASS" if is_valid else "✗ FAIL"

        table.add_row(
            rule_name,
            f"[{status_style}]{status_text}[/{status_style}]",
            expected,
            actual,
            description
        )

    console.print(table)