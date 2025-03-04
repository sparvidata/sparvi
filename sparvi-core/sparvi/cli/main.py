import os
import sys
from typing import Optional

import click
from rich.console import Console

from sparvi.version import __version__
from sparvi.cli.profile import profile
from sparvi.cli.validate import validate

console = Console()


@click.group(help="Sparvi CLI - Data profiling and validation")
@click.version_option(version=__version__)
@click.option(
    "--verbose", "-v", is_flag=True, help="Enable verbose output", default=False
)
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """Main CLI entrypoint for Sparvi."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


# Add subcommands
cli.add_command(profile)
cli.add_command(validate)


@cli.command()
def info() -> None:
    """Display information about the Sparvi installation."""
    console.print("[bold]Sparvi Core[/bold]", f"version {__version__}")
    console.print("\n[bold]System Information:[/bold]")
    console.print(f"  Python version: {sys.version.split()[0]}")
    console.print(f"  Platform: {sys.platform}")


if __name__ == "__main__":
    cli()