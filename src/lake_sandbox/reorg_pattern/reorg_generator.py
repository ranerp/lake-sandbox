import typer


def delta_reorg(
    path: str = typer.Argument(..., help="Path to reorganize"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without making changes"),
) -> None:
    """Reorganize delta files in the specified path."""
    if dry_run:
        typer.echo(f"Dry run: Would reorganize delta files in {path}")
    else:
        typer.echo(f"Reorganizing delta files in {path}")