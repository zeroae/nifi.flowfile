"""Console script for nifi.flowfile."""

import sys
import click


@click.command()
def flowfile(args=None):
    """Console script for nifi.flowfile."""
    # fmt: off
    click.echo("Replace this message by putting your code into "
               "flowfile.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    # fmt: on
    return 0


if __name__ == "__main__":
    sys.exit(flowfile)  # pragma: no cover