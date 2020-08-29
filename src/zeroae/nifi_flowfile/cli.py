"""Console script for nifi.flowfile."""

import sys
import click


@click.command()
def nifi_flowfile(args=None):
    """Console script for nifi.flowfile."""
    # fmt: off
    click.echo("Replace this message by putting your code into "
               "nifi_flowfile.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    # fmt: on
    return 0


if __name__ == "__main__":
    sys.exit(nifi_flowfile)  # pragma: no cover