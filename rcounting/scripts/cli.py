import click
from .log_thread import log
from .validate import validate
from .update_thread_directory import update_directory


@click.group(commands=[log, validate, update_directory],
             context_settings=dict(help_option_names=["-h", "--help"]))
def cli():
    pass


if __name__ == "__main__":
    cli()
