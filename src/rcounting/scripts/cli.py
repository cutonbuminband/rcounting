"""
The main entry point for the command line interface.
See the subcommands for details on their behaviour.
"""

import click

from .ftf import pin_or_create_ftf
from .log_all_side_threads import main
from .log_thread import log
from .update_thread_directory import update_directory
from .validate import validate
from .weekly_side_thread_stats import generate_stats_post


@click.group(
    commands=[
        log,
        validate,
        update_directory,
        pin_or_create_ftf,
        generate_stats_post,
        main,
    ],
    context_settings=dict(help_option_names=["-h", "--help"]),
)
@click.version_option()
def cli():
    pass
