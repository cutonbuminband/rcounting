import logging


def setup(logger, verbose, quiet):
    logger.propagate = False
    logger.handlers.clear()
    # If quiet is true, only warnings are logged. Otherwise, at increasing
    # verbosity levels, both information and debug messages are logged
    logger.setLevel(30 - 10 * (1 - quiet) * (1 + verbose))
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(ch)
