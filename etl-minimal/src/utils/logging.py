import logging

_FORMAT = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def configure_logging(level: int = logging.INFO) -> None:
    """Configure the root logger with a timestamped formatter and a stderr stream handler."""
    logging.basicConfig(
        level=level,
        format=_FORMAT,
        datefmt=_DATE_FORMAT,
        handlers=[logging.StreamHandler()],
    )
