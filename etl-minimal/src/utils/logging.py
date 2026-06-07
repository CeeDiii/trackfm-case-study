import logging

_FORMAT = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format=_FORMAT,
        datefmt=_DATE_FORMAT,
        handlers=[logging.StreamHandler()],
    )
