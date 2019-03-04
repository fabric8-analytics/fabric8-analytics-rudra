"""Initialize the ml_utils package."""

import datetime
import os
import logging
import daiquiri

DEBUG = os.getenv('DEBUG', False) == 'true'

formatter = daiquiri.formatter.ColorExtrasFormatter(
    fmt=(daiquiri.formatter.DEFAULT_EXTRAS_FORMAT +
         " [%(filename)s:%(lineno)s F:%(funcName)s()]"))

daiquiri.setup(
    level=logging.DEBUG if DEBUG else logging.ERROR,
    outputs=(
        daiquiri.output.TimedRotatingFile('/tmp/rudra.errors.log',
                                          level=logging.WARNING,
                                          interval=datetime.timedelta(hours=48)),
        daiquiri.output.Stream(formatter=formatter)
    )
)

logger = daiquiri.getLogger(__name__)
