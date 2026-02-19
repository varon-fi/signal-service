"""Strategy package.

Importing this package registers built-in strategies with varon_fi.
"""

from signal_service.strategy import mtf_confluence  # noqa: F401
from signal_service.strategy import volatility_expansion  # noqa: F401
from signal_service.strategy import volume_range_breakout  # noqa: F401
from signal_service.strategy import momentum  # noqa: F401
from signal_service.strategy import atr_breakout  # noqa: F401
from signal_service.strategy import low_vol_momentum  # noqa: F401
