from math import atan

# THIS CAN BE CONFIGURED
BX_UNIT     = 32                        # TDC / BX
BXTIME_UNIT = 25                        # ns  / BX
OB_UNIT     = 3564                      # BX  / OB

# THIS IS FIXED
TDCTIME_UNIT = BXTIME_UNIT / BX_UNIT    # ns  / TDC
BX_MAXDIFF   = 16                       # BX  counts
TDC_MAXDIFF  = BX_MAXDIFF * BX_UNIT     # TDC counts

# SL CONSTRUNCTION
MAX_CELLS   = 16
CELL_HEIGHT = 13                            # mm
CELL_WIDTH  = 42                            # mm
TAN_MAX     = CELL_WIDTH / CELL_HEIGHT      # u.a.
THETA_MAX   = atan(CELL_WIDTH/CELL_HEIGHT)  # rad
V_DRIFT     = 54.5e-3                       # mm / ns 
