"""
Adaptive lateness processor entry point for the BDA experiment matrix.

Delegates to the unified StreamRunner so that every mechanism shares
the same core processor and measurement methodology.

Mechanisms:
  cmix     - baseline, no finalization
  fixed    - fixed allowed-lateness watermark + finalize/reconcile
  aloa     - ALOA adaptive budget + finalize/reconcile
  maso     - ALOA + memory-aware tier organization (MASO)
  earm     - ALOA + MASO + guarded retention (EARM)
  full     - ALOA + MASO + EARM (safe adaptive guard)
  earm_agg - like full but with a small fixed guard (trade-off probe)
"""

import sys

from src.bda.cmix.runner import main


if __name__ == "__main__":
    sys.exit(main())