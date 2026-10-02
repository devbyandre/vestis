import pandas as pd

import db_utils as db


def store_fx_rates(df: pd.DataFrame):
    """Pass-through middleware function for FX rates."""
    db.store_fx_rates(df)
