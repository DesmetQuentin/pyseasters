import logging

import pandas as pd
import pint

__all__ = ["convert_dataframe_unit", "check_dataframe_unit"]

log = logging.getLogger(__name__)
ureg = pint.UnitRegistry()


def _standard_unit(unit: str) -> str:
    """Return the unique string equivalent to ``unit``."""
    return str(ureg.Quantity(1, unit).units)


def convert_dataframe_unit(df: pd.DataFrame, target_unit: str) -> pd.DataFrame:
    """Convert units of a pandas DataFrame using the pint library.

    This function reads the unit from the DataFrame's 'units' attribute,
    performs the unit conversion, and updates the attribute to the new unit.

    Args:
        df: DataFrame with a 'units' attribute specifying its current unit.

        target_unit: Target unit to convert the data to (e.g., 'inch/day').

    Returns:
        A new DataFrame with converted values and updated 'units' attribute.

    Raises:
        ValueError: If the DataFrame has no 'units' attribute.

        Pint package errors.
    """

    source_unit = df.attrs.get("units", None)
    if source_unit is None:
        raise ValueError("No 'units' attribute found in the DataFrame.")

    log.info(
        "Proceed to unit conversion: %s -> %s",
        _standard_unit(source_unit),
        _standard_unit(target_unit),
    )
    quantity = df * ureg(source_unit)
    df_converted = pd.DataFrame(
        quantity.to(target_unit).magnitude, columns=df.columns, index=df.index  # type: ignore[operator]
    )
    df_converted.attrs["units"] = target_unit

    return df_converted


def check_dataframe_unit(df: pd.DataFrame, target_unit: str) -> pd.DataFrame:
    """
    Check if the DataFrame's unit matches the target unit, and convert if necessary.

    This function compares the DataFrame's 'units' attribute with a specified target
    unit. If they are the same, it returns the original DataFrame. Otherwise, it
    converts the data using ``convert_dataframe_unit()`` and updates the 'units'
    attribute.

    Args:
        df: The input DataFrame with a 'units' attribute in ``df.attrs``.

        target_unit: The desired unit to match or convert to.

    Returns:
        A DataFrame with the target unit, either unchanged or converted.

    Raises:
        ValueError: If the DataFrame does not have a 'units' attribute.
    """
    source_unit = df.attrs.get("units", None)
    if source_unit is None:
        raise ValueError("No 'units' attribute found in the DataFrame.")

    source_u = _standard_unit(source_unit)
    target_u = _standard_unit(target_unit)
    if source_u == target_u:
        log.info(
            f"DataFrame's unit equivalent to target unit ('{target_u}')."
            + " No conversion needed."
        )
        return df
    else:
        log.info("Conversion is needed.")
        return convert_dataframe_unit(df, target_unit)
