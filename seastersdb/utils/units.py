"""Provide tools to handle the units of ``pandas`` objects.

.. note::

   This modules relies on
   `Pint Python library <https://pint.readthedocs.io/en/stable/>`_, notably for parsing
   unit strings, making it quite **flexible**: e.g., "mm" is equivalent to
   "millimeter".
"""

import logging
from typing import Union, overload

import pandas as pd
from pint import UnitRegistry

__all__ = ["convert_dataframe_unit", "check_dataframe_unit"]

log = logging.getLogger(__name__)
ureg = UnitRegistry()

# TODO: Define okta
"""
Values in oktas:
CLR:00 None, SKC or CLR
FEW:01 One okta - 1/10 or less but not zero
FEW:02 Two oktas - 2/10 - 3/10, or FEW
SCT:03 Three oktas - 4/10
SCT:04 Four oktas - 5/10, or SCT
BKN:05 Five oktas - 6/10
BKN:06 Six oktas - 7/10 - 8/10
BKN:07 Seven oktas - 9/10 or more but not 10/10, or BKN
OVC:08 Eight oktas - 10/10, or OVC
VV:09 Sky obscured, or cloud amount cannot be estimated
X:10 Partial obscuration
"""


def _standard_unit(unit: str) -> str:
    """Return the unique string equivalent to ``unit``."""
    return str(ureg.Quantity(1, unit).units)


@overload
def convert_dataframe_unit(  # noqa: E704
    data: pd.DataFrame, target_unit: str
) -> pd.DataFrame: ...
@overload  # noqa: E302
def convert_dataframe_unit(  # noqa: E704
    data: pd.Series, target_unit: str
) -> pd.Series: ...


def convert_dataframe_unit(
    data: Union[pd.DataFrame, pd.Series], target_unit: str
) -> Union[pd.DataFrame, pd.Series]:
    """Convert units of a ``pandas`` object.

    This function reads the unit from ``data``'s 'units' attribute,
    performs the unit conversion, and updates the attribute to the new unit.

    Parameters
    ----------
    data
        DataFrame or Series with a 'units' attribute specifying its current units.
    target_unit
        Target unit to convert the data to (e.g., 'inch/day').

    Returns
    -------
    data_converted : Same type as ``data``
        A new DataFrame or Series with converted values and updated 'units' attribute.

    Raises
    ------
    ValueError
        If ``data`` has no 'units' attribute.
    Pint package errors.
    """

    source_unit = data.attrs.get("units", None)
    if source_unit is None:
        raise ValueError("No 'units' attribute found in the ``data``.")

    log.info(
        "Proceed to unit conversion: %s -> %s",
        _standard_unit(source_unit),
        _standard_unit(target_unit),
    )

    # Convert
    quantity = ureg.Quantity(data.values, source_unit)
    converted = quantity.to(target_unit).magnitude

    # Format
    if isinstance(data, pd.DataFrame):
        res = pd.DataFrame(converted, columns=data.columns, index=data.index)
    else:
        res = pd.Series(converted, index=data.index, name=data.name)

    # Attributes
    res.attrs = dict(data.attrs)
    res.attrs["units"] = target_unit

    return res


@overload
def check_dataframe_unit(  # noqa: E704
    data: pd.DataFrame, target_unit: str
) -> pd.DataFrame: ...
@overload  # noqa: E302
def check_dataframe_unit(  # noqa: E704
    data: pd.Series, target_unit: str
) -> pd.Series: ...


def check_dataframe_unit(
    data: Union[pd.DataFrame, pd.Series], target_unit: str
) -> Union[pd.DataFrame, pd.Series]:
    """
    Check if ``data``'s unit matches the target unit, and convert if necessary.

    This function compares a ``pandas`` object's 'units' attribute with a specified
    target unit. If they are the same, it returns the original ``data``. Otherwise, it
    converts it using :func:`convert_dataframe_unit`.

    Parameters
    ----------
    data
        DataFrame or Series with a 'units' attribute specifying its current units.
    target_unit
        The target unit to match or convert the data to (e.g., 'inch/day').

    Returns
    -------
    result : Same type as ``data``
        A DataFrame or Series with the target unit
        (either unchanged because already matching, or converted).

    Raises
    ------
    ValueError
        If ``data`` does not have a 'units' attribute.
    """
    source_unit = data.attrs.get("units", None)
    if source_unit is None:
        raise ValueError("No 'units' attribute found in ``data``.")

    source_u = _standard_unit(source_unit)
    target_u = _standard_unit(target_unit)
    if source_u == target_u:
        log.info(
            f"``data``'s unit equivalent to target unit ('{target_u}')."
            + " No conversion needed."
        )
        return data
    else:
        log.info("Conversion is needed.")
        return convert_dataframe_unit(data, target_unit)
