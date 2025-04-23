# tests/test_units.py

import pandas as pd
import pytest
from pint import UnitRegistry

from pyseasters.utils.units import (
    _standard_unit,
    check_dataframe_unit,
    convert_dataframe_unit,
)

ureg = UnitRegistry()


class TestStandardUnit:
    def test(self):
        """Single test for this function."""
        assert _standard_unit("m") == _standard_unit("meter")
        assert _standard_unit("kg/day") == _standard_unit("kilogram / day")


class TestConvertDataframeUnit:
    def unit_dataframe(self, set_units=True):
        """Return a realistic DataFrame with the optional 'units' attribute."""
        df = pd.DataFrame([[10], [20]], columns=["value"])
        if set_units:
            df.attrs["units"] = "mm/day"
        return df

    def test(self):
        """Test nominal behavior."""
        df = self.unit_dataframe()
        converted = convert_dataframe_unit(df, "inch/day")
        expected = (df.values * ureg("mm/day")).to("inch/day").magnitude
        assert pytest.approx(converted.values) == expected
        assert converted.attrs["units"] == "inch/day"

    def test_error_missing_units(self):
        """Test error raising when the 'units' attribute is missing."""
        df = self.unit_dataframe(set_units=False)
        with pytest.raises(ValueError):
            convert_dataframe_unit(df, "inch/day")


class TestCheckDataframeUnit:
    def unit_dataframe(self, set_units=True):
        """Return a realistic DataFrame with the optional 'units' attribute."""
        df = pd.DataFrame([[10], [20]], columns=["value"])
        if set_units:
            df.attrs["units"] = "mm/day"
        return df

    def test_no_conversion_needed(self):
        """Test when no conversion is needed."""
        df = self.unit_dataframe()
        assert check_dataframe_unit(df, "millimeter/day").equals(df)

    def test_conversion_is_needed(self):
        """Test when conversion is needed."""
        df = self.unit_dataframe()
        result = check_dataframe_unit(df, "inch/day")
        expected = (df.values * ureg("mm/day")).to("inch/day").magnitude
        assert pytest.approx(result.values) == expected
        assert result.attrs["units"] == "inch/day"

    def test_error_missing_units(self):
        """Test error raising when the 'units' attribute is missing."""
        df = self.unit_dataframe(set_units=False)
        with pytest.raises(ValueError):
            check_dataframe_unit(df, "inch/day")
