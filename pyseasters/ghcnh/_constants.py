import importlib.resources

import yaml

__all__ = ["ATTRIBUTES", "VAR_TO_META"]


ATTRIBUTES = [
    "_Measurement_Code",
    "_Quality_Code",
    "_Report_Type",
    "_Source_Code",
    "_Source_Station_ID",
]

with (
    importlib.resources.files("pyseasters.ghcnh.data")
    .joinpath("var_metadata.yaml")
    .open("r") as file
):
    VAR_TO_META = yaml.safe_load(file)
