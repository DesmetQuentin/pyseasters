import importlib.resources

import yaml

__all__ = ["VAR_TO_META"]

with importlib.resources.files("pyseasters.ghcnd.data").joinpath(
    "var_metadata.yaml"
).open("r") as file:
    VAR_TO_META = yaml.safe_load(file)
