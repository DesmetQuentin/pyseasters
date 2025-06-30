import re
from pathlib import Path

LOC_DIR = Path(__name__).resolve().parent
API_DIR = LOC_DIR / "docs" / "api"
JOIN_STRING = "\n      "
EXCLUDE = ["paths", "COUNTRIES"]

# Loop over generated API rst file paths
for rst_path in API_DIR.glob("*.rst"):

    # Resolve python file path
    stem = rst_path.stem  # module name without .rst
    py_path = LOC_DIR / Path(*stem.split("."))  # path from module
    py_path = py_path.with_suffix(".py")  # add .py extension

    if not py_path.exists():
        continue

    # Read source code
    with open(py_path, "r") as file:
        content = file.read()

    # Look for __all__ = [...]
    match = re.search(r"__all__\s*=\s*\[(.*?)\]", content, re.DOTALL)
    if not match:
        continue

    # Retrieve the list of function names
    list_string = match.group(1)
    functions = [f for f in re.findall(r'"([^"]+)"', list_string) if f not in EXCLUDE]
    if not functions:
        continue

    # Append autosummary directive
    with open(rst_path, "a") as file:
        file.write(
            f"""
   .. rubric:: Functions

   .. autosummary::
      :toctree:
      :signatures: short

      {JOIN_STRING.join(functions)}
"""  # noqa: E231, E241, E203
        )
