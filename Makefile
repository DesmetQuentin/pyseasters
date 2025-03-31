lint:
	isort .
	black .

check:
	flake8 .
	mypy pyseasters

apidoc:
	sphinx-apidoc -o docs pyseasters

doc: apidoc
	sphinx-build -b html docs docs/_build
