lint:
	isort .
	black .

check:
	flake8 .
	mypy pyseasters

doc:
	sphinx-build -b html docs docs/_build
