lint:
	isort .
	black .

check:
	flake8 .
	mypy pyseasters

doc:
	find docs/_build/ -mindepth 1 -maxdepth 1 ! -name '.gitignore' -exec rm -rf {} + && \
	sphinx-build -b html docs docs/_build

doc-api:
	sphinx-apidoc -f -d1 -T -e -M -t docs/_templates -o docs/api/ pyseasters
	python docs/autosummary_modules.py

purge-server:
	fuser -k 8000/tcp >/dev/null 2>&1 || true

doc-serve: purge-server
	python -m http.server --directory docs/_build > /tmp/pyseasters_http.log 2>&1 & (sleep 2; python -m webbrowser "http://0.0.0.0:8000/")

doc-test: doc doc-serve

doc-full: doc-api doc

doc-full-test: doc-full doc-serve
