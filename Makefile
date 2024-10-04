env:
	python3 -m venv .venv

make freeze:
	pip freeze > requirements.txt

install:
	pip install -r requirements.txt

uninstall:
	pip uninstall -r requirements.txt

run:
	python src/main.py

lint:
	pylint `git ls-files '*.py'`

format:
	black src/

all: env install lint
