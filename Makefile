# Makefile for sftp2s3 project using uv

.PHONY: install install-dev install-uv format lint test run

# Install uv if not already installed
install-uv:
	@if ! command -v uv >/dev/null 2>&1; then \
		echo "uv not found. Installing uv..."; \
		python3 -m pip install --upgrade pip; \
		python3 -m pip install uv; \
	else \
		echo "uv already installed."; \
	fi

# Install only production dependencies
install: install-uv
	uv pip install -e .

# Install production + development dependencies
install-dev: install-uv
	uv pip install ".[dev]"

# Format code using Ruff
format:
	ruff format .

# Lint code using Ruff
lint:
	ruff check .

# Run tests with pytest
test:
	pytest

# Run the sftp2s3 sync
run:
	python sftp2s3.py --config-file ./config.conf

setup-config:
	cp config.example.yaml config.yaml