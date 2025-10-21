# MoneyBin Development Makefile
# This Makefile provides development commands for the MoneyBin project

.PHONY: help setup clean install install-dev test test-cov lint format type-check pre-commit venv activate status install-uv

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
RESET := \033[0m

# Python and virtual environment settings
PYTHON := python3
VENV_DIR := .venv
VENV_ACTIVATE := $(VENV_DIR)/bin/activate

# Python environment settings
ifdef VIRTUAL_ENV
    PYTHON_ENV := $(PYTHON)
else
    PYTHON_ENV := $(VENV_DIR)/bin/python
endif

help: ## Show this help message
	@echo "$(BLUE)MoneyBin Development Commands$(RESET)"
	@echo ""
	@echo "$(GREEN)Setup & Installation:$(RESET)"
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z_-]+:.*?## / && /Setup|Install|Environment/ {printf "  $(YELLOW)%-20s$(RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(GREEN)Development:$(RESET)"
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z_-]+:.*?## / && /Development|Code|Format|Lint|Type|Test/ {printf "  $(YELLOW)%-20s$(RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""

	@echo "$(GREEN)Utility:$(RESET)"
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z_-]+:.*?## / && /Clean|Status|Utility/ {printf "  $(YELLOW)%-20s$(RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(BLUE)Quick Start:$(RESET)"
	@echo "  make setup          # Complete development environment setup"
	@echo "  make status         # Check environment status"
	@echo ""
	@echo "$(BLUE)Development Workflow:$(RESET)"
	@echo "  make test              # Run tests (recommended)"
	@echo "  make check             # Format and validate code (recommended)"
	@echo "  make dagster-dev       # Start Dagster server"
	@echo "  make update-deps       # Update all dependencies"
	@echo ""
	@echo "$(BLUE)Dependency Management:$(RESET)"
	@echo "  make sync           # Sync from lockfile (fast, reproducible)"
	@echo "  make lock           # Generate/update lockfile"
	@echo "  make clean          # Clean environment and start fresh"

check-python: ## Setup & Installation: Verify Python installation
	@echo "$(BLUE)🐍 Checking Python installation...$(RESET)"
	@if command -v python3 >/dev/null 2>&1; then \
		echo "$(GREEN)✅ Python 3 is available$(RESET)"; \
	else \
		echo "$(RED)❌ Python 3 is not available$(RESET)"; \
		echo "Please install Python 3 and try again."; \
		exit 1; \
	fi

setup: check-python venv lock sync pre-commit ## Setup & Installation: Complete development environment setup
	@echo "$(GREEN)🎉 Setup complete! Your MoneyBin development environment is ready.$(RESET)"
	@echo ""
	@echo "$(BLUE)Next steps:$(RESET)"
	@echo "  make test                 # Run tests"
	@echo "  make check                # Format and validate code"
	@echo "  make dagster-dev          # Start Dagster development server"
	@echo "  moneybin extract plaid    # Extract financial data"
	@echo ""
	@echo "$(BLUE)Useful commands:$(RESET)"
	@echo "  make status               # Check environment status"
	@echo "  make update-deps          # Update dependencies"
	@echo "  make help                 # Show all available commands"


venv: $(VENV_ACTIVATE) ## Setup & Installation: Create virtual environment

$(VENV_ACTIVATE):
	@echo "$(BLUE)🚀 Setting up MoneyBin development environment...$(RESET)"
	@if [ ! -d "$(VENV_DIR)" ]; then \
		if [ -f ".python-version" ]; then \
			required_python=$$(cat .python-version); \
			echo "$(BLUE)🐍 Creating virtual environment with Python $$required_python...$(RESET)"; \
		else \
			required_python="3.11"; \
			echo "$(BLUE)🐍 Creating virtual environment with Python $$required_python or higher...$(RESET)"; \
		fi; \
		if command -v uv >/dev/null 2>&1; then \
			uv venv $(VENV_DIR) --python $$required_python; \
		else \
			echo "$(YELLOW)⚠️  uv not available, installing first...$(RESET)"; \
			curl -LsSf https://astral.sh/uv/install.sh | sh; \
			export PATH="$$HOME/.local/bin:$$PATH"; \
			uv venv $(VENV_DIR) --python $$required_python; \
		fi; \
		echo "$(GREEN)✅ Virtual environment created$(RESET)"; \
	else \
		echo "$(GREEN)✅ Virtual environment already exists$(RESET)"; \
	fi


sync: venv ## Setup & Installation: Sync dependencies from lockfile (modern, reproducible)
	@echo "$(BLUE)🔄 Syncing dependencies from lockfile...$(RESET)"
	@uv sync --extra dev
	@echo "$(GREEN)✅ Dependencies synchronized from lockfile$(RESET)"

sync-prod: venv ## Setup & Installation: Sync production dependencies only
	@echo "$(BLUE)🔄 Syncing production dependencies...$(RESET)"
	@uv sync
	@echo "$(GREEN)✅ Production dependencies synchronized from lockfile$(RESET)"

update-deps: venv ## Setup & Installation: Update all dependencies to latest versions
	@echo "$(BLUE)🔄 Updating all dependencies to latest versions...$(RESET)"
	@uv lock --upgrade
	@uv sync --extra dev
	@echo "$(GREEN)✅ All dependencies updated and synchronized$(RESET)"

lock: venv ## Setup & Installation: Generate/update lockfile without installing
	@echo "$(BLUE)🔒 Generating lockfile...$(RESET)"
	@uv lock
	@echo "$(GREEN)✅ Lockfile updated$(RESET)"

pre-commit: venv ## Setup & Installation: Install pre-commit hooks
	@echo "$(BLUE)🔒 Installing pre-commit hooks...$(RESET)"
	@uv run pre-commit install
	@echo "$(GREEN)✅ Pre-commit hooks installed$(RESET)"
	@echo "$(BLUE)ℹ️  Pre-commit will use uv run for consistent tool versions$(RESET)"

test: venv ## Development: Run tests
	@echo "$(BLUE)🧪 Running tests...$(RESET)"
	@uv run pytest tests/

test-cov: venv ## Development: Run tests with coverage report
	@echo "$(BLUE)🧪 Running tests with coverage...$(RESET)"
	@uv run pytest --cov=src tests/
	@echo "$(BLUE)📊 Coverage report generated$(RESET)"

test-unit: venv ## Development: Run unit tests only
	@echo "$(BLUE)🧪 Running unit tests...$(RESET)"
	@uv run pytest tests/ -m "unit"

test-integration: venv ## Development: Run integration tests only
	@echo "$(BLUE)🧪 Running integration tests...$(RESET)"
	@uv run pytest tests/ -m "integration"

format: venv ## Development: Format code with ruff
	@echo "$(BLUE)🎨 Formatting code with ruff...$(RESET)"
	@uv run ruff format .
	@echo "$(BLUE)🔧 Fixing auto-fixable issues...$(RESET)"
	@uv run ruff check --fix .
	@echo "$(BLUE)🔧 Fixing whitespace and file ending issues...$(RESET)"
	@uv run pre-commit run trailing-whitespace --all-files || true
	@uv run pre-commit run end-of-file-fixer --all-files || true
	@echo "$(GREEN)✅ Code formatted and fixed$(RESET)"

lint: venv ## Development: Lint code with ruff
	@echo "$(BLUE)🔍 Linting code with ruff...$(RESET)"
	@uv run ruff check .
	@echo "$(GREEN)✅ Linting complete$(RESET)"

type-check: venv ## Development: Type check with pyright
	@echo "$(BLUE)🔍 Type checking with pyright...$(RESET)"
	@uv run pyright
	@echo "$(GREEN)✅ Type checking complete$(RESET)"

check: format lint type-check ## Development: Run all code quality checks
	@echo "$(GREEN)✅ All code quality checks complete$(RESET)"

jupyter: venv ## Development: Start Jupyter notebook server
	@echo "$(BLUE)📓 Starting Jupyter notebook server...$(RESET)"
	@uv run jupyter notebook notebooks/

dagster-dev: venv ## Development: Start Dagster development server
	@echo "$(BLUE)🚀 Starting Dagster development server...$(RESET)"
	@cd pipelines && uv run dagster dev

clean-cache: ## Utility: Clean Python cache files
	@echo "$(BLUE)🧹 Cleaning Python cache files...$(RESET)"
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)✅ Cache files cleaned$(RESET)"

clean-venv: ## Utility: Remove virtual environment
	@echo "$(BLUE)🧹 Removing virtual environment...$(RESET)"
	@rm -rf $(VENV_DIR)
	@echo "$(GREEN)✅ Virtual environment removed$(RESET)"



clean: clean-cache clean-venv ## Utility: Clean all generated files and virtual environment
	@echo "$(GREEN)✅ All clean!$(RESET)"

status: ## Utility: Show development environment status
	@echo "$(BLUE)MoneyBin Development Environment Status$(RESET)"
	@echo ""
	@echo "$(GREEN)Python Environment:$(RESET)"
	@if [ -d "$(VENV_DIR)" ]; then \
		echo "  Virtual Environment: $(GREEN)✅ Created$(RESET)"; \
		venv_python=$$($(VENV_DIR)/bin/python --version 2>&1); \
		echo "  Python Version: $$venv_python"; \
		if [ -f "$(VENV_DIR)/pyvenv.cfg" ]; then \
			echo "  Location: $(VENV_DIR)"; \
		fi; \
	else \
		echo "  Virtual Environment: $(RED)❌ Not found$(RESET)"; \
	fi
	@echo ""
	@echo "$(GREEN)Package Management (uv):$(RESET)"
	@if command -v uv >/dev/null 2>&1; then \
		uv_version=$$(uv --version 2>/dev/null | head -n1); \
		echo "  System uv: $(GREEN)✅ $$uv_version$(RESET)"; \
	else \
		echo "  System uv: $(RED)❌ Not available$(RESET)"; \
	fi

	@if [ -f "uv.lock" ]; then \
		echo "  uv.lock: $(GREEN)✅ Found$(RESET)"; \
	else \
		echo "  uv.lock: $(YELLOW)⚠️  Not found - run 'make lock' to create$(RESET)"; \
	fi
	@if [ -f "pyproject.toml" ]; then \
		echo "  pyproject.toml: $(GREEN)✅ Found$(RESET)"; \
	else \
		echo "  pyproject.toml: $(RED)❌ Not found$(RESET)"; \
	fi
	@echo ""
	@echo "$(GREEN)Python Version Management:$(RESET)"
	@if [ -f ".python-version" ]; then \
		required_version=$$(cat .python-version); \
		echo "  .python-version: $(GREEN)✅ Found ($$required_version)$(RESET)"; \
	else \
		echo "  .python-version: $(YELLOW)⚠️  Not found (will use 3.11+)$(RESET)"; \
	fi
	@if command -v python3 >/dev/null 2>&1; then \
		system_python=$$(python3 --version 2>&1); \
		echo "  System Python: $(GREEN)✅ $$system_python$(RESET)"; \
	else \
		echo "  System Python: $(RED)❌ Not available$(RESET)"; \
	fi
	@echo ""
	@echo "$(GREEN)Installed Packages:$(RESET)"
	@if [ -d "$(VENV_DIR)" ]; then \
		echo "  Core packages:"; \
		uv pip list 2>/dev/null | grep -E "(dagster|dbt-core|duckdb|polars|ruff|pytest|pyright)" | sed 's/^/    /' || echo "    $(YELLOW)⚠️  No core packages found$(RESET)"; \
	else \
		echo "  $(YELLOW)⚠️  Virtual environment not found$(RESET)"; \
	fi
