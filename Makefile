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

# Check if virtual environment is active
ifdef VIRTUAL_ENV
    PYTHON_ENV := $(PYTHON)
    VENV_UV := $(VENV_DIR)/bin/uv
    UV_PIP_INSTALL := uv pip install
    UV_PIP_ARGS :=
else
    PYTHON_ENV := $(VENV_DIR)/bin/python
    VENV_UV := $(VENV_DIR)/bin/uv
    UV_PIP_INSTALL := $(VENV_UV) pip install
    UV_PIP_ARGS :=
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
	@echo "$(BLUE)Usage Examples:$(RESET)"
	@echo "  make setup          # Complete development environment setup"
	@echo "  make test           # Run all tests"
	@echo "  make format         # Format code with ruff"
	@echo "  make check          # Lint code and check formatting"

check-python: ## Setup & Installation: Verify Python installation
	@echo "$(BLUE)🐍 Checking Python installation...$(RESET)"
	@if command -v python3 >/dev/null 2>&1; then \
		echo "$(GREEN)✅ Python 3 is available$(RESET)"; \
	else \
		echo "$(RED)❌ Python 3 is not available$(RESET)"; \
		echo "Please install Python 3 and try again."; \
		exit 1; \
	fi

setup: check-python venv install-dev pre-commit ## Setup & Installation: Complete development environment setup
	@echo "$(GREEN)🎉 Setup complete! Your MoneyBin development environment is ready.$(RESET)"
	@echo ""
	@echo "$(BLUE)Next steps:$(RESET)"
	@echo "  1. Activate the virtual environment: source venv/bin/activate"
	@echo "  2. Start developing!"

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
		echo "$(BLUE)📥 Installing uv in virtual environment...$(RESET)"; \
		uv pip install -p $(VENV_DIR)/bin/python uv; \
		echo "$(GREEN)✅ Virtual environment created with uv$(RESET)"; \
	else \
		echo "$(GREEN)✅ Virtual environment already exists$(RESET)"; \
		if [ ! -f "$(VENV_UV)" ]; then \
			echo "$(BLUE)📥 Installing uv in existing virtual environment...$(RESET)"; \
			if [ -f "$(VENV_DIR)/bin/pip" ]; then \
				$(VENV_DIR)/bin/pip install uv; \
			else \
				uv pip install -p $(VENV_DIR)/bin/python uv; \
			fi; \
		fi; \
	fi

install: venv ## Setup & Installation: Install project dependencies
	@echo "$(BLUE)📥 Installing MoneyBin with uv...$(RESET)"
	@$(UV_PIP_INSTALL) $(UV_PIP_ARGS) -e .

install-dev: venv ## Setup & Installation: Install development dependencies (includes testing tools)
	@echo "$(BLUE)📥 Installing MoneyBin with development dependencies (includes testing) using uv...$(RESET)"
	@$(UV_PIP_INSTALL) $(UV_PIP_ARGS) -e ".[dev]"

sync: venv ## Setup & Installation: Sync dependencies using uv (modern approach)
	@echo "$(BLUE)🔄 Syncing dependencies with uv...$(RESET)"
	@$(VENV_UV) sync
	@echo "$(GREEN)✅ Dependencies synchronized$(RESET)"

pre-commit: $(VENV_ACTIVATE) ## Setup & Installation: Install pre-commit hooks (uses venv ruff for consistency)
	@echo "$(BLUE)🔒 Installing pre-commit hooks...$(RESET)"
	@$(VENV_DIR)/bin/pre-commit install
	@echo "$(GREEN)✅ Pre-commit hooks installed$(RESET)"
	@echo "$(BLUE)ℹ️  Pre-commit uses the same ruff version as your virtual environment$(RESET)"

test: $(VENV_ACTIVATE) ## Development: Run tests (requires install-dev)
	@echo "$(BLUE)🧪 Running tests...$(RESET)"
	@$(VENV_DIR)/bin/pytest tests/

test-cov: $(VENV_ACTIVATE) ## Development: Run tests with coverage report (requires install-dev)
	@echo "$(BLUE)🧪 Running tests with coverage...$(RESET)"
	@$(VENV_DIR)/bin/pytest --cov=src tests/
	@echo "$(BLUE)📊 Coverage report generated$(RESET)"

test-unit: $(VENV_ACTIVATE) ## Development: Run unit tests only
	@echo "$(BLUE)🧪 Running unit tests...$(RESET)"
	@$(VENV_DIR)/bin/pytest tests/ -m "unit"

test-integration: $(VENV_ACTIVATE) ## Development: Run integration tests only
	@echo "$(BLUE)🧪 Running integration tests...$(RESET)"
	@$(VENV_DIR)/bin/pytest tests/ -m "integration"

format: $(VENV_ACTIVATE) ## Development: Format code with ruff and fix whitespace issues
	@echo "$(BLUE)🎨 Formatting code with ruff...$(RESET)"
	@$(VENV_DIR)/bin/ruff format .
	@echo "$(BLUE)🔧 Fixing auto-fixable issues...$(RESET)"
	@$(VENV_DIR)/bin/ruff check --fix .
	@echo "$(BLUE)🔧 Fixing whitespace and file ending issues...$(RESET)"
	@$(VENV_DIR)/bin/pre-commit run trailing-whitespace --all-files || true
	@$(VENV_DIR)/bin/pre-commit run end-of-file-fixer --all-files || true
	@echo "$(GREEN)✅ Code formatted and fixed$(RESET)"

lint: $(VENV_ACTIVATE) ## Development: Lint code with ruff
	@echo "$(BLUE)🔍 Linting code with ruff...$(RESET)"
	@$(VENV_DIR)/bin/ruff check .
	@echo "$(GREEN)✅ Linting complete$(RESET)"

type-check: $(VENV_ACTIVATE) ## Development: Type check with pyright
	@echo "$(BLUE)🔍 Type checking with pyright...$(RESET)"
	@$(VENV_DIR)/bin/pyright
	@echo "$(GREEN)✅ Type checking complete$(RESET)"

check: format lint type-check ## Development: Run all code quality checks
	@echo "$(GREEN)✅ All code quality checks complete$(RESET)"

jupyter: $(VENV_ACTIVATE) ## Development: Start Jupyter notebook server
	@echo "$(BLUE)📓 Starting Jupyter notebook server...$(RESET)"
	@$(VENV_DIR)/bin/jupyter notebook notebooks/

dagster-dev: $(VENV_ACTIVATE) ## Development: Start Dagster development server
	@echo "$(BLUE)🚀 Starting Dagster development server...$(RESET)"
	@cd pipelines && ../$(VENV_DIR)/bin/dagster dev

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
	@if [ -f "$(VENV_UV)" ]; then \
		venv_uv_version=$$($(VENV_UV) --version 2>/dev/null | head -n1); \
		echo "  Virtual Environment uv: $(GREEN)✅ $$venv_uv_version$(RESET)"; \
	else \
		echo "  Virtual Environment uv: $(YELLOW)⚠️  Not installed in venv$(RESET)"; \
	fi
	@if [ -f "uv.lock" ]; then \
		echo "  uv.lock: $(GREEN)✅ Found$(RESET)"; \
	else \
		echo "  uv.lock: $(YELLOW)⚠️  Not found$(RESET)"; \
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
	@if [ -f "$(VENV_UV)" ] && [ -d "$(VENV_DIR)" ]; then \
		echo "  Core packages:"; \
		$(VENV_UV) pip list 2>/dev/null | grep -E "(dagster|dbt-core|ruff|pytest|pyright)" | sed 's/^/    /' || echo "    $(YELLOW)⚠️  No core packages found$(RESET)"; \
	else \
		echo "  $(YELLOW)⚠️  Cannot check packages (uv not available in venv)$(RESET)"; \
	fi


activate: ## Utility: Show how to activate virtual environment
	@echo "$(BLUE)To activate the virtual environment, run:$(RESET)"
	@echo "  source $(VENV_DIR)/bin/activate"
	@echo ""
	@echo "$(BLUE)To deactivate:$(RESET)"
	@echo "  deactivate"
