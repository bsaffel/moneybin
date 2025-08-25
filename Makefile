# MoneyBin Development Makefile
# This Makefile provides development commands for the MoneyBin project

.PHONY: help setup clean install install-dev test test-cov lint format type-check pre-commit init-frameworks venv activate status install-uv

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
UV := $(shell command -v uv 2> /dev/null)

# Check if virtual environment is active
ifdef VIRTUAL_ENV
    PYTHON_ENV := $(PYTHON)
    UV_ENV := uv
else
    PYTHON_ENV := $(VENV_DIR)/bin/python
    UV_ENV := uv
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
	@echo "$(GREEN)Framework Initialization:$(RESET)"
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z_-]+:.*?## / && /Framework|Initialize|Init/ {printf "  $(YELLOW)%-20s$(RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(GREEN)Utility:$(RESET)"
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z_-]+:.*?## / && /Clean|Status|Utility/ {printf "  $(YELLOW)%-20s$(RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(BLUE)Usage Examples:$(RESET)"
	@echo "  make setup          # Complete development environment setup"
	@echo "  make test           # Run all tests"
	@echo "  make format         # Format code with ruff"
	@echo "  make lint           # Lint code and check formatting"
	@echo "  make init-frameworks # Initialize git, dagster, and dbt core"
	@echo ""
	@echo "$(BLUE)Next Steps After Setup:$(RESET)"
	@echo "  1. Activate virtual environment: source venv/bin/activate"
	@echo "  2. Initialize frameworks: make init-frameworks"
	@echo "  3. Start developing!"

install-uv: ## Setup & Installation: Install uv package manager
	@if command -v uv >/dev/null 2>&1; then \
		echo "$(GREEN)✅ uv is already installed$(RESET)"; \
	else \
		echo "$(BLUE)📥 Installing uv package manager...$(RESET)"; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
		echo "$(GREEN)✅ uv installed successfully$(RESET)"; \
		echo "$(YELLOW)⚠️  Please restart your terminal or run 'source ~/.bashrc' to use uv$(RESET)"; \
		exit 1; \
	fi

setup: install-uv venv install-dev pre-commit ## Setup & Installation: Complete development environment setup
	@echo "$(GREEN)🎉 Setup complete! Your MoneyBin development environment is ready.$(RESET)"
	@echo ""
	@echo "$(BLUE)Next steps:$(RESET)"
	@echo "  1. Activate the virtual environment: source venv/bin/activate"
	@echo "  2. Initialize frameworks: make init-frameworks"
	@echo "  3. Start developing!"

venv: $(VENV_ACTIVATE) ## Setup & Installation: Create virtual environment

$(VENV_ACTIVATE):
	@echo "$(BLUE)🚀 Setting up MoneyBin development environment...$(RESET)"
	@if command -v pyenv >/dev/null 2>&1 && [ -f ".python-version" ]; then \
		echo "$(BLUE)🐍 Using pyenv with .python-version file...$(RESET)"; \
		pyenv install --skip-existing; \
		pyenv local; \
		python_version=$$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"); \
		echo "$(GREEN)✅ Python version (pyenv): $$python_version$(RESET)"; \
	else \
		python_version=$$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"); \
		required_version="3.11"; \
		if [ "$$(printf '%s\n' "$$required_version" "$$python_version" | sort -V | head -n1)" != "$$required_version" ]; then \
			echo "$(RED)❌ Error: Python 3.11 or higher is required. Found: $$python_version$(RESET)"; \
			echo "Please upgrade Python and try again."; \
			echo "Consider using pyenv: https://github.com/pyenv/pyenv"; \
			exit 1; \
		fi; \
		echo "$(GREEN)✅ Python version (system): $$python_version$(RESET)"; \
	fi
	@if [ ! -d "$(VENV_DIR)" ]; then \
		echo "$(BLUE)📦 Creating virtual environment with uv...$(RESET)"; \
		uv venv $(VENV_DIR); \
		echo "$(GREEN)✅ Virtual environment created$(RESET)"; \
	else \
		echo "$(GREEN)✅ Virtual environment already exists$(RESET)"; \
	fi

install: venv ## Setup & Installation: Install project dependencies
	@echo "$(BLUE)📥 Installing MoneyBin with uv...$(RESET)"
	@uv pip install -e .

install-dev: venv ## Setup & Installation: Install development dependencies
	@echo "$(BLUE)📥 Installing MoneyBin with development dependencies using uv...$(RESET)"
	@uv pip install -e ".[dev]"

pre-commit: $(VENV_ACTIVATE) ## Development: Install pre-commit hooks
	@echo "$(BLUE)🔒 Installing pre-commit hooks...$(RESET)"
	@$(VENV_DIR)/bin/pre-commit install
	@echo "$(GREEN)✅ Pre-commit hooks installed$(RESET)"

test: $(VENV_ACTIVATE) ## Development: Run tests
	@echo "$(BLUE)🧪 Running tests...$(RESET)"
	@$(VENV_DIR)/bin/pytest tests/

test-cov: $(VENV_ACTIVATE) ## Development: Run tests with coverage report
	@echo "$(BLUE)🧪 Running tests with coverage...$(RESET)"
	@$(VENV_DIR)/bin/pytest --cov=src tests/
	@echo "$(BLUE)📊 Coverage report generated$(RESET)"

test-unit: $(VENV_ACTIVATE) ## Development: Run unit tests only
	@echo "$(BLUE)🧪 Running unit tests...$(RESET)"
	@$(VENV_DIR)/bin/pytest tests/ -m "unit"

test-integration: $(VENV_ACTIVATE) ## Development: Run integration tests only
	@echo "$(BLUE)🧪 Running integration tests...$(RESET)"
	@$(VENV_DIR)/bin/pytest tests/ -m "integration"

format: $(VENV_ACTIVATE) ## Development: Format code with ruff
	@echo "$(BLUE)🎨 Formatting code with ruff...$(RESET)"
	@$(VENV_DIR)/bin/ruff format .
	@echo "$(BLUE)🔧 Fixing auto-fixable issues...$(RESET)"
	@$(VENV_DIR)/bin/ruff check --fix .
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

init-git: ## Framework: Initialize git repository
	@if [ ! -d ".git" ]; then \
		echo "$(BLUE)📦 Initializing git repository...$(RESET)"; \
		git init; \
		echo "$(GREEN)✅ Git repository initialized$(RESET)"; \
	else \
		echo "$(YELLOW)⚠️  Git repository already exists$(RESET)"; \
	fi

init-dagster: $(VENV_ACTIVATE) ## Framework: Initialize dagster project
	@if [ ! -d "pipelines" ]; then \
		echo "$(BLUE)📦 Initializing dagster project in pipelines/ directory...$(RESET)"; \
		$(VENV_DIR)/bin/dagster project scaffold --name pipelines; \
		echo "$(BLUE)🧹 Cleaning up duplicate files and nested directories...$(RESET)"; \
		rm -rf pipelines/pipelines_tests pipelines/setup.py pipelines/setup.cfg pipelines/README.md pipelines/pyproject.toml; \
		mv pipelines/pipelines/* pipelines/ 2>/dev/null || true; \
		rmdir pipelines/pipelines 2>/dev/null || true; \
		echo "$(GREEN)✅ Dagster project initialized and cleaned up$(RESET)"; \
	else \
		echo "$(YELLOW)⚠️  Dagster project already exists in pipelines/$(RESET)"; \
	fi

init-dbt: $(VENV_ACTIVATE) ## Framework: Initialize dbt core project
	@if [ ! -d "dbt" ]; then \
		echo "$(BLUE)📦 Initializing dbt core project...$(RESET)"; \
		$(VENV_DIR)/bin/dbt init dbt; \
		echo "$(GREEN)✅ DBT core project initialized$(RESET)"; \
	else \
		echo "$(YELLOW)⚠️  DBT project already exists$(RESET)"; \
	fi

init-frameworks: init-git init-dagster init-dbt ## Framework: Initialize all frameworks (git, dagster, dbt core)
	@echo "$(GREEN)🎉 All frameworks initialized!$(RESET)"

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

clean-dagster: ## Utility: Clean dagster project files and directories
	@echo "$(BLUE)🧹 Cleaning dagster project...$(RESET)"
	@rm -rf pipelines/pipelines_tests pipelines/setup.py pipelines/setup.cfg pipelines/README.md pipelines/pyproject.toml 2>/dev/null || true
	@if [ -d "pipelines/pipelines" ]; then \
		mv pipelines/pipelines/* pipelines/ 2>/dev/null || true; \
		rmdir pipelines/pipelines 2>/dev/null || true; \
	fi
	@echo "$(GREEN)✅ Dagster project cleaned$(RESET)"

clean: clean-cache clean-venv clean-dagster ## Utility: Clean all generated files and virtual environment
	@echo "$(GREEN)✅ All clean!$(RESET)"

status: ## Utility: Show development environment status
	@echo "$(BLUE)MoneyBin Development Environment Status$(RESET)"
	@echo ""
	@echo "$(GREEN)Python Environment:$(RESET)"
	@if [ -d "$(VENV_DIR)" ]; then \
		echo "  Virtual Environment: $(GREEN)✅ Created$(RESET)"; \
		venv_python=$$($(VENV_DIR)/bin/python --version 2>&1); \
		echo "  Python Version: $$venv_python"; \
	else \
		echo "  Virtual Environment: $(RED)❌ Not found$(RESET)"; \
	fi
	@echo ""
	@echo "$(GREEN)Framework Status:$(RESET)"
	@if [ -d ".git" ]; then echo "  Git: $(GREEN)✅ Initialized$(RESET)"; else echo "  Git: $(RED)❌ Not initialized$(RESET)"; fi
	@if [ -d "pipelines" ]; then echo "  Dagster: $(GREEN)✅ Initialized$(RESET)"; else echo "  Dagster: $(RED)❌ Not initialized$(RESET)"; fi
	@if [ -d "dbt" ]; then echo "  DBT Core: $(GREEN)✅ Initialized$(RESET)"; else echo "  DBT Core: $(RED)❌ Not initialized$(RESET)"; fi
	@echo ""
	@echo "$(GREEN)Dependencies:$(RESET)"
	@if command -v uv >/dev/null 2>&1; then echo "  uv: $(GREEN)✅ Available$(RESET)"; else echo "  uv: $(RED)❌ Not available$(RESET)"; fi
	@if command -v pyenv >/dev/null 2>&1; then echo "  pyenv: $(GREEN)✅ Available$(RESET)"; else echo "  pyenv: $(YELLOW)⚠️  Not available$(RESET)"; fi
	@if [ -f ".python-version" ]; then echo "  .python-version: $(GREEN)✅ Found$(RESET)"; else echo "  .python-version: $(YELLOW)⚠️  Not found$(RESET)"; fi

activate: ## Utility: Show how to activate virtual environment
	@echo "$(BLUE)To activate the virtual environment, run:$(RESET)"
	@echo "  source $(VENV_DIR)/bin/activate"
	@echo ""
	@echo "$(BLUE)To deactivate:$(RESET)"
	@echo "  deactivate"

# Development workflow shortcuts
dev: setup ## Development: Complete setup and start development
	@echo "$(GREEN)🚀 Development environment ready!$(RESET)"
	@echo "$(BLUE)Run 'make activate' to see activation instructions$(RESET)"

quick-check: lint type-check ## Development: Quick code quality check (lint + type check)
	@echo "$(GREEN)✅ Quick quality check complete$(RESET)"
