# Cursor Project Rules

This directory contains project-specific rules for Cursor AI integration. These rules are automatically applied when working on the MoneyBin project.

## Rule Files

### Always Applied Rules

- **`data-ownership-privacy.mdc`**: Core data ownership and privacy principles
- **`documentation-sources.mdc`**: Primary documentation sources for project dependencies
- **`path-objects.mdc`**: Path object usage standards for filesystem operations
- **`python-development.mdc`**: Comprehensive Python development guidelines and coding standards

### Context-Specific Rules

- **`testing-strategy.mdc`**: Focused testing strategy and implementation guidelines
- **`cli-development.mdc`**: CLI command development patterns and standards
- **`data-processing.mdc`**: Data processing and integration standards
- **`duckdb-functions.mdc`**: DuckDB-specific SQL functions and patterns
- **`financial-analysis.mdc`**: Financial analysis queries and success metrics

## How It Works

Cursor automatically detects and applies these rules based on:

- **Always Applied**: Rules with `alwaysApply: true` are included in every AI interaction
- **File Pattern Matching**: Rules with `globs` patterns are applied when working with matching files
- **Context Relevance**: Cursor intelligently applies relevant rules based on the current context

## Rule Format

Each `.mdc` file follows this structure:

```yaml
---
description: Brief description of the rule's purpose
globs: ["file/pattern/**/*"]  # Optional: files where rule applies
alwaysApply: false           # Optional: always include this rule
---

# Rule Content

Detailed guidelines and requirements in markdown format.
```

## Relationship to PROJECT_RULES.md

The comprehensive project guidelines are maintained in the root `PROJECT_RULES.md` file. These `.mdc` files are focused, actionable versions optimized for Cursor AI integration.
