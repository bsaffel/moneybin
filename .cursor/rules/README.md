# Cursor Project Rules

This directory contains project-specific rules for Cursor AI integration. These rules are automatically applied when working on the MoneyBin project.

## Rule Files

- **`data-ownership-privacy.mdc`**: Core data ownership and privacy principles (always applied)
- **`development-standards.mdc`**: Development standards and best practices (always applied)
- **`documentation-sources.mdc`**: Primary documentation sources for project dependencies (always applied)
- **`data-processing.mdc`**: Data processing and integration standards (applied to relevant files)
- **`financial-analysis.mdc`**: Financial analysis queries and success metrics (applied to SQL/analysis files)

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
