# CLI Reference

MoneyBin's CLI is organized by domain with commands in workflow order. Every command supports `--help` for detailed usage.

## Global Options

| Option | Short | Description |
|--------|-------|-------------|
| `--profile` | `-p` | Use a specific profile for this command |
| `--verbose` | `-v` | Enable debug logging |

## Command Tree

```
moneybin
├── profile          Profile lifecycle
│   ├── create       Create a new profile
│   ├── list         List all profiles (marks active)
│   ├── switch       Change default profile
│   ├── delete       Delete a profile and all data
│   ├── show         Show resolved settings
│   └── set          Set a config value
│
├── import           Data import
│   ├── file         Import a financial file (auto-detects type)
│   ├── status       Summary of all imported data
│   ├── history      List recent imports with batch details
│   ├── preview      Preview file structure (dry run)
│   ├── revert       Undo an import batch
│   └── formats      Manage saved import formats
│       ├── list     List all available formats
│       ├── show     Show format details
│       └── delete   Delete a user-saved format
│
├── transform        SQLMesh pipeline
│   ├── apply        Apply pending changes
│   ├── plan         Preview what will change
│   ├── status       Current model state
│   ├── validate     Check model SQL parses correctly
│   ├── audit        Run data quality audits
│   └── restate      Force recompute a model for a date range
│
├── categorize       Categorization management
│   ├── apply-rules  Run rules on uncategorized transactions
│   ├── seed         Initialize default categories (Plaid PFCv2)
│   ├── stats        Coverage statistics
│   └── list-rules   Display active rules
│
├── stats            Observability
│   └── show         Lifetime metric aggregates
│
├── mcp              MCP server
│   ├── serve        Start the MCP server
│   ├── list-tools   List all registered tools
│   ├── list-prompts List all registered prompts
│   └── config
│       └── generate Generate client config (Claude Desktop, Cursor, Windsurf)
│
├── db               Database management
│   ├── init         Create a new encrypted database
│   ├── shell        Interactive DuckDB SQL shell
│   ├── ui           DuckDB web UI (browser-based)
│   ├── query        Run a SQL query
│   ├── info         Database metadata (size, tables, encryption, versions)
│   ├── backup       Create timestamped backup
│   ├── restore      Restore from a backup file
│   ├── lock         Clear cached encryption key
│   ├── unlock       Derive key from passphrase and cache
│   ├── key          Print the encryption key
│   ├── rotate-key   Re-encrypt with a new key
│   ├── ps           Show processes holding the database
│   ├── kill         Kill processes holding the database
│   └── migrate
│       ├── apply    Apply pending schema migrations
│       └── status   Show migration state and drift warnings
│
├── logs             Log management
│   ├── tail         View recent log entries (-f to follow)
│   ├── path         Print log directory path
│   └── clean        Delete old log files
│
└── synthetic        Test data generation
    ├── generate     Generate synthetic data for a persona
    └── reset        Wipe and regenerate from scratch
```

## Common Workflows

### First-time setup

```bash
moneybin profile create personal
moneybin categorize seed
moneybin import file ~/Downloads/checking.qfx
```

### Import and categorize

```bash
moneybin import file ~/Downloads/chase_may.csv --account-name "Chase Checking"
moneybin categorize apply-rules
moneybin categorize summary
```

### Query your data

```bash
moneybin db query "SELECT category, SUM(amount) FROM core.fct_transactions GROUP BY category ORDER BY SUM(amount)"
moneybin db shell
```

### Connect an AI assistant

```bash
moneybin mcp config generate --client claude-desktop --install
```

### Database maintenance

```bash
moneybin db backup
moneybin db info
moneybin db migrate status
```
