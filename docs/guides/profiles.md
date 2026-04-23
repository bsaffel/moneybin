# Multi-Profile Support

Each profile is a complete isolation boundary with its own database, configuration, and logs. Profiles live under `~/.moneybin/profiles/<name>/`.

## Profile Structure

```
~/.moneybin/
└── profiles/
    ├── default/           # Default profile (created automatically)
    │   ├── moneybin.duckdb    # Encrypted database
    │   ├── config.yaml        # Profile configuration
    │   ├── logs/              # Daily log files
    │   ├── backups/           # Database backups
    │   └── temp/              # Temporary files
    └── work/              # Example additional profile
        ├── moneybin.duckdb
        ├── config.yaml
        ├── logs/
        ├── backups/
        └── temp/
```

## Commands

```bash
# Create a new profile
moneybin profile create work

# List all profiles (shows which is active)
moneybin profile list

# Switch the default profile
moneybin profile switch work

# Use a specific profile for one command (doesn't change default)
moneybin --profile work import file statement.qfx

# Show resolved settings for a profile
moneybin profile show

# Set a configuration value
moneybin profile set institution_name "Acme Corp"

# Delete a profile and all its data (database, logs, config)
moneybin profile delete old-test
```

## Use Cases

| Scenario | How |
|----------|-----|
| **Personal vs. business finances** | `moneybin profile create business`, import business files into that profile |
| **Testing with synthetic data** | `moneybin synthetic generate --persona basic --profile test-data` |
| **Multiple users on shared machine** | Each user gets their own profile with its own encrypted database |
| **Trying risky operations** | Create a throwaway profile, experiment freely, delete when done |

## Environment Variable

Set `MONEYBIN_PROFILE` to override the default profile for all commands in a shell session:

```bash
export MONEYBIN_PROFILE=work
moneybin import status  # uses 'work' profile
```

The `--profile` flag takes precedence over the environment variable.
