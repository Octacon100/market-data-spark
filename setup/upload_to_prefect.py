"""
Upload .env values to Prefect Variables and Secrets.

Sensitive keys go to Prefect Secrets (encrypted).
Everything else goes to Prefect Variables.

Naming: UPPER_SNAKE_CASE -> lower-kebab-case
  e.g. ALPHA_VANTAGE_API_KEY -> alpha-vantage-api-key

Usage:
  python upload_to_prefect.py
  python upload_to_prefect.py --dry-run
"""

import asyncio
import argparse
from pathlib import Path
import dotenv

# Keys treated as Prefect Secrets (encrypted at rest)
SECRET_KEYS = {
    'ALPHA_VANTAGE_API_KEY',
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'GMAIL_APP_PASSWORD',
}

# Keys to skip entirely (Prefect config itself, or not relevant to flows)
SKIP_KEYS = {
    'PREFECT_API_URL',
    'PREFECT_API_KEY',
    'PREFECT_LOGGING_LEVEL',
}

# Values that indicate a placeholder — skip these
PLACEHOLDER_MARKERS = (
    'your_',
    'xxxxxxxxx',
    'your-',
)


def to_prefect_name(env_key: str) -> str:
    """Convert UPPER_SNAKE_CASE to lower-kebab-case."""
    return env_key.lower().replace('_', '-')


def is_placeholder(value: str) -> bool:
    return any(marker in value.lower() for marker in PLACEHOLDER_MARKERS)


async def upload_secret(name: str, value: str, overwrite: bool = True):
    from prefect.blocks.system import Secret
    block = Secret(value=value)
    await block.save(name=name, overwrite=overwrite)


def upload_variable(name: str, value: str):
    from prefect.variables import Variable
    Variable.set(name, value, overwrite=True)


def main(dry_run: bool = False):
    env_path = '.env'
    values = dotenv.dotenv_values(env_path)

    secrets = {}
    variables = {}

    for key, value in values.items():
        if not value:
            continue
        if key in SKIP_KEYS:
            print(f"[INFO] Skipping {key} (Prefect config key)")
            continue
        if is_placeholder(value):
            print(f"[WARN] Skipping {key} (placeholder value)")
            continue

        prefect_name = to_prefect_name(key)
        if key in SECRET_KEYS:
            secrets[prefect_name] = value
        else:
            variables[prefect_name] = value

    print(f"\n[INFO] Variables to upload ({len(variables)}):")
    for name, value in variables.items():
        print(f"  {name} = {value}")

    print(f"\n[INFO] Secrets to upload ({len(secrets)}):")
    for name in secrets:
        print(f"  {name} = ****")

    if dry_run:
        print("\n[INFO] Dry run — nothing uploaded.")
        return

    print("\n[INFO] Uploading variables...")
    for name, value in variables.items():
        try:
            upload_variable(name, value)
            print(f"  [OK] Variable: {name}")
        except Exception as e:
            print(f"  [ERROR] Variable {name}: {e}")

    print("\n[INFO] Uploading secrets...")
    for name, value in secrets.items():
        try:
            asyncio.run(upload_secret(name, value))
            print(f"  [OK] Secret: {name}")
        except Exception as e:
            print(f"  [ERROR] Secret {name}: {e}")

    print("\n[OK] Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload .env to Prefect Variables and Secrets")
    parser.add_argument('--dry-run', action='store_true', help="Preview without uploading")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
