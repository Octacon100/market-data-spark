"""Shared config resolution: Prefect Variables/Secrets first, env vars as fallback."""

import asyncio
import os
from typing import Optional

import dotenv

dotenv.load_dotenv()


def _get_prefect_variable(name: str) -> Optional[str]:
    try:
        from prefect.variables import Variable
        return Variable.get(name)
    except Exception:
        return None


def _get_prefect_secret(name: str) -> Optional[str]:
    try:
        from prefect.blocks.system import Secret
        return Secret.load(name).get()
    except Exception:
        return None


def resolve(prefect_name: str, env_name: str, is_secret: bool = False) -> str:
    """Resolve a config value: Prefect first, then env var."""
    val = _get_prefect_secret(prefect_name) if is_secret else _get_prefect_variable(prefect_name)
    return val or os.getenv(env_name, '')


def make_boto3_client(service: str, region: str = None):
    """Create a boto3 client using resolved AWS credentials."""
    import boto3
    region = region or resolve('aws-region', 'AWS_DEFAULT_REGION') or os.getenv('AWS_REGION', 'us-east-1')
    access_key = resolve('aws-access-key-id', 'AWS_ACCESS_KEY_ID', is_secret=True)
    secret_key = resolve('aws-secret-access-key', 'AWS_SECRET_ACCESS_KEY', is_secret=True)
    kwargs = {'region_name': region}
    if access_key and secret_key:
        kwargs['aws_access_key_id'] = access_key
        kwargs['aws_secret_access_key'] = secret_key
    return boto3.client(service, **kwargs)
