"""
OneLake Storage Client for Microsoft Fabric.

OneLake is ADLS Gen2-compatible. This module wraps azure-storage-file-datalake
to provide read/write operations analogous to the boto3 S3 calls in the AWS pipeline.

Endpoint:  https://onelake.dfs.fabric.microsoft.com
Account:   <workspace-id>   (the GUID from your Fabric workspace URL)
Container: <workspace-id>   (OneLake uses workspace ID as the account AND container)
Path:      <lakehouse-name>/Files/<your-path>

Required env vars (or Prefect Variables/Secrets):
  AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
  FABRIC_WORKSPACE_ID
  FABRIC_LAKEHOUSE_NAME  (default: MarketDataLakehouse)
"""

import io
from config_utils import resolve


def make_onelake_client():
    """
    Create an authenticated DataLakeServiceClient for OneLake.
    Auth: service principal (CLIENT_ID + CLIENT_SECRET + TENANT_ID), or
    DefaultAzureCredential (managed identity / az login) as fallback.
    """
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.identity import ClientSecretCredential, DefaultAzureCredential

    tenant_id = resolve('azure-tenant-id', 'AZURE_TENANT_ID')
    client_id = resolve('azure-client-id', 'AZURE_CLIENT_ID')
    client_secret = resolve('azure-client-secret', 'AZURE_CLIENT_SECRET', is_secret=True)

    if tenant_id and client_id and client_secret:
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    else:
        print("[INFO] Azure service principal not configured, using DefaultAzureCredential")
        credential = DefaultAzureCredential()

    return DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=credential,
    )


class OneLakeClient:
    """
    Thin OneLake wrapper. Paths are relative to Files/ inside the lakehouse.
    Example: client.upload_bytes("stocks/AAPL/data.json", b"...") writes to
             <workspace-id>/<lakehouse>/Files/stocks/AAPL/data.json
    """

    def __init__(self):
        self._svc = make_onelake_client()
        self.workspace_id = resolve('fabric-workspace-id', 'FABRIC_WORKSPACE_ID')
        self.lakehouse = (
            resolve('fabric-lakehouse-name', 'FABRIC_LAKEHOUSE_NAME')
            or 'MarketDataLakehouse'
        )
        if not self.workspace_id:
            raise ValueError("FABRIC_WORKSPACE_ID not set in env or Prefect Variables")

    def _fs(self):
        return self._svc.get_file_system_client(file_system=self.workspace_id)

    def _full(self, path: str) -> str:
        return f"{self.lakehouse}/Files/{path}"

    def upload_bytes(self, path: str, data: bytes) -> None:
        """Write bytes to OneLake (creates parent directories automatically)."""
        fc = self._fs().get_file_client(self._full(path))
        fc.create_file()
        fc.upload_data(data, overwrite=True)

    def upload_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        self.upload_bytes(path, text.encode(encoding))

    def download_bytes(self, path: str) -> bytes:
        """Read bytes from OneLake."""
        fc = self._fs().get_file_client(self._full(path))
        return fc.download_file().readall()

    def download_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.download_bytes(path).decode(encoding)

    def list_files(self, prefix: str, suffix: str = None) -> list:
        """
        List files under Files/<prefix>.
        Returns paths relative to Files/ (not the full lakehouse path).
        """
        full_prefix = self._full(prefix)
        paths = []
        try:
            for p in self._fs().get_paths(path=full_prefix, recursive=True):
                if p.is_directory:
                    continue
                rel = p.name[len(f"{self.lakehouse}/Files/"):]
                if suffix is None or rel.endswith(suffix):
                    paths.append(rel)
        except Exception:
            pass
        return paths

    def file_exists(self, path: str) -> bool:
        try:
            fc = self._fs().get_file_client(self._full(path))
            fc.get_file_properties()
            return True
        except Exception:
            return False

    def abfss_path(self, path: str = "") -> str:
        """
        Return an ABFSS URI for use in Fabric Spark notebooks.
        Format: abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/<path>
        """
        base = (
            f"abfss://{self.workspace_id}"
            f"@onelake.dfs.fabric.microsoft.com"
            f"/{self.lakehouse}/Files"
        )
        return f"{base}/{path}" if path else base
