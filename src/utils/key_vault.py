import os
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient


class KeyVault:

    tenant_id = os.getenv("TENANT_ID", "")
    client_id = os.getenv("CLIENT_ID", "")
    client_secret = os.getenv("CLIENT_SECRET", "")

    print(tenant_id)

    credential = ClientSecretCredential(
        tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
    )

    def get_secret(self, key_vault_name, secret_name, _):
        keyvault_url = f"https://{key_vault_name}.vault.azure.net/"
        secret_client = SecretClient(vault_url=keyvault_url, credential=self.credential)
        try:
            secret_value = secret_client.get_secret(secret_name)
            return secret_value.value
        except Exception as e:
            raise Exception(f"Error al obtener el secreto: {e}")
