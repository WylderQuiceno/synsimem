import pytest
from unittest.mock import patch, Mock
from src.utils.key_vault import KeyVault


@pytest.fixture
def key_vault():
    return KeyVault()


@patch("src.utils.key_vault.SecretClient")
def test_get_secret(mock_secret_client, key_vault):
    # Configura el mock para SecretClient y su metodo get_secret
    mock_secret = Mock()
    mock_secret.value = "secret_value"
    mock_secret_client.return_value.get_secret.return_value = mock_secret

    # Llama a getSecret y verifica que retorna el valor esperaado
    secret_value = key_vault.get_secret("my-vault", "my-secret", None)
    assert secret_value == "secret_value"
