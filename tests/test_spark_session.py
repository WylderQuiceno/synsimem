import pytest
from unittest.mock import patch, MagicMock
from src.utils.spark_session import ConfigSpark


# Fixture para configurar y limpiar las variables de entorno
@pytest.fixture(autouse=True)
def setup_env_vars(monkeypatch):
    monkeypatch.setenv("TENANT_ID", "fake_tenant_id")
    monkeypatch.setenv("CLIENT_ID", "fake_client_id")
    monkeypatch.setenv("CLIENT_SECRET", "fake_client_secret")
    monkeypatch.setenv("STORAGE_ACCOUNT_NAME", "fake_storage_account_name")
    monkeypatch.setenv("KEYVAULT_NAME", "fake_keyvault_name")
    # Asegúrate de limpiar la instancia singleton antes y después de cada prueba
    ConfigSpark._instance = None
    yield
    ConfigSpark._instance = None


# Simular SparkSession y sus métodos
@pytest.fixture
def mock_spark_session():
    with patch("pyspark.sql.SparkSession.builder.getOrCreate") as mock_getOrCreate:
        mock_spark = MagicMock()
        mock_getOrCreate.return_value = mock_spark
        yield mock_spark


def test_config_spark_singleton():
    spark1 = ConfigSpark().get_spark()
    spark2 = ConfigSpark().get_spark()
    assert spark1 is spark2
