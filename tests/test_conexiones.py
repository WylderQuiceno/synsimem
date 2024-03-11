import pytest
from pyspark.sql import SparkSession
from src.conexiones import Conexiones
from unittest.mock import patch


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("pytest-pyspark-local-testing")
        .master("local[2]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_create_jdbc_url():
    conexiones = Conexiones()
    odbc_url = "Server=test;Database=test_db;User Id=user;Password=password;"
    expected_jdbc_url, _, expected_properties = conexiones.create_jdbc_url(odbc_url)

    assert expected_jdbc_url == "jdbc:sqlserver://test:1433;databaseName=test_db"
    assert expected_properties == {
        "user": "user",
        "password": "password",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
