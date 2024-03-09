import os
from pathlib import Path
from pyspark.sql import SparkSession


class ConfigSpark:

    _instance = None

    tenant_id = os.getenv("TENANT_ID", "")
    client_id = os.getenv("CLIENT_ID", "")
    client_secret = os.getenv("CLIENT_SECRET", "")
    storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME", "")
    keyvault_name = os.getenv("KEYVAULT_NAME", "")

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigSpark, cls).__new__(cls)
        return cls._instance

    def get_spark(self) -> SparkSession:

        current_path = Path(__file__)
        jars_path = current_path.parent.parent.parent / "jars/*"
        jars_path = str(jars_path)
        print(jars_path)

        spark = (
            SparkSession.builder.appName("Synapse")
            # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.extraClassPath", jars_path)
            # .config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.1.0')
            .getOrCreate()
        )

        spark.sparkContext.environment["keyvault"] = self.keyvault_name
        spark.sparkContext.environment["storageaccount"] = self.storage_account_name

        spark.conf.set(
            f"fs.azure.account.auth.type.{self.storage_account_name}.dfs.core.windows.net",
            "OAuth",
        )
        spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{self.storage_account_name}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.id.{self.storage_account_name}.dfs.core.windows.net",
            self.client_id,
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.secret.{self.storage_account_name}.dfs.core.windows.net",
            self.client_secret,
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{self.storage_account_name}.dfs.core.windows.net",
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token",
        )
        return spark
