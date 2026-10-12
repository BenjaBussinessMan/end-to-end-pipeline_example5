"""Environment-aware configuration for the lakehouse pipeline."""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    env: str = "dev"
    catalog_prefix: str = "lakehouse"

    @property
    def catalog(self) -> str:
        return f"{self.catalog_prefix}_{self.env}"

    @property
    def landing_path(self) -> str:
        return f"/Volumes/{self.catalog}/landing/raw"

    @property
    def checkpoint_path(self) -> str:
        return f"/Volumes/{self.catalog}/landing/_checkpoints"

    def table(self, schema: str, name: str) -> str:
        """Fully qualified Unity Catalog name: catalog.schema.table."""
        return f"{self.catalog}.{schema}.{name}"


def get_config() -> Config:
    return Config(env=os.getenv("LAKEHOUSE_ENV", "dev"))
