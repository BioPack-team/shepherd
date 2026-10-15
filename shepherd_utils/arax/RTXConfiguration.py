# Shepherd stand-in for RTXteam/RTX @ 9485431, code/RTXConfiguration.py.
# Not a port: ARAX's version reads config_dbs.json, config_secrets.json, the
# OpenAPI YAML, git branch names and the deployment domain. This keeps the same
# class name and exposes only the attributes the ported modules read, backed by
# Shepherd's settings. Add attributes here as more modules are ported.
# See docs/ARAX_PORT_BASELINE.md (DEC-6, DEC-14).
from urllib.parse import urlparse

from shepherd_utils.config import settings
from shepherd_utils.data_download import (
    ARAX_AUTOCOMPLETE,
    ARAX_COHD,
    ARAX_CURIE_TO_PMIDS,
    ARAX_EXPLAINABLE_DTD,
    ARAX_FDA_APPROVED_DRUGS,
    arax_db_path,
    arax_pathfinder_sqlite_paths,
)

# ARAX's UI/OpenAPI/python-flask-server/openapi_server/openapi/openapi.yaml @ 9485431
ARAX_VERSION = "1.6.2"  # info.version
TRAPI_VERSION = "1.6.0"  # info.x-trapi.version
BIOLINK_VERSION = "4.2.5"  # info.x-translator.biolink-version


class RTXConfiguration:
    def __init__(self):
        self.arax_version = ARAX_VERSION
        self.trapi_version = TRAPI_VERSION
        first_two_trapi_version_nums = self.trapi_version.split(".")[:2]
        self.trapi_major_version = ".".join(first_two_trapi_version_nums)
        self.version = f"ARAX {self.arax_version}"

    @property
    def maturity(self) -> str:
        # Same vocabulary as ARAX's: development / staging / testing / production
        return settings.server_maturity

    # ARAX derives these from its git checkout and deployment domain, and only
    # logs them (ARAX_query's startup debug line). Shepherd has neither.
    current_branch_name = None
    is_itrb_instance = False

    @property
    def is_production_server(self) -> bool:
        return self.maturity == "production"

    @property
    def kg2c_sqlite_path(self) -> str:
        # ARAX's kg2c_sqlite and tier0_sqlite point at the same
        # tier0-info-for-overlay file, which the pathfinder already downloads.
        return arax_pathfinder_sqlite_paths()[1]

    @property
    def fda_approved_drugs_path(self) -> str:
        return arax_db_path(ARAX_FDA_APPROVED_DRUGS)

    @property
    def curie_to_pmids_path(self) -> str:
        return arax_db_path(ARAX_CURIE_TO_PMIDS)

    @property
    def curie_ngd_path(self) -> str:
        return arax_pathfinder_sqlite_paths()[0]

    @property
    def cohd_database_path(self) -> str:
        return arax_db_path(ARAX_COHD)

    @property
    def explainable_dtd_db_path(self) -> str:
        return arax_db_path(ARAX_EXPLAINABLE_DTD)

    @property
    def tier0_sqlite_path(self) -> str:
        return self.kg2c_sqlite_path

    @property
    def autocomplete_path(self) -> str:
        return arax_db_path(ARAX_AUTOCOMPLETE)

    @property
    def instance_name(self) -> str:
        return settings.server_location

    @property
    def domain(self) -> str:
        return urlparse(settings.server_url).netloc

    @staticmethod
    def _file_version(path: str, extension: str) -> str:
        # ARAX's rule: the filename after its last "_v", minus the extension
        return path.split("/")[-1].split("_v")[-1].replace(extension, "")

    def get_config_settings(self) -> dict:
        """ARAX's /status?mode=site_config: the same keys, in ARAX's order."""
        config = {
            "arax_version": self.arax_version,
            "trapi_version": self.trapi_version,
            "trapi_major_version": self.trapi_major_version,
            "version": self.version,
            "instance_name": self.instance_name,
            "domain": self.domain,
            "current_branch_name": self.current_branch_name,
            "maturity": self.maturity,
            "is_itrb_instance": self.is_itrb_instance,
            "is_production_server": self.is_production_server,
        }
        for name, extension in (
            ("cohd_database", ".db"),
            ("curie_to_pmids", ".sqlite"),
            ("curie_ngd", ".sqlite"),
            ("kg2c_sqlite", ".sqlite"),
            ("tier0_sqlite", ".sqlite"),
            ("fda_approved_drugs", ".pickle"),
            ("autocomplete", ".sqlite"),
            ("explainable_dtd_db", ".db"),
        ):
            config[f"{name}_version"] = self._file_version(
                getattr(self, f"{name}_path"), extension
            )
        return {"config": config}
