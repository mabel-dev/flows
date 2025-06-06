from typing import Any
from typing import Dict

import yaml


class TenantModel:
    """
    Represents a tenant's configuration loaded from a YAML file.

    Parameters:
        name: str
            The tenant's name.
        variables: Dict
            The loaded variables for the tenant.
    """

    def __init__(self, name: str, variables: Dict[str, Any]):
        self.name = name
        self.variables = variables

    @classmethod
    def from_name(cls, tenant_name: str, base_path: str = "tenants") -> "TenantModel":
        """
        Loads tenant variables from a YAML file.

        Args:
            name: The tenant's name.
            base_path: The base directory where tenant folders are stored.

        Returns:
            TenantModel instance.
        """
        if "." in tenant_name:
            raise ValueError("Tenant name should not contain dots. Use underscores instead.")

        from flows.providers.tenants import get_tenants_provider

        tenants_provider = get_tenants_provider()
        tenant_variables = tenants_provider.get(tenant_name)

        return cls(name=tenant_name, variables=tenant_variables)
