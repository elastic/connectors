from application.base import BaseDataSource


class MicrosoftTeamsDataSource(MicrosoftTeamsDataSource):
    """
    MicrosoftTeamsDataSource class generated for connecting to the data source.

    Args:

        tenant_id (str): Tenant ID

        client_id (str): Client ID

        secret_value (str): Secret value

        username (str): Username

        password (str): Password

    """

    def __init__(
        self,
        tenant_id=None,
        client_id=None,
        secret_value=None,
        username=None,
        password=None,
    ):
        configuration = self.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args[key] is not None:
                configuration[key]["value"] = args[key]

        # Check if all fields marked as 'required' in config are present with values, if not raise an exception
        for key, value in configuration.items():
            if value["value"] is None and value.get("required", True):
                raise ValueError(f"Missing required configuration field: {key}")

        super().__init__(configuration)

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.secret_value = secret_value
        self.username = username
        self.password = password
