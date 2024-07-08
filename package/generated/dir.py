from application.base import BaseDataSource


class DirectoryDataSource(DirectoryDataSource):
    """
    DirectoryDataSource class generated for connecting to the data source.

    Args:

        directory (str): Directory path

        pattern (str): File glob-like pattern

    """

    def __init__(
        self, directory="/Users/jedr/connectors/connectors/sources", pattern="**/*.*"
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

        self.directory = directory
        self.pattern = pattern
