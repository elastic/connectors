from application.base import BaseDataSource


class SalesforceDataSource(SalesforceDataSource):
    """
    SalesforceDataSource class generated for connecting to the data source.

    Args:

        domain (str): Domain
            - The domain for your Salesforce instance. If your Salesforce URL is 'foo.my.salesforce.com', the domain would be 'foo'.

        client_id (str): Client ID
            - The client id for your OAuth2-enabled connected app. Also called 'consumer key'

        client_secret (str): Client Secret
            - The client secret for your OAuth2-enabled connected app. Also called 'consumer secret'

        use_text_extraction_service (bool): Use text extraction service
            - Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.

        use_document_level_security (bool): Enable document level security
            - Document level security ensures identities and permissions set in Salesforce are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.

    """

    def __init__(
        self,
        domain=None,
        client_id=None,
        client_secret=None,
        use_text_extraction_service=False,
        use_document_level_security=False,
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

        self.domain = domain
        self.client_id = client_id
        self.client_secret = client_secret
        self.use_text_extraction_service = use_text_extraction_service
        self.use_document_level_security = use_document_level_security
