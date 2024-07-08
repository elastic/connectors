from application.base import BaseDataSource


class GMailDataSource(GMailDataSource):
    """
    GMailDataSource class generated for connecting to the data source.

    Args:

        service_account_credentials (str): GMail service account JSON

        subject (str): Google Workspace admin email
            - Admin account email address

        customer_id (str): Google customer id
            - Google admin console -> Account -> Settings -> Customer Id

        include_spam_and_trash (bool): Include spam and trash emails
            - Will include spam and trash emails, when set to true.

        use_document_level_security (bool): Enable document level security
            - Document level security ensures identities and permissions set in GMail are maintained in Elasticsearch. This enables you to restrict and personalize read-access users have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.

    """

    def __init__(
        self,
        service_account_credentials=None,
        subject=None,
        customer_id=None,
        include_spam_and_trash=False,
        use_document_level_security=True,
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

        self.service_account_credentials = service_account_credentials
        self.subject = subject
        self.customer_id = customer_id
        self.include_spam_and_trash = include_spam_and_trash
        self.use_document_level_security = use_document_level_security
