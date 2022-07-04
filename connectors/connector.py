class Connector:
    def __init__(self, definition):
        self.definition = definition
        self.service_type = definition["service_type"]
        self.index_name = definition["index_name"]
        self.configuration = {}
        for key, value in definition["configuration"].items():
            self.configuration[key] = value['value']

    def json(self):
        return self.definition
