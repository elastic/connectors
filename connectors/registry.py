import importlib


def get_klass(fqn):
    module_name, klass_name = fqn.split(":")
    module = importlib.import_module(module_name)
    return getattr(module, klass_name)


def get_connector_instance(definition, config):
    logger.debug(f"Getting connector instance for {definition}")
    service_type = definition["service_type"]
    klass = get_klass(config["connectors"][service_type])
    logger.debug(f"Found a matching plugin {klass}")
    return klass(definition)


def get_connectors(config):
    for name, fqn in config["connectors"].items():
        yield get_klass(fqn)
