from importlib.metadata import version

def connectors_version():
    return version("elasticsearch-connectors")
