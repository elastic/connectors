from typing import Dict, List

"""
This is a fixture for generating test data and listing paths in a file system.
"""


class PathProperties:
    def __init__(self, name: str, is_directory: bool = False):
        self.name = name
        self.is_directory = is_directory

    def to_dict(self):
        return {"name": self.name, "is_directory": self.is_directory}


class ItemPaged:
    def __init__(self, items: List[PathProperties]):
        self.items = items

    def __iter__(self):
        for item in self.items:
            yield item


class FileSystemClient:
    def __init__(self, file_system_name: str):
        self.file_system_name = file_system_name
        self.files = {}

    def add_file(self, file_path: str, is_directory: bool = False):
        self.files[file_path] = is_directory

    def get_paths(self, path: str = None):
        paths = [
            PathProperties(name=file_path, is_directory=is_directory)
            for file_path, is_directory in self.files.items()
            if path is None or file_path.startswith(path)
        ]
        return ItemPaged(paths)


FILE_SYSTEMS: Dict[str, FileSystemClient] = {}


def create_file_system(name: str):
    if name not in FILE_SYSTEMS:
        FILE_SYSTEMS[name] = FileSystemClient(name)


def load(config: Dict):
    """
    Loads initial data into the backend based on OneLake configuration.

    Args:
        config: Dictionary containing OneLake configuration with format:
            {
                "configuration": {
                    "workspace_name": {"value": str},
                    "data_path": {"value": str},
                    ...
                }
            }
    """
    if not config.get("configuration"):
        raise ValueError("Invalid configuration format")

    conf = config["configuration"]
    workspace_name = conf["workspace_name"]["value"]
    data_path = conf["data_path"]["value"]

    create_file_system(workspace_name)
    generate_test_data(workspace_name, data_path, file_count=10000)


def generate_test_data(file_system_name: str, folder_path: str, file_count: int):
    create_file_system(file_system_name)
    file_system = FILE_SYSTEMS[file_system_name]

    file_system.add_file(folder_path, is_directory=True)

    for i in range(file_count):
        file_name = f"{folder_path}/file_{i}.txt"
        file_system.add_file(file_name)


def list_paths(file_system_name: str, folder_path: str):
    if file_system_name not in FILE_SYSTEMS:
        return []

    file_system = FILE_SYSTEMS[file_system_name]
    return file_system.get_paths(path=folder_path)
