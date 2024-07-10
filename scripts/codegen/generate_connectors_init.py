import os
from jinja2 import Environment, FileSystemLoader
from black import format_file_in_place, FileMode, WriteBack, NothingChanged
from pathlib import Path

from connectors.config import _default_config


def generate_init_file(output_dir, template_env):
    init_file_path = os.path.join(output_dir, "__init__.py")
    imports = []

    connectors_config = _default_config()
    data_source_classes = connectors_config["sources"]

    for module, module_path in data_source_classes.items():
        _, class_name = module_path.split(":")
        class_name = class_name.replace("DataSource", "Connector")
        imports.append((module, class_name))

    template = template_env.get_template("init_template.jinja2")
    init_code = template.render(imports=imports)

    with open(init_file_path, "w") as init_file:
        init_file.write(init_code)

    format_file_in_place(
        Path(init_file_path), fast=False, mode=FileMode(), write_back=WriteBack.YES
    )


if __name__ == "__main__":
    connectors_dir = os.path.join("package", "connectors")
    env = Environment(loader=FileSystemLoader("scripts/codegen/templates"))
    generate_init_file(connectors_dir, env)
