import os
import importlib
from jinja2 import Environment, FileSystemLoader
from black import format_file_in_place, FileMode, WriteBack
from pathlib import Path

from connectors.config import _default_config


def generate_wrapper_class_code(template_env, data_source_class, class_name):
    config = data_source_class.get_default_configuration()
    base_class_name = data_source_class.__name__

    params = [(key, value.get("value", None)) for key, value in config.items()]

    template = template_env.get_template("datasource_wrapper.jinja2")
    class_code = template.render(
        class_name=class_name,
        base_class_name=base_class_name,
        params=params,
        config=config,
    )
    return class_code


def write_class_to_file(class_code, class_name, output_dir):
    file_path = os.path.join(output_dir, f"{class_name.lower()}.py")
    with open(file_path, "w") as file:
        file.write(class_code)
    format_file_in_place(Path(file_path), fast=False, mode=FileMode(), write_back=WriteBack.YES)


def generate_and_write_wrapper_classes(sources, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    env = Environment(loader=FileSystemLoader("scripts/codegen/templates"))

    for key, value in sources.items():
        module_name, class_name = value.split(":")
        module = importlib.import_module(module_name)
        data_source_class = getattr(module, class_name)
        class_code = generate_wrapper_class_code(env, data_source_class, class_name)
        write_class_to_file(class_code, key, output_dir)


# Example usage
connectors_config = _default_config()
data_source_classes = connectors_config["sources"]
output_dir = os.path.join("package", "generated")
generate_and_write_wrapper_classes(data_source_classes, output_dir)
