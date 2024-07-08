#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os
import importlib
from jinja2 import Environment, FileSystemLoader
from black import format_file_in_place, FileMode, WriteBack
from pathlib import Path

from connectors.config import _default_config

# service-specific configuration keys that should not be exposed in the packaged class
CONFIG_KEYS_TO_SKIP = {"use_document_level_security", "use_text_extraction_service"}


def generate_wrapper_class_code(
    template_env, data_source_class, data_source_module, class_name
):
    config = data_source_class.get_default_configuration()

    # remove keys that should not be exposed in the packaged class
    connector_config = {
        key: value for key, value in config.items() if key not in CONFIG_KEYS_TO_SKIP
    }

    constructor_args = [
        (key, value.get("value", value.get("default_value", None)))
        for key, value in connector_config.items()
    ]

    template = template_env.get_template("connector_template.jinja2")
    class_code = template.render(
        class_name=class_name.replace('DataSource', 'Connector'),
        data_source_class=data_source_class.__name__,
        data_source_module=data_source_module,
        params=constructor_args,
        config=connector_config,
    )
    return class_code


def write_class_to_file(class_code, class_name, output_dir):
    file_path = os.path.join(output_dir, f"{class_name.lower()}.py")
    with open(file_path, "w") as file:
        file.write(class_code)
    format_file_in_place(
        Path(file_path), fast=False, mode=FileMode(), write_back=WriteBack.YES
    )


def generate_and_write_wrapper_classes(sources, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    env = Environment(loader=FileSystemLoader("scripts/codegen/templates"))

    for key, value in sources.items():
        module_name, class_name = value.split(":")
        module = importlib.import_module(module_name)
        data_source_class = getattr(module, class_name)
        class_code = generate_wrapper_class_code(
            template_env=env,
            data_source_class=data_source_class,
            data_source_module=module_name,
            class_name=class_name,
        )
        write_class_to_file(class_code, key, output_dir)


if __name__ == "__main__":
    connectors_config = _default_config()
    data_source_classes = connectors_config["sources"]
    output_dir = os.path.join("package", "connectors", "generated")
    generate_and_write_wrapper_classes(data_source_classes, output_dir)
