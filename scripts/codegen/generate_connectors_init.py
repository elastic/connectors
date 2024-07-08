import os
from jinja2 import Environment, FileSystemLoader
from black import format_file_in_place, FileMode, WriteBack, NothingChanged
from pathlib import Path

def generate_init_file(output_dir, generated_dir, template_env):
    init_file_path = os.path.join(output_dir, '__init__.py')
    imports = []

    for filename in os.listdir(generated_dir):
        if filename.endswith('.py') and filename != '__init__.py':
            module_name = filename[:-3]
            class_name = ''.join(word.title() for word in module_name.split('_')) + 'Connector'
            imports.append((module_name, class_name))

    template = template_env.get_template("init_template.jinja2")
    init_code = template.render(imports=imports)

    with open(init_file_path, 'w') as init_file:
        init_file.write(init_code)

    format_file_in_place( Path(init_file_path), fast=False, mode=FileMode(), write_back=WriteBack.YES)


if __name__ == "__main__":
    connectors_dir = os.path.join("package", "connectors")
    generated_dir = os.path.join(connectors_dir, "generated")
    env = Environment(loader=FileSystemLoader("scripts/codegen/templates"))
    generate_init_file(connectors_dir, generated_dir, env)
