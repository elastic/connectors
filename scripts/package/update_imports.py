#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os
import re


def update_imports(directory, old_import, new_import):
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                # Replace old import with new import
                updated_content = re.sub(
                    r"\b" + old_import + r"\b", new_import, content
                )

                if content != updated_content:
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(updated_content)
                    print(f"Updated imports in {file_path}")


if __name__ == "__main__":
    # Update these paths and import strings as necessary
    old_import = "from connectors"
    new_import = "from elastic_connectors.connectors"

    update_imports(
        os.path.join("package", "elastic_connectors"), old_import, new_import
    )
