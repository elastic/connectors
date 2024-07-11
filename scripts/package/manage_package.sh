#!/bin/bash

set -e

function generate_connector_package_code() {
    bin/python scripts/package/codegen/generate_connectors.py
    bin/python scripts/package/codegen/generate_connectors_init.py
}

function generate_connector_package() {
    generate_connector_package_code
    mkdir -p package/elastic_connectors
    cp -r package/* package/elastic_connectors
    rm -rf package/elastic_connectors/elastic_connectors
    cp -r connectors requirements package/elastic_connectors
    bin/python scripts/package/update_imports.py
}

function clean_connector_package() {
    cd package && rm -rf elastic_connectors build dist *.egg-info
}

function build_connector_package() {
    clean_connector_package
    generate_connector_package
    cd package && ../bin/python setup.py sdist bdist_wheel
}

function publish_connector_package() {
    build_connector_package
    cd package && twine upload --repository testpypi dist/*
}

case "$1" in
    generate-connector-package-code)
        generate_connector_package_code
        ;;
    generate-connector-package)
        generate_connector_package
        ;;
    clean-connector-package)
        clean_connector_package
        ;;
    build-connector-package)
        build_connector_package
        ;;
    publish-connector-package)
        publish_connector_package
        ;;
    *)
        echo "Usage: $0 {generate-connector-package-code|generate-connector-package|clean-connector-package|build-connector-package|publish-connector-package}"
        exit 1
esac
