echo "---\nUninstalling connectors_sdk\n---"
pip uninstall connectors_sdk
echo "---\nRemoving dist/ to be sure we nuked it\n---"
rm -rf dist
echo "---\nBuilding connectors_sdk with hatch\n---"
hatch build
echo "---\nInstalling connectors_sdk\n---"
pip install dist/connectors_sdk-0.1.0.tar.gz
