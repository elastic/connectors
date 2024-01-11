mkdir -p /var/app/

git clone https://github.com/elastic/connectors.git /var/app
cd /var/app
git clean -ffxdq
git fetch -v --prune -- origin $1
git checkout -f $1

make clean install
chmod a+rxw /var/app -R
