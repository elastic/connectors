
if ! [ -f sampledata.archive ]; then
  curl -L -o sampledata.archive https://atlas-education.s3.amazonaws.com/sampledata.archive
fi
docker exec -i mongo sh -c 'mongorestore --drop --archive' < sampledata.archive
