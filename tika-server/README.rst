Tika Server
===========

Runs a dockerized version of Tika Server with `FileSystemFetcher` enabled.

To run it, use:

```
./run.sh
```

You can drop files in this directory and send them to the service with:

```
curl -X PUT http://localhost:9998/tika --header "fetcherName: fsf" --header "Accept: text/plain" --header "fetchKey: /files/<FILENAME>"
```

Where FILENAME is your file name.

The Sharepoint connector is plugged to use that service in this branch.
