docker run -p 127.0.0.1:9999:9998 -v `pwd`:/files -v `pwd`/tika-config.xml:/tika-config.xml apache/tika --config /tika-config.xml
