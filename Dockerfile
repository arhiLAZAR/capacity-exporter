FROM ubuntu:18.04

COPY main /app/main

ENTRYPOINT [ "sh", "-c", "while true; do sleep 1000; done" ]
