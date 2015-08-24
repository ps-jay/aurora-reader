```
docker build --tag="local/aurora" .
docker run -d -m 128m \
    -v=/opt/energy:/data:rw \
    --restart=always \
    --name=aurora local/aurora
```
