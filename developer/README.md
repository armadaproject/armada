# Dependencies (TEMPORARY)

## Mage

```bash
go install github.com/magefile/mage@v1.14.0
```

## Protoc

```bash
VERSION=22.0
ARCH=linux-x86_64
PROTOC_ZIP=protoc-$VERSION-linux-x86_64.zip

```
curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v$VERSION/protoc-$VERSION-linux-x86_64.zip
sudo unzip -o $PROTOC_ZIP -d /usr/local bin/protoc
sudo unzip -o $PROTOC_ZIP -d /usr/local 'include/*'
rm -f $PROTOC_ZIP
```