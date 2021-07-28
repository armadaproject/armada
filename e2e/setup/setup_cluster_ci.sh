if curl -s --fail http://127.0.0.1:10080/config; then
    exit 0
fi
docker run -d --name kube --privileged -p 8443:8443 -p 10080:10080 bsycorp/kind:v1.21.1 || true
