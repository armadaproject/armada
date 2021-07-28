if curl -s --fail http://127.0.0.1:10080/kubernetes-ready; then
    exit 0
fi
docker stop kube && docker rm kube || true
docker run -d --name kube --privileged -p 8443:8443 -p 10080:10080 bsycorp/kind:v1.17.9 || true
