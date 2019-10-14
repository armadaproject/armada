docker run -d --name kube --privileged -p 8443:8443 -p 10080:10080 bsycorp/kind:v1.15.1
until curl -s --fail http://127.0.0.1:10080/kubernetes-ready; do
  sleep 1;
done
