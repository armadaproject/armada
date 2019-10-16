mkdir .kube || true
rm .kube/config || true
wget -P .kube http://localhost:10080/config
