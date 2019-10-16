mkdir .kube
rm .kube/config
wget -P .kube http://localhost:10080/config
