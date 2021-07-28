until curl -s --fail http://localhost:10080/config; do
  sleep 1;
done
mkdir .kube || true
rm .kube/config || true
wget -P .kube http://localhost:10080/config
