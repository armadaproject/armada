until curl -s --fail http://localhost:10080/config; do
  sleep 1;
done
mkdir .kube || true
rm .kube/config || true
wget -P .kube http://localhost:10080/config
sed -i -r 's/(\b[0-9]{1,3}\.){3}[0-9]{1,3}\b'/"localhost"/ .kube/config
