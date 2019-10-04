count=0
while [ ! -f ./done.txt ]
do
  count=`expr $count + 1`
  sleep 1
done