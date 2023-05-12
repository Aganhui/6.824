if [ $# -eq 0 ]; then
  workernum=10
fi
if [ $# -eq 1 ]; then
  workernum=$1
fi
# for i in {1..$workernum}
# for i in {1..5}
# for i in 1 2 3 4 5
# for i in $(seq 1 5)
for i in $(seq 1 $workernum)
do
  echo "run worker test $i"
done
