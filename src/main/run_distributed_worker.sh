if [ $# -eq 0 ]; then
  workernum=10
fi
if [ $# -eq 1 ]; then
  workernum=$1
fi
go build -race -buildmode=plugin ../mrapps/wc2.go
# go run -race mrworker.go wc2.so
# for i in {1..$workernum}
for i in $(seq 1 $workernum)
do
  echo "run worker $i"
  go run -race mrworker.go wc2.so &
  # go run mrworker.go wc2.so &
done
