workernum=30
pgpath=./

# go build -race -buildmode=plugin ../mrapps/wc2.go
go build -buildmode=plugin ../mrapps/wc2.go
rm mr-out*
# go run -race mrcoordinator.go $pgpath/pg-*.txt &
go run mrcoordinator.go $pgpath/pg-*.txt &
for i in {1..$workernum}
do
  echo "run worker $i"
  # go run -race mrworker.go wc2.so &
  go run mrworker.go wc2.so &
done
