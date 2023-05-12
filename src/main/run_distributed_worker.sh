go build -race -buildmode=plugin ../mrapps/wc2.go
go run -race mrworker.go wc2.so
