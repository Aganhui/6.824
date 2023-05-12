go build -race -buildmode=plugin ../mrapps/wc.go
# rm mr-out*
go run -race mrsequential.go wc.so ./pg-save/pg*.txt
