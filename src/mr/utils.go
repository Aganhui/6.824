package mr

import (
	"time"

	"github.com/rs/xid"
)

func Strlist2Interfacelist(x []string) []interface{} {
	y := make([]interface{}, len(x))
	for i, v := range x {
		y[i] = v
	}
	return y
}

func GetUnixTimeNow() int64 {
	return time.Now().Unix()
}

func Getuid() string {
	return xid.New().String()
}
