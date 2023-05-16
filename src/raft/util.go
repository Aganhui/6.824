package raft

import (
	"log"
	"math/rand"
	"time"

	"github.com/rs/xid"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

var (
	LeaderHeartbeatInterval = time.Millisecond * 500
	TimeoutRandomRangeMin   = time.Millisecond * 3000
	TimeoutRandomRangeMax   = time.Millisecond * 6000
	TimeoutForCollectVote   = time.Millisecond * 500
)

func GetUid() string {
	return xid.New().String()
}

func GetRandomTimeoutDuration() time.Duration {
	return TimeoutRandomRangeMin + time.Duration(rand.Float64()*float64(TimeoutRandomRangeMax-TimeoutRandomRangeMin))
}

var (
	NullPeerID = -1
)

var (
	StrStatusFollower  = "follower"
	StrStatusCandidate = "candidate"
	StrStatusLeader    = "leader"
)

var (
	EmptyInt = -2
	EmptyStr = "null"
)

var (
	ReturnBreak   = -2
	ReturnFailed  = -1
	ReturnSucceed = 0
)
