package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func between(min int, max int) time.Duration {
	return time.Duration(rand.Intn(max-min)+min) * time.Millisecond
}
