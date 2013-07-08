package raft

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

// Writes to standard error.
func warn(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}

func randBetween(min time.Duration, max time.Duration) time.Duration {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, (max - min)
	if delta > 0 {
		d += time.Duration(rand.Int63n(int64(delta)))
	}
	return d
}
