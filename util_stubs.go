// +build !linux

package raft

func isBtrfs(fd uintptr) bool {
	return false
}

func setNOCOW(fd uintptr) {
}
