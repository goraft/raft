// +build linux
// +build cgo

package raft

import (
	"syscall"
	"unsafe"
)

/*
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <linux/magic.h>

int myioctl(int d, unsigned long request, int *attr) {
	return ioctl(d, request, attr);
}
*/
import "C"

// isBtrfs checks whether the file is in btrfs
func isBtrfs(fd uintptr) bool {
	var buf syscall.Statfs_t
	if err := syscall.Fstatfs(int(fd), &buf); err != nil {
		debugln("syscall.statfs.fail ", err)
		return false
	}
	traceln("syscall.statfs.type ", buf.Type)
	// #define BTRFS_SUPER_MAGIC       0x9123683E
	if buf.Type != C.BTRFS_SUPER_MAGIC {
		return false
	}
	debugln("syscall.statfs.is.btrfs")
	return true
}

// setNOCOW sets NOCOW flag for the file
func setNOCOW(fd uintptr) {
	var attr int
	if C.myioctl(C.int(fd), C.FS_IOC_GETFLAGS, (*C.int)(unsafe.Pointer(&attr))) != 0 {
		debugln("ioctl.get.flags.fail")
		return
	}
	attr |= C.FS_NOCOW_FL
	if C.myioctl(C.int(fd), C.FS_IOC_SETFLAGS, (*C.int)(unsafe.Pointer(&attr))) != 0 {
		debugln("ioctl.set.flags.fail")
		return
	}
	debugln("ioctl.set.nocow.succeed")
}
