// +build linux
// +build cgo

package raft

import (
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestSetNOCOW(t *testing.T) {
	f, err := ioutil.TempFile("/", "test")
	if err != nil {
		t.Fatal("Failed creating tempfile")
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	if isBtrfs(f.Fd()) {
		setNOCOW(f.Fd())
		cmd := exec.Command("lsattr", f.Name())
		out, err := cmd.Output()
		if err != nil {
			t.Fatal("Failed executing lsattr")
		}
		if !strings.Contains(string(out), "---------------C") {
			t.Fatal("Failed setting NOCOW:\n", out)
		}
	}
}
