//go:build !windows

package runtime

import (
	"os"
	"syscall"
)

func checkProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	// Sending signal 0 performs error checking without actually sending a signal.
	// If the process exists, err is nil.
	err = proc.Signal(syscall.Signal(0))
	return err == nil
}
