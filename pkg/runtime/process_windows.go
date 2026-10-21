//go:build windows

package runtime

import (
	"syscall"
)

const (
	processQueryLimitedInformation = 0x1000
	stillActive                    = 259
)

func checkProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}

	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(h)

	var exitCode uint32
	err = syscall.GetExitCodeProcess(h, &exitCode)
	if err != nil {
		return false
	}

	return exitCode == stillActive
}
