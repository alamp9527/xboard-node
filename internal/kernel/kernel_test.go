package kernel

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

func TestUserDiffTreatsDeviceIdentityChangesAsReplace(t *testing.T) {
	oldUsers := []model.UserSpec{{ID: 11, UserID: 1, DeviceID: "ios-old", UUID: "uuid-1"}}
	newUsers := []model.UserSpec{{ID: 11, UserID: 1, DeviceID: "ios-new", UUID: "uuid-1"}}

	toAdd, toRemove := UserDiff(oldUsers, newUsers)

	if len(toAdd) != 1 || len(toRemove) != 1 {
		t.Fatalf("toAdd=%v toRemove=%v, want one replace", toAdd, toRemove)
	}
	if toAdd[0].DeviceID != "ios-new" || toRemove[0].DeviceID != "ios-old" {
		t.Fatalf("unexpected diff: add=%v remove=%v", toAdd, toRemove)
	}
}

func TestComputeHashIncludesDeviceIdentity(t *testing.T) {
	usersA := []model.UserSpec{{ID: 11, UserID: 1, DeviceID: "ios-a", UUID: "uuid-1"}}
	usersB := []model.UserSpec{{ID: 11, UserID: 1, DeviceID: "ios-b", UUID: "uuid-1"}}

	if ComputeHash(nil, usersA) == ComputeHash(nil, usersB) {
		t.Fatal("expected different hashes when device identity changes")
	}
}
