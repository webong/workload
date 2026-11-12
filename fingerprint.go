package workload

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

// A fingerprint binds the exact spec revision, semantic configuration, launch
// and driver settings. DesiredState/lease expiry are not execution configuration.
func launchFingerprint(spec Spec, launch LaunchSpec, driver any) string {
	var configuration any
	if len(spec.Configuration) != 0 {
		decoder := json.NewDecoder(bytes.NewReader(spec.Configuration))
		decoder.UseNumber()
		_ = decoder.Decode(&configuration)
	}
	encoded, _ := json.Marshal(struct {
		ID, Assignment, Target, Kind string
		Revision                     int64
		Configuration                any
		Launch                       LaunchSpec
		Driver                       any
	}{spec.ID, spec.AssignmentID, spec.TargetID, spec.Type, spec.Revision, configuration, launch, driver})
	return identityHash(string(encoded))[:56]
}

func dockerFingerprint(c DockerDriverConfig, s Spec, l LaunchSpec) (string, error) {
	var content string
	if c.EnvironmentFile != "" {
		b, err := os.ReadFile(c.EnvironmentFile)
		if err != nil {
			return "", fmt.Errorf("read Docker environment file: %w", err)
		}
		content = identityHash(string(b))
	}
	return launchFingerprint(s, l, struct {
		Config          DockerDriverConfig
		EnvironmentHash string
	}{c, content}), nil
}
