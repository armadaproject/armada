package main

import (
	"runtime/debug"
	"testing"
)

func TestIsPinnedGotestsumBuild(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    string
		module  string
		version string
		replace *debug.Module
		want    bool
	}{
		{name: "matching", path: gotestsumModule, module: gotestsumModule, version: gotestsumVersion, want: true},
		{name: "stale", path: gotestsumModule, module: gotestsumModule, version: "v1.8.2"},
		{name: "wrong command", path: "example.com/other", module: gotestsumModule, version: gotestsumVersion},
		{name: "wrong module", path: gotestsumModule, module: "example.com/other", version: gotestsumVersion},
		{name: "development", path: gotestsumModule, module: gotestsumModule, version: "(devel)"},
		{name: "missing version", path: gotestsumModule, module: gotestsumModule},
		{name: "replacement", path: gotestsumModule, module: gotestsumModule, version: gotestsumVersion, replace: &debug.Module{Path: "example.com/other"}},
		{name: "empty metadata"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &debug.BuildInfo{
				Path: tc.path,
				Main: debug.Module{Path: tc.module, Version: tc.version, Replace: tc.replace},
			}
			if got := isPinnedGotestsumBuild(metadata); got != tc.want {
				t.Errorf("isPinnedGotestsumBuild(%+v) = %v, want %v", metadata, got, tc.want)
			}
		})
	}
}
