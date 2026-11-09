package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
)

func TestEnsureGotestsum(t *testing.T) {
	accessErr := errors.New("access denied")
	installErr := errors.New("installation failed")
	for _, tc := range []struct {
		name        string
		existing    string
		installed   string
		checkErr    error
		verifyErr   error
		installErr  error
		wantInstall bool
		wantErr     string
	}{
		{name: "absent", installed: "pinned", wantInstall: true},
		{name: "matching", existing: "pinned"},
		{name: "stale", existing: "v1.8.2", installed: "pinned", wantInstall: true},
		{name: "unidentified", existing: "unknown", installed: "pinned", wantInstall: true},
		{name: "access error", existing: "v1.8.2", checkErr: accessErr, wantErr: "checking gotestsum"},
		{name: "failed install", existing: "v1.8.2", installed: "partial", installErr: installErr, wantInstall: true, wantErr: "installing gotestsum"},
		{name: "missing installed binary", existing: "v1.8.2", wantInstall: true, wantErr: "installed gotestsum is not"},
		{name: "wrong installed version", existing: "v1.8.2", installed: "wrong", wantInstall: true, wantErr: "installed gotestsum is not"},
		{name: "unidentified installed binary", installed: "unknown", wantInstall: true, wantErr: "installed gotestsum is not"},
		{name: "verification access error", existing: "v1.8.2", installed: "pinned", verifyErr: accessErr, wantInstall: true, wantErr: "verifying installed gotestsum"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binDir := t.TempDir()
			binary := filepath.Join(binDir, "gotestsum")
			unrelated := filepath.Join(binDir, "other-tool")
			writeTestTool(t, unrelated, "preserved")
			if tc.existing != "" {
				writeTestTool(t, binary, tc.existing)
			}
			matches := func(path string) (bool, error) {
				if path == binary && tc.checkErr != nil {
					return false, tc.checkErr
				}
				if path != binary && tc.verifyErr != nil {
					return false, tc.verifyErr
				}
				data, err := os.ReadFile(path)
				if os.IsNotExist(err) {
					return false, nil
				}
				return string(data) == "pinned", err
			}
			installs := 0
			install := func(dir string) error {
				installs++
				if filepath.Dir(dir) != binDir || dir == binDir {
					t.Fatalf("installer must use owned staging directory, got %s", dir)
				}
				if tc.installed != "" {
					writeTestTool(t, filepath.Join(dir, "gotestsum"), tc.installed)
				}
				return tc.installErr
			}
			err := ensureGotestsum(binary, matches, install)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected %q error, got %v", tc.wantErr, err)
			}
			for _, cause := range []error{tc.checkErr, tc.verifyErr, tc.installErr} {
				if cause != nil && !errors.Is(err, cause) {
					t.Errorf("error %v does not wrap %v", err, cause)
				}
			}
			wantInstalls := 0
			if tc.wantInstall {
				wantInstalls = 1
			}
			if installs != wantInstalls {
				t.Errorf("got %d installs, want %d", installs, wantInstalls)
			}
			want := tc.existing
			if tc.wantErr == "" {
				want = "pinned"
			}
			data, readErr := os.ReadFile(binary)
			if want == "" {
				if !os.IsNotExist(readErr) {
					t.Errorf("failed update created managed binary: %q, %v", data, readErr)
				}
			} else if readErr != nil || string(data) != want {
				t.Errorf("managed binary = %q, %v; want %q", data, readErr, want)
			}
			data, err = os.ReadFile(unrelated)
			if err != nil || string(data) != "preserved" {
				t.Errorf("unrelated tool changed: %q, %v", data, err)
			}
			entries, err := os.ReadDir(binDir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), ".gotestsum-") {
					t.Errorf("staging directory left behind: %s", entry.Name())
				}
			}
		})
	}
}

func TestEnsureGotestsumCreatesBinDirectory(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "bin", "gotestsum")
	matches := func(path string) (bool, error) {
		data, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			return false, nil
		}
		return string(data) == "pinned", err
	}
	if err := ensureGotestsum(binary, matches, func(dir string) error {
		return os.WriteFile(filepath.Join(dir, "gotestsum"), []byte("pinned"), 0o755)
	}); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(binary); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("binary not created at %s: %v", binary, err)
	}
}

// TestIsPinnedGotestsumInstalled installs the real pinned tool and expects a match.
func TestIsPinnedGotestsumInstalled(t *testing.T) {
	if testing.Short() {
		t.Skip("installing gotestsum needs the module cache or network")
	}
	dir := t.TempDir()
	if err := installGotestsum(dir); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, gotestsumBinaryName())
	matches, err := isPinnedGotestsum(binary)
	if err != nil || !matches {
		t.Fatalf("got (%v, %v), want match=true", matches, err)
	}
	if runtime.GOOS == "windows" {
		return // Windows has no exec permission bits to check.
	}
	if err := os.Chmod(binary, 0o644); err != nil {
		t.Fatal(err)
	}
	matches, err = isPinnedGotestsum(binary)
	if err != nil || matches {
		t.Fatalf("non-executable binary: got (%v, %v), want match=false", matches, err)
	}
}

// TestIsPinnedGotestsumLocalBuild checks that a Go binary that is not the pinned tool is rejected.
func TestIsPinnedGotestsumLocalBuild(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "main.go")
	if err := os.WriteFile(src, []byte("package main\n\nfunc main() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, gotestsumBinaryName())
	build := exec.Command("go", "build", "-o", binary, src)
	build.Dir = dir
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building test binary: %v\n%s", err, out)
	}
	matches, err := isPinnedGotestsum(binary)
	if err != nil || matches {
		t.Fatalf("got (%v, %v), want match=false", matches, err)
	}
}

func TestIsPinnedGotestsum(t *testing.T) {
	for _, name := range []string{"absent", "unidentified", "directory", "unreadable"} {
		t.Run(name, func(t *testing.T) {
			binary := filepath.Join(t.TempDir(), "gotestsum")
			wantErr := false
			switch name {
			case "unidentified":
				writeTestTool(t, binary, "not a Go executable")
			case "directory":
				if err := os.Mkdir(binary, 0o755); err != nil {
					t.Fatal(err)
				}
				wantErr = true
			case "unreadable":
				writeTestTool(t, binary, "not a Go executable")
				if err := os.Chmod(binary, 0); err != nil {
					t.Fatal(err)
				}
				if _, err := os.ReadFile(binary); err == nil {
					t.Skip("user can read files without permission bits")
				}
				wantErr = true
			}
			matches, err := isPinnedGotestsum(binary)
			if matches || (err != nil) != wantErr {
				t.Fatalf("got (%v, %v), want match=false, error=%v", matches, err, wantErr)
			}
		})
	}
}

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

func writeTestTool(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
		t.Fatal(err)
	}
}
