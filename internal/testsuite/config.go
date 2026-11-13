package testsuite

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/pkg/api"
)

// ConfigHook applies the config overrides in dir to the Armada deployment under test. The directory has one file per
// component, named <component>.yaml. An empty directory restores the config of the deployment without overrides.
type ConfigHook func(ctx context.Context, dir string) error

// CommandConfigHook returns a ConfigHook that runs the executable at path with the directory as its only argument.
func CommandConfigHook(path string, out io.Writer) ConfigHook {
	return func(ctx context.Context, dir string) error {
		cmd := exec.CommandContext(ctx, path, dir)
		cmd.Stdout = out
		cmd.Stderr = out
		if err := cmd.Run(); err != nil {
			return errors.Wrapf(err, "config hook %s failed", path)
		}
		return nil
	}
}

// configRestoreTimeout limits the removal of the overrides at the end of a run. The removal recreates components.
const configRestoreTimeout = 5 * time.Minute

// configGroup holds the test cases that need the same config overrides. A config change applies to the whole
// deployment, so the test cases of different groups cannot run at the same time.
type configGroup struct {
	// key is the canonical JSON of the overrides, and it is empty for test cases without overrides.
	key    string
	config map[string]*types.Struct
	// indexes are the positions of the test cases in the input.
	indexes []int
}

// groupByConfig returns the test cases grouped by their config overrides. The group without overrides comes first.
// The other groups follow in the order of their keys, so each run applies the configs in the same order.
func groupByConfig(testSpecs []*api.TestSpec) ([]*configGroup, error) {
	groupsByKey := map[string]*configGroup{}
	for i, testSpec := range testSpecs {
		key, err := canonicalConfig(testSpec.Config)
		if err != nil {
			return nil, errors.WithMessagef(err, "invalid config of test case %s", testSpec.Name)
		}
		group, ok := groupsByKey[key]
		if !ok {
			group = &configGroup{key: key, config: testSpec.Config}
			groupsByKey[key] = group
		}
		group.indexes = append(group.indexes, i)
	}
	groups := make([]*configGroup, 0, len(groupsByKey))
	for _, group := range groupsByKey {
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].key < groups[j].key })
	return groups, nil
}

// componentNamePattern restricts component names, because each name becomes a file name in the hook directory.
var componentNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// canonicalConfig returns the overrides as JSON with sorted keys, so two test cases with the same overrides get the
// same key. It returns "" when there are no overrides.
func canonicalConfig(config map[string]*types.Struct) (string, error) {
	if len(config) == 0 {
		return "", nil
	}
	values := make(map[string]any, len(config))
	for component, override := range config {
		if !componentNamePattern.MatchString(component) {
			return "", errors.Errorf("component name %q must match %s", component, componentNamePattern)
		}
		value, err := structToValue(override)
		if err != nil {
			return "", err
		}
		values[component] = value
	}
	// encoding/json writes the keys of a map in sorted order.
	key, err := json.Marshal(values)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return string(key), nil
}

// writeConfigFiles writes one <component>.yaml file per override into dir. JSON is valid YAML, so the components
// read the files as config files.
func writeConfigFiles(dir string, config map[string]*types.Struct) error {
	for component, override := range config {
		value, err := structToValue(override)
		if err != nil {
			return err
		}
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return errors.WithStack(err)
		}
		if err := os.WriteFile(filepath.Join(dir, component+".yaml"), data, 0o644); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// configFromDocs returns the config overrides of a test case. Each YAML document can hold the config key.
func configFromDocs(docs [][]byte) (map[string]*types.Struct, error) {
	var config map[string]*types.Struct
	for _, doc := range docs {
		docJson, err := yaml.YAMLToJSON(doc)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		var parsed struct {
			Config map[string]json.RawMessage `json:"config"`
		}
		if err := json.Unmarshal(docJson, &parsed); err != nil {
			return nil, errors.WithStack(err)
		}
		for component, raw := range parsed.Config {
			override := &types.Struct{}
			if err := jsonpb.UnmarshalString(string(raw), override); err != nil {
				return nil, errors.WithMessagef(err, "invalid config of component %s", component)
			}
			if config == nil {
				config = map[string]*types.Struct{}
			}
			config[component] = override
		}
	}
	return config, nil
}

func structToValue(s *types.Struct) (any, error) {
	if s == nil {
		return map[string]any{}, nil
	}
	var buf bytes.Buffer
	if err := (&jsonpb.Marshaler{}).Marshal(&buf, s); err != nil {
		return nil, errors.WithStack(err)
	}
	var value any
	if err := json.Unmarshal(buf.Bytes(), &value); err != nil {
		return nil, errors.WithStack(err)
	}
	return value, nil
}

// applyConfig writes the overrides into a temporary directory and calls the hook with it. Empty overrides restore
// the config of the deployment.
func applyConfig(ctx context.Context, hook ConfigHook, config map[string]*types.Struct) error {
	dir, err := os.MkdirTemp("", "armada-testsuite-config-")
	if err != nil {
		return errors.WithStack(err)
	}
	defer os.RemoveAll(dir)
	if err := writeConfigFiles(dir, config); err != nil {
		return err
	}
	return hook(ctx, dir)
}
