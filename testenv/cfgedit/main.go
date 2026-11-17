// Command cfgedit sets values in a YAML file without losing its comments.
//
// Usage: cfgedit <file> <path>=<yaml value> ...
//
// <path> is a "/"-separated list of mapping keys ("double_puppet/secrets"). The value is parsed as YAML, so a map or
// a list replaces the whole node ("bridge/permissions={\"localhost\": admin}"). Every key must already exist, which
// keeps typos from silently adding unused settings. testenv/dev.sh uses it to fill in the bridge config that
// `traq-matrix-bridge -e` generates.
//
// Secrets must not be passed on the command line (other users can read it from /proc): write "${NAME}" in a value
// and cfgedit substitutes the environment variable NAME before parsing it. $NAME is expanded the same way, an unset
// variable is an error, and values must not contain a literal "$".
package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: cfgedit <file> <path>=<yaml value> ...\n  ${NAME} in a value is replaced by the environment variable NAME (keeps secrets off the command line)")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "cfgedit:", err)
		os.Exit(1)
	}
}

func run(file string, assignments []string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("%s: %w", file, err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 {
		return fmt.Errorf("%s: not a single YAML document", file)
	}
	for _, a := range assignments {
		path, value, ok := strings.Cut(a, "=")
		if !ok {
			return fmt.Errorf("%q: want <path>=<yaml value>", a)
		}
		var missing string
		value = os.Expand(value, func(name string) string {
			v, set := os.LookupEnv(name)
			if !set {
				missing = name
			}
			return v
		})
		if missing != "" {
			return fmt.Errorf("%s: environment variable %s is not set", path, missing)
		}
		if err := set(doc.Content[0], strings.Split(path, "/"), value); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(4)
	if err := enc.Encode(&doc); err != nil {
		return err
	}
	if err := enc.Close(); err != nil {
		return err
	}
	return os.WriteFile(file, buf.Bytes(), 0o600)
}

// set replaces the value of the mapping entry at path under n.
func set(n *yaml.Node, path []string, value string) error {
	if n.Kind != yaml.MappingNode {
		return fmt.Errorf("%q is not under a mapping", path[0])
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value != path[0] {
			continue
		}
		if len(path) > 1 {
			return set(n.Content[i+1], path[1:], value)
		}
		var parsed yaml.Node
		if err := yaml.Unmarshal([]byte(value), &parsed); err != nil {
			return fmt.Errorf("value %q: %w", value, err)
		}
		if parsed.Kind != yaml.DocumentNode || len(parsed.Content) != 1 {
			return fmt.Errorf("value %q: not a single YAML value", value)
		}
		old := n.Content[i+1]
		repl := parsed.Content[0]
		repl.HeadComment, repl.LineComment, repl.FootComment = old.HeadComment, old.LineComment, old.FootComment
		n.Content[i+1] = repl
		return nil
	}
	return fmt.Errorf("key %q not found", path[0])
}
