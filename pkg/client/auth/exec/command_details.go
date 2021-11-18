package exec

// EnvVar is used for setting environment variables when executing an exec-based auth command
type EnvVar struct {
	Name  string
	Value string
}

type CommandDetails struct {
	Cmd         string
	Args        []string
	Env         []EnvVar
	Interactive bool
}
