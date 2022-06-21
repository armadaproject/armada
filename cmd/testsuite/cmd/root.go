package cmd

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/G-Research/armada/internal/testsuite"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

// RootCmd is the root Cobra command that gets called from the main func.
// All other sub-commands should be registered here.
func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "testsuite",
		Short: "testsuite contains a suite of automated tests for Armada clusters.",
		Long: `testsuite contains a suite of automated tests for Armada clusters.


Persistent config can be saved in a config file so it doesn't have to be specified every command.

Example structure:
armadaUrl: localhost:50051
basicAuth:
username: user1
password: password123

The location of this file can be passed in using the --config argument.
If not provided, $HOME/.armadactl.yaml is used.`}

	client.AddArmadaApiConnectionCommandlineArgs(cmd)

	cmd.AddCommand(
		submitCmd(testsuite.New()),
		versionCmd(testsuite.New()),
		junitCmd(testsuite.New()),
	)

	return cmd
}

// Print version info and exit.
func versionCmd(app *testsuite.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, app)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.Version()
		},
	}
	return cmd
}

// Print version info and exit.
func junitCmd(app *testsuite.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "junit",
		Short: "Print version.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, app)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			foo := &junit.Testsuites{}
			fmt.Println(foo)
			xmlBytes, err := xml.MarshalIndent(foo, "", "\t")
			if err != nil {
				return err
			}

			// I can write junit xml reports
			// Now I need to create the testsuites object
			// TestSuites - TestSuite - TestCase - Result
			// Create a single TestSuites
			// If there are files in the root directory, create a TestSuite with the name of the root directory
			// Create a TestSuite for each sub-directory
			// Create a TestCase + Result for each file

			// Let's write code that creates a TestCase for each file in a directory

			testFilesPattern, err := cmd.Flags().GetString("tests")
			if err != nil {
				return err
			}

			testFiles, err := filepath.Glob(testFilesPattern)
			if err != nil {
				return err
			}
			for _, testFile := range testFiles {
				fmt.Println(testFile)
			}

			if err = os.WriteFile("foo.xml", xmlBytes, 0644); err != nil {
				return err
			}
			return nil
		},
	}

	cmd.Flags().String("tests", "", "Test file pattern, e.g., './testcases/*.yaml'.")

	return cmd
}

// Submit batches of jobs and wait for those jobs to finish.
// Prints job completion statistics on exit.
func submitCmd(app *testsuite.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit batches of jobs and wait for them to complete.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, app)
		},
		RunE: func(cmd *cobra.Command, args []string) error {

			testFilePath, err := cmd.Flags().GetString("testFile")
			if err != nil {
				return err
			}

			testSpec := &api.TestSpec{}
			yamlBytes, err := ioutil.ReadFile(testFilePath)
			if err != nil {
				return errors.WithStack(err)
			}
			if err := testsuite.UnmarshalTestCase(yamlBytes, testSpec); err != nil {
				return errors.WithStack(err)
			}

			fmt.Printf("%#v\n", testSpec)
			fmt.Printf("%#v\n", testSpec.Jobs[0])
			fmt.Printf("%#v\n", testSpec.ExpectedEvents[0])
			fmt.Printf("%#v\n", testSpec.ExpectedEvents[0].GetSubmitted())

			// TODO: Make an argument?
			testSpec.JobSetId = uuid.New().String()

			return app.Submit(testSpec)

			// expectedEventsPath, err := cmd.Flags().GetString("expectedFile")
			// if err != nil {
			// 	return err
			// }

			// reader, err := os.Open(filePath)
			// if err != nil {
			// 	return err
			// }

			// event := &api.EventMessage{}

			// yamlBytes, err := ioutil.ReadFile(expectedEventsPath)
			// if err != nil {
			// 	return err
			// }
			// jsonBytes, _ := yaml.YAMLToJSON(yamlBytes)
			// fmt.Println(string(jsonBytes))
			// err = jsonpb.UnmarshalString(string(jsonBytes), event)
			// if err != nil {
			// 	return err
			// }

			// err = util.BindJsonOrYaml(expectedEventsPath, event)
			// if err != nil {
			// 	return err
			// }

			// fmt.Printf("%#v\n", event)
		},
	}

	cmd.Flags().String("testFile", "", "Path to test file.")

	return cmd
}

// func unmarshalTestCase(yamlBytes []byte, testSpec *api.TestSpec) error {
// 	docs := bytes.Split(yamlBytes, []byte("---"))
// 	for _, docYamlBytes := range docs {
// 		if err := yaml.Unmarshal(docYamlBytes, testSpec); err != nil {
// 			fmt.Println(err)
// 			// continue
// 		}

// 		docJsonBytes, err := yaml.YAMLToJSON(docYamlBytes)
// 		if err != nil {
// 			fmt.Println(err)
// 			continue
// 			// return errors.WithStack(err)
// 		}
// 		fmt.Println(string(docJsonBytes))
// 		// err = jsonpb.UnmarshalString(string(jsonBytes), testSpec)
// 		unmarshaler := jsonpb.Unmarshaler{AllowUnknownFields: true}
// 		err = unmarshaler.Unmarshal(bytes.NewReader(docJsonBytes), testSpec)
// 		// err = jsonpb.UnmarshalString(string(yamlBytes), testSpec)
// 		// err = protojson.Unmarshal(jsonBytes, testSpec)
// 		if err != nil {
// 			fmt.Println("Unmarshalling error")
// 			fmt.Printf("%#v\n", testSpec)
// 			// return errors.WithStack(err)
// 		}
// 	}
// 	return nil
// }

// // Submit batches of jobs and wait for those jobs to finish.
// // Prints job completion statistics on exit.
// func submitCmd(app *testsuite.App) *cobra.Command {
// 	cmd := &cobra.Command{
// 		Use:   "submit",
// 		Short: "Submit batches of jobs and wait for them to complete.",
// 		PreRunE: func(cmd *cobra.Command, args []string) error {
// 			return initParams(cmd, app)
// 		},
// 		RunE: func(cmd *cobra.Command, args []string) error {

// 			jobFilePath, err := cmd.Flags().GetString("jobFile")
// 			if err != nil {
// 				return err
// 			}

// 			numBatches, err := cmd.Flags().GetUint("numBatches")
// 			if err != nil {
// 				return err
// 			}

// 			batchSize, err := cmd.Flags().GetUint("batchSize")
// 			if err != nil {
// 				return err
// 			}

// 			timeout, err := cmd.Flags().GetFloat64("timeout")
// 			if err != nil {
// 				return err
// 			}

// 			submitInterval, err := cmd.Flags().GetFloat64("submitInterval")
// 			if err != nil {
// 				return err
// 			}

// 			randomizeJobSet, err := cmd.Flags().GetBool("randomizeJobSet")
// 			if err != nil {
// 				return err
// 			}

// 			ok, err := validation.ValidateSubmitFile(jobFilePath)
// 			if !ok {
// 				return err
// 			}

// 			jobFile := &domain.JobSubmitFile{}
// 			err = util.BindJsonOrYaml(jobFilePath, jobFile)
// 			if err != nil {
// 				return err
// 			}

// 			if randomizeJobSet {
// 				jobFile.JobSetId = uuid.New().String()
// 			}
// 			return app.Submit(&testsuite.SubmitConfig{
// 				JobFile:    jobFile,
// 				NumBatches: numBatches,
// 				BatchSize:  batchSize,
// 				Timeout:    time.Duration(int64(timeout*1e9)) * time.Nanosecond,
// 				Interval:   time.Duration(int64(submitInterval*1e9)) * time.Nanosecond,
// 			})
// 		},
// 	}

// 	cmd.Flags().String("jobFile", "", "Path to job file to submit.")
// 	cmd.Flags().Uint("numBatches", 1, "Number of job batches to submit. Submits forever if set to zero.")
// 	cmd.Flags().Uint("batchSize", 1, "Number of jobs per batch.")
// 	cmd.Flags().Float64("timeout", 0, "Number of seconds to wait for jobs to finish.")
// 	cmd.Flags().Float64("submitInterval", 0.01, "Number of seconds between submitting batches of jobs.")
// 	cmd.Flags().Bool("randomizeJobSet", true, "Whether to replace the job set with a randomly generated string.")

// 	return cmd
// }

func initParams(cmd *cobra.Command, app *testsuite.App) error {
	client.LoadCommandlineArgs()
	app.Params.ApiConnectionDetails = client.ExtractCommandlineArmadaApiConnectionDetails()
	return nil
}
