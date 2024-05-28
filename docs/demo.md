# Armada Demo

This guide will show you how to take a quick test drive of an Armada
instance already deployed to AWS EKS.

## EKS

The Armada UI (lookout) can be found at this URL:

- [https://ui.demo.armadaproject.io](https://ui.demo.armadaproject.io)

## Local prerequisites

- Git
- Go 1.21

## Obtain the armada source
Clone [this](https://github.com/armadaproject/armada) repository:

```bash
git clone https://github.com/armadaproject/armada.git
cd armada
```
All commands are intended to be run from the root of the repository.

## Armadactl configuration
Armadactl config files are structured as follows:
```yaml
currentContext: main # Default context to be used by Armadactl
contexts:
  main:
    armadaUrl: <Your Armada API endpoint>
    execAuth:
      cmd: <Your auth command>
      args:
        - <Arg>
  test:
    armadaUrl: <Your Armada API endpoint>
    execAuth:
      cmd: <Your auth command>
      args:
          - <Arg>
```

By default, armadactl assumes that a configuration file exists at `$HOME/.armadactl.yaml`. You can provide your own 
config file by specifying `--config $CONFIG_FILE_PATH` when running armadactl. 

We also support a legacy armadactl config structure, although this will soon be deprecated:
```yaml
armadaUrl: <Your Armada API endpoint>
execAuth:
  cmd: <Your auth command>
  args:
      - <Arg>
```

Under both structures, BasicAuth and various oidc auth methods are also supported. 
See ApiConnectionDetails under pkg/client/connection.go for all supported auth methods.

### Using contexts
It's quite common for Armada users to interact with multiple Armada instances, which we refer to as _contexts_. We have various 
armadactl commands that allow users to get, set and view contexts. 
```bash
armadactl config use-context test # Sets the context for future armadactl calls to "test"
armadactl config get-contexts # Gets all contexts defined in the current config
armadactl config current-context # Returns the current context
```
Contexts are not supported with the legacy armadactl configuration.

## Setup an easy-to-use alias
If you are on a Windows System, use a linux-supported terminal to run this command, for example [Git Bash](https://git-scm.com/downloads) or [Hyper](https://hyper.is/)
```bash
alias armadactl='go run cmd/armadactl/main.go'
```

## Create queues and jobs
Create queues, submit some jobs, and monitor progress:

### Queue Creation
Use a unique name for the queue. Make sure you remember it for the next steps.
```bash
armadactl create queue $QUEUE_NAME --priority-factor 1
armadactl create queue $QUEUE_NAME --priority-factor 2
```

For queues created in this way, user and group owners of the queue have permissions to:
- submit jobs
- cancel jobs
- reprioritize jobs
- watch queue

For more control, queues can be created via `armadactl create`, which allows for setting specific permission; see the following example.

```bash
armadactl create -f ./docs/quickstart/queue-a.yaml
armadactl create -f ./docs/quickstart/queue-b.yaml
```

Make sure to manually edit both of these `yaml` files using a code or text editor before running the commands above.

```
name: $QUEUE_NAME
```

### Job Submission
```
armadactl submit ./docs/quickstart/job-queue-a.yaml
armadactl submit ./docs/quickstart/job-queue-b.yaml
```

Make sure to manually edit both of these `yaml` files using a code or text editor before running the commands above.
```
queue: $QUEUE_NAME
```

### Monitor Job Progress

```bash
armadactl watch $QUEUE_NAME job-set-1
```
```bash
armadactl watch $QUEUE_NAME job-set-1
```

Try submitting lots of jobs and see queues get built and processed:

#### Windows (using Git Bash):

Use a text editor of your choice.
Copy and paste the following lines into the text editor:
```
#!/bin/bash

for i in {1..50}
do
  armadactl submit ./docs/quickstart/job-queue-a.yaml
  armadactl submit ./docs/quickstart/job-queue-b.yaml
done
```
Save the file with a ".sh" extension (e.g., myscript.sh) in the root directory of the project.
Open Git Bash, navigate to the project's directory using the 'cd' command, and then run the script by typing ./myscript.sh and pressing Enter.

#### Linux:

Open a text editor (e.g., Nano or Vim) in the terminal and create a new file by running: nano myscript.sh (replace "nano" with your preferred text editor if needed).
Copy and paste the script content from above into the text editor.
Save the file and exit the text editor.
Make the script file executable by running: chmod +x myscript.sh.
Run the script by typing ./myscript.sh in the terminal and pressing Enter.

#### macOS:

Follow the same steps as for Linux, as macOS uses the Bash shell by default.
With this approach, you create a shell script file that contains your multi-line script, and you can run it as a whole by executing the script file in the terminal.

## Observing job progress

CLI:

```bash
$ armadactl watch queue-a job-set-1
Watching job set job-set-1
Nov  4 11:43:36 | Queued:   0, Leased:   0, Pending:   0, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobSubmittedEvent, job id: 01drv3mey2mzmayf50631tzp9m
Nov  4 11:43:36 | Queued:   1, Leased:   0, Pending:   0, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobQueuedEvent, job id: 01drv3mey2mzmayf50631tzp9m
Nov  4 11:43:36 | Queued:   1, Leased:   0, Pending:   0, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobSubmittedEvent, job id: 01drv3mf7b6fd1rraeq1f554fn
Nov  4 11:43:36 | Queued:   2, Leased:   0, Pending:   0, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobQueuedEvent, job id: 01drv3mf7b6fd1rraeq1f554fn
Nov  4 11:43:38 | Queued:   1, Leased:   1, Pending:   0, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobLeasedEvent, job id: 01drv3mey2mzmayf50631tzp9m
Nov  4 11:43:38 | Queued:   0, Leased:   2, Pending:   0, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobLeasedEvent, job id: 01drv3mf7b6fd1rraeq1f554fn
Nov  4 11:43:38 | Queued:   0, Leased:   1, Pending:   1, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobPendingEvent, job id: 01drv3mey2mzmayf50631tzp9m
Nov  4 11:43:38 | Queued:   0, Leased:   0, Pending:   2, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobPendingEvent, job id: 01drv3mf7b6fd1rraeq1f554fn
Nov  4 11:43:41 | Queued:   0, Leased:   0, Pending:   1, Running:   1, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobRunningEvent, job id: 01drv3mf7b6fd1rraeq1f554fn
Nov  4 11:43:41 | Queued:   0, Leased:   0, Pending:   0, Running:   2, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobRunningEvent, job id: 01drv3mey2mzmayf50631tzp9m
Nov  4 11:44:17 | Queued:   0, Leased:   0, Pending:   0, Running:   1, Succeeded:   1, Failed:   0, Cancelled:   0 | event: *api.JobSucceededEvent, job id: 01drv3mf7b6fd1rraeq1f554fn
Nov  4 11:44:26 | Queued:   0, Leased:   0, Pending:   0, Running:   0, Succeeded:   2, Failed:   0, Cancelled:   0 | event: *api.JobSucceededEvent, job id: 01drv3mey2mzmayf50631tzp9m
```

Web UI:

Open [https://ui.demo.armadaproject.io](https://ui.demo.armadaproject.io) in your browser.

![Lookout UI](./quickstart/img/lookout.png "Lookout UI")
