// Package mpibootstrap starts the runtime daemons of an MPI job on the workers of an Armada MPI job without a
// remote shell.
//
// MPI launchers such as mpirun start one daemon on every node of a job, and by default they do so over ssh. That
// needs an ssh server, keys and usually root in every image. In an Armada MPI job the workers are already running,
// so this package turns the connection around: every worker calls home to the launcher and waits to be told which
// daemon to run. Nothing listens on a worker, and the launcher never executes anything a worker sends it.
//
// The package has three parts, all served by the armada-mpi binary:
//   - Launch runs in the launcher. It accepts the connections of the workers, waits until all of them have called
//     home, and then runs the command of the user, typically mpirun.
//   - Agent is what mpirun calls in place of ssh. It hands the daemon command for a worker to Launch over a unix
//     socket, which passes it on to that worker.
//   - Work runs in every worker. It calls home, runs the command it receives, and calls home again.
//
// The launcher only accepts a connection claiming to be worker N if it comes from the address that the DNS name of
// worker N resolves to. Those names are published by Kubernetes for the pods of the gang, so a worker is
// identified by the platform rather than by a shared secret.
package mpibootstrap
