package logic

import (
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func PrintEvents(url, topic, subscription string, verbose bool) error {
	fmt.Println("Subscribing to Pulsar events")
	fmt.Println("URL:", url)
	fmt.Println("Topic:", topic)
	fmt.Println("Subscription", subscription)
	return withSetup(url, topic, subscription, func(ctx *armadacontext.Context, producer pulsar.Producer, consumer pulsar.Consumer) error {
		// Number of active jobs.
		numJobs := 0

		// Time at which numJobs most recently changed from 0 to 1 and from 1 to 0, respectively.
		risingEdge := time.Now()

		for {
			msg, err := consumer.Receive(ctx)
			if err != nil {
				fmt.Println(err)
				time.Sleep(time.Second)
				continue
			}

			util.RetryUntilSuccess(
				ctx,
				func() error { return consumer.Ack(msg) },
				func(err error) {
					fmt.Println(err)
					time.Sleep(time.Second)
				},
			)

			sequence := &armadaevents.EventSequence{}
			err = proto.Unmarshal(msg.Payload(), sequence)
			if err != nil {
				fmt.Println(err)
				continue
			}

			// Skip sequences with no events.
			if len(sequence.Events) == 0 {
				continue
			}

			// Count number of active jobs
			for _, event := range sequence.Events {
				if isSubmitJob(event) {
					if numJobs == 0 {
						risingEdge = time.Now()
					}
					numJobs++
				} else if isJobFailed(event) {
					numJobs--
				} else if isJobSucceeded(event) {
					numJobs--
				}
			}

			if !verbose {
				fmt.Printf("> EventSequence w. %d events (%d jobs active, for %s)\n",
					len(sequence.Events), numJobs, time.Since(risingEdge))
				fmt.Printf("  (Queue: %s, JobSetName: %s, UserId: %s, Groups: %v)\n",
					sequence.GetQueue(), sequence.GetJobSetName(), sequence.GetUserId(), sequence.GetGroups())
				for _, event := range sequence.Events {
					// On error, we print an empty id.
					jobId, _ := armadaevents.JobIdFromEvent(event)
					fmt.Printf("\t%T (job %s)\n", event.Event, armadaevents.UlidFromProtoUuid(jobId))

					if submitJob, ok := event.Event.(*armadaevents.EventSequence_Event_SubmitJob); ok {
						mainObject := submitJob.SubmitJob.GetMainObject()
						fmt.Printf("\t\tMainObject: %T\n", mainObject.Object)
						fmt.Printf("\t\t\tObjectMeta: %v\n", mainObject.GetObjectMeta())
						if mainObject, ok := (submitJob.SubmitJob.GetMainObject().Object).(*armadaevents.KubernetesMainObject_PodSpec); ok {
							fmt.Printf("\t\t\tTolerations: %v\n", mainObject.PodSpec.GetPodSpec().Tolerations)
						}
						for i, object := range submitJob.SubmitJob.GetObjects() {
							fmt.Printf("\t\tObject %d: %T\n", i, object.GetObject())
							fmt.Printf("\t\t\tObjectMeta: %v\n", object.GetObjectMeta())
						}
					} else if duplicateDetected, ok := event.Event.(*armadaevents.EventSequence_Event_JobDuplicateDetected); ok {
						newId, err := armadaevents.UlidStringFromProtoUuid(duplicateDetected.JobDuplicateDetected.NewJobId)
						if err != nil {
							panic(err)
						}
						oldId, err := armadaevents.UlidStringFromProtoUuid(duplicateDetected.JobDuplicateDetected.OldJobId)
						if err != nil {
							panic(err)
						}
						fmt.Printf("\t\tNew job %s is a duplicate of existing job %s\n", newId, oldId)
					} else if jobRunErrors, ok := event.Event.(*armadaevents.EventSequence_Event_JobRunErrors); ok {
						for _, e := range jobRunErrors.JobRunErrors.Errors {
							fmt.Printf("\t\t%T\n", e.Reason)
						}
					} else if jobErrors, ok := event.Event.(*armadaevents.EventSequence_Event_JobErrors); ok {
						for _, e := range jobErrors.JobErrors.Errors {
							fmt.Printf("\t\t%T\n", e.Reason)
						}
					}
				}
			} else {
				// Remove fields from PodSpecs that result in panics when printing.
				for _, event := range sequence.Events {
					stripPodSpecsInEvent(event)
				}
				// TODO: This results in panics when there are tolerations in the podspec.
				fmt.Printf("> EventSequence w. %d events (%d jobs active, for %s)\n%s\n",
					len(sequence.Events), numJobs, time.Since(risingEdge),
					proto.MarshalTextString(sequence))
			}
		}
		return nil
	})
}

func isSubmitJob(e *armadaevents.EventSequence_Event) bool {
	_, ok := (e.Event).(*armadaevents.EventSequence_Event_SubmitJob)
	return ok
}

func isJobFailed(e *armadaevents.EventSequence_Event) bool {
	if m, ok := (e.Event).(*armadaevents.EventSequence_Event_JobRunErrors); ok {
		for _, err := range m.JobRunErrors.Errors {
			if err.Terminal {
				return true
			}
		}
	}
	if m, ok := (e.Event).(*armadaevents.EventSequence_Event_JobErrors); ok {
		for _, err := range m.JobErrors.Errors {
			if err.Terminal {
				return true
			}
		}
	}
	if _, ok := (e.Event).(*armadaevents.EventSequence_Event_CancelledJob); ok {
		return true
	}
	return false
}

func isJobSucceeded(e *armadaevents.EventSequence_Event) bool {
	_, ok := (e.Event).(*armadaevents.EventSequence_Event_JobSucceeded)
	return ok
}

func stripPodSpecsInEvent(event *armadaevents.EventSequence_Event) {
	submitJob, ok := (event.Event).(*armadaevents.EventSequence_Event_SubmitJob)
	if ok {
		if podSpec, ok := (submitJob.SubmitJob.MainObject.Object).(*armadaevents.KubernetesMainObject_PodSpec); ok {
			podSpec.PodSpec.PodSpec = stripPodSpec(podSpec.PodSpec.PodSpec)
		}
	}
}

// stripPodSpec returns a new PodSpec with the Resources field zeroed out,
// which we remove because it contains private values, which cause a panic when printed.
func stripPodSpec(spec *v1.PodSpec) *v1.PodSpec {
	containers := make([]v1.Container, len(spec.Containers), len(spec.Containers))
	for i, container := range spec.Containers {
		containers[i] = v1.Container{
			Name:                     container.Name,
			Image:                    container.Image,
			Command:                  container.Command,
			Args:                     container.Args,
			WorkingDir:               container.WorkingDir,
			Ports:                    container.Ports,
			EnvFrom:                  container.EnvFrom,
			Env:                      container.Env,
			Resources:                v1.ResourceRequirements{}, // This is the problem
			VolumeMounts:             container.VolumeMounts,
			VolumeDevices:            container.VolumeDevices,
			LivenessProbe:            container.LivenessProbe,
			ReadinessProbe:           container.ReadinessProbe,
			StartupProbe:             container.StartupProbe,
			Lifecycle:                container.Lifecycle,
			TerminationMessagePath:   container.TerminationMessagePath,
			TerminationMessagePolicy: container.TerminationMessagePolicy,
			ImagePullPolicy:          container.ImagePullPolicy,
			SecurityContext:          container.SecurityContext,
			Stdin:                    container.Stdin,
			StdinOnce:                container.StdinOnce,
			TTY:                      container.TTY,
		}
	}
	spec.Containers = containers
	return spec
}

// Run action with an Armada submit client and a Pulsar producer and consumer.
func withSetup(url, topic, subscription string, action func(ctx *armadacontext.Context, producer pulsar.Producer, consumer pulsar.Consumer) error) error {
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: url,
	})
	if err != nil {
		return err
	}
	defer pulsarClient.Close()

	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		return err
	}
	defer producer.Close()

	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscription,
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	return action(armadacontext.Background(), producer, consumer)
}
