package service

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	authorizationv1 "k8s.io/api/authorization/v1"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clientTesting "k8s.io/client-go/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/executor/domain"
)

func TestConvertLogs_ReturnsLogLineWithTime(t *testing.T) {
	line := "2022-02-08T11:32:21.183268868Z Hello world!"
	logLines, errs := ConvertLogs([]byte(line))

	assert.Len(t, logLines, 1)
	assert.Len(t, errs, 0)
	assert.Equal(t, "2022-02-08T11:32:21.183268868Z", logLines[0].Timestamp)
	assert.Equal(t, "Hello world!", logLines[0].Line)
}

func TestConvertLogs_ReturnsNoLogWithNoTimestamp(t *testing.T) {
	line := "Hello world!"
	logLines, errs := ConvertLogs([]byte(line))

	assert.Len(t, logLines, 0)
	assert.Len(t, errs, 1)
}

func TestConvertLogs_ReturnsNoLogWithNoSpace(t *testing.T) {
	// A space is always expected after the timestamp, even for empty logs
	line := "2022-02-08T11:32:21.183268868Z"
	logLines, errs := ConvertLogs([]byte(line))

	assert.Len(t, logLines, 0)
	assert.Len(t, errs, 1)
}

func TestConvertLogs_EmptyLog(t *testing.T) {
	line := "2022-02-08T11:32:21.183268868Z "
	logLines, errs := ConvertLogs([]byte(line))

	assert.Len(t, logLines, 1)
	assert.Len(t, errs, 0)
	assert.Equal(t, "2022-02-08T11:32:21.183268868Z", logLines[0].Timestamp)
	assert.Equal(t, "", logLines[0].Line)
}

func TestConvertLogs_MultipleLogLines(t *testing.T) {
	lines := []string{
		"2022-02-08T11:32:21.183268868Z these are",
		"2022-02-08T11:32:22.183268868Z some Logs",
		"hello world",
		"2022-02-08T11:32:24.183268868Z done",
	}
	rawLog := strings.Join(lines, "\n")

	expected := [][]string{
		{"2022-02-08T11:32:21.183268868Z", "these are"},
		{"2022-02-08T11:32:22.183268868Z", "some Logs"},
		{"2022-02-08T11:32:24.183268868Z", "done"},
	}

	logLines, errs := ConvertLogs([]byte(rawLog))

	assert.Len(t, logLines, 3)
	assert.Len(t, errs, 1)
	for i := 0; i < len(expected); i++ {
		assert.Equal(t, expected[i][0], logLines[i].Timestamp)
		assert.Equal(t, expected[i][1], logLines[i].Line)
	}
}

func TestConvertLogs_IgnoreEmptyLines(t *testing.T) {
	rawLog := "2022-02-08T11:32:21.183268868Z these are\n" +
		"2022-02-08T11:32:22.183268868Z some Logs\n" +
		"\n" +
		"2022-02-08T11:32:24.183268868Z done\n" +
		"\n"

	expected := [][]string{
		{"2022-02-08T11:32:21.183268868Z", "these are"},
		{"2022-02-08T11:32:22.183268868Z", "some Logs"},
		{"2022-02-08T11:32:24.183268868Z", "done"},
	}

	logLines, errs := ConvertLogs([]byte(rawLog))

	assert.Len(t, logLines, len(expected))
	assert.Len(t, errs, 0)
	for i := 0; i < len(expected); i++ {
		assert.Equal(t, expected[i][0], logLines[i].Timestamp)
		assert.Equal(t, expected[i][1], logLines[i].Line)
	}
}

func TestConvertLogs_LargerThanMaxBytesTruncatesLogs(t *testing.T) {
	someTime := "2022-02-08T11:32:21.183268868Z "
	line := someTime + strings.Repeat("x", 999-len(someTime)) + "\n"
	nLines := MaxLogBytes / len(line)

	log := strings.Repeat(line, nLines+53)
	log = log[:len(log)-1] // Exclude last newline

	logLines, errs := ConvertLogs([]byte(log))

	assert.Len(t, logLines, nLines, fmt.Sprintf("should be %d, is %d", nLines, len(logLines)))
	assert.Len(t, errs, 0)
}

func TestKubernetesLogService_FindPodName(t *testing.T) {
	tests := []struct {
		name     string
		pods     []*v1.Pod
		runId    string
		wantName string
		wantCode codes.Code
	}{
		{
			name:     "the pod of the run is found",
			pods:     []*v1.Pod{runPod("armada-run-1", "job-1", "run-1"), runPod("armada-run-2", "job-1", "run-2")},
			runId:    "run-1",
			wantName: "armada-run-1",
		},
		{
			name:     "a pod of another job with the same run ID is not a match",
			pods:     []*v1.Pod{runPod("other", "job-2", "run-1")},
			runId:    "run-1",
			wantCode: codes.NotFound,
		},
		{
			name:     "two pods with the labels of the run are an error",
			pods:     []*v1.Pod{runPod("armada-run-1", "job-1", "run-1"), runPod("copy", "job-1", "run-1")},
			runId:    "run-1",
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "a run ID that is not a valid label value is rejected",
			runId:    "run-1,other=x",
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			for _, p := range tc.pods {
				_, err := client.CoreV1().Pods(p.Namespace).Create(armadacontext.Background(), p, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			logService := NewKubernetesLogService(&FakeClientProvider{FakeClient: client})

			name, err := logService.findPodName(armadacontext.Background(), "default", "job-1", tc.runId)

			assert.Equal(t, tc.wantName, name)
			assert.Equal(t, tc.wantCode, status.Code(err))
		})
	}
}

func TestKubernetesLogService_GetLogs_RunId(t *testing.T) {
	tests := []struct {
		name            string
		reviewAllowed   bool
		reviewErr       error
		wantCode        codes.Code
		wantServiceList bool
	}{
		{
			name:            "a user who may read logs gets the pod looked up with the service account",
			reviewAllowed:   true,
			wantCode:        codes.OK,
			wantServiceList: true,
		},
		{
			name:     "a user who may not read logs is denied before the lookup",
			wantCode: codes.PermissionDenied,
		},
		{
			name:      "a user who may not check permissions is denied before the lookup",
			reviewErr: k8serrors.NewForbidden(authorizationv1.Resource("selfsubjectaccessreviews"), "", fmt.Errorf("denied")),
			wantCode:  codes.PermissionDenied,
		},
		{
			name:      "a failed permission check is an internal error",
			reviewErr: fmt.Errorf("unavailable"),
			wantCode:  codes.Internal,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTwoClientProvider(tc.reviewAllowed, tc.reviewErr)
			_, err := provider.service.CoreV1().Pods("default").Create(armadacontext.Background(), runPod("armada-run-1", "job-1", "run-1"), metav1.CreateOptions{})
			require.NoError(t, err)
			provider.service.ClearActions()
			logService := NewKubernetesLogService(provider)

			_, err = logService.GetLogs(armadacontext.Background(), runLogParams())

			assert.Equal(t, tc.wantCode, status.Code(err))
			assert.Equal(t, tc.wantServiceList, hasListPodsAction(provider.service))
			assert.False(t, hasListPodsAction(provider.user), "the user client never lists pods")
		})
	}
}

func runPod(name, jobId, runId string) *v1.Pod {
	return &v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: "default",
		Labels:    map[string]string{domain.JobId: jobId, domain.JobRunId: runId},
	}}
}

func runLogParams() *LogParams {
	return &LogParams{
		Principal:  auth.NewStaticPrincipal("user", "test", nil),
		Namespace:  "default",
		JobId:      "job-1",
		RunId:      "run-1",
		LogOptions: &v1.PodLogOptions{},
	}
}

// twoClientProvider gives binoculars a separate service account client and user client, so a test can
// check which client makes which call.
type twoClientProvider struct {
	service *fake.Clientset
	user    *fake.Clientset
}

func newTwoClientProvider(reviewAllowed bool, reviewErr error) *twoClientProvider {
	user := fake.NewSimpleClientset()
	user.PrependReactor("create", "selfsubjectaccessreviews", func(clientTesting.Action) (bool, runtime.Object, error) {
		return true, &authorizationv1.SelfSubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: reviewAllowed}}, reviewErr
	})
	return &twoClientProvider{service: fake.NewSimpleClientset(), user: user}
}

func (p *twoClientProvider) ClientForUser(string, []string) (kubernetes.Interface, error) {
	return p.user, nil
}

func (p *twoClientProvider) Client() kubernetes.Interface {
	return p.service
}

func (p *twoClientProvider) ClientConfig() *rest.Config {
	return nil
}

func hasListPodsAction(client *fake.Clientset) bool {
	for _, action := range client.Actions() {
		if action.GetVerb() == "list" && action.GetResource().Resource == "pods" {
			return true
		}
	}
	return false
}
