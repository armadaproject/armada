package protoutil

import (
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var (
	msg              = &armadaevents.CancelJob{JobId: armadaevents.ProtoUuidFromUuid(uuid.New())}
	compressor       = compress.NewThreadSafeZlibCompressor(1024)
	decompressor     = compress.NewThreadSafeZlibDecompressor()
	marshalledMsg, _ = proto.Marshal(msg)
	compressedMsg, _ = compressor.Compress(marshalledMsg)
	invalidMsg       = []byte{0x3}
)

func TestUnmarshall_Valid(t *testing.T) {
	unmarshalled, err := Unmarshall(marshalledMsg, &armadaevents.CancelJob{})
	require.NoError(t, err)
	assert.Equal(t, msg, unmarshalled)
}

func TestUnmarshall_Invalid(t *testing.T) {
	_, err := Unmarshall(invalidMsg, &armadaevents.CancelJob{})
	require.Error(t, err)
}

func TestMustUnmarshall(t *testing.T) {
	unmarshalled := MustUnmarshall(marshalledMsg, &armadaevents.CancelJob{})
	assert.Equal(t, msg, unmarshalled)
}

func TestDecompressAndUnmarshall_Valid(t *testing.T) {
	unmarshalled, err := DecompressAndUnmarshall(compressedMsg, &armadaevents.CancelJob{}, decompressor)
	require.NoError(t, err)
	assert.Equal(t, msg, unmarshalled)
}

func TestDecompressAndUnmarshall_Invalid(t *testing.T) {
	_, err := DecompressAndUnmarshall(invalidMsg, &armadaevents.CancelJob{}, decompressor)
	require.Error(t, err)
}

func TestMustDecompressAndUnmarshall(t *testing.T) {
	unmarshalled := MustDecompressAndUnmarshall(compressedMsg, &armadaevents.CancelJob{}, decompressor)
	assert.Equal(t, msg, unmarshalled)
}

func TestMarshallAndCompress(t *testing.T) {
	bytes, err := MarshallAndCompress(msg, compressor)
	require.NoError(t, err)
	assert.Equal(t, compressedMsg, bytes)
}

func TestMustMarshallAndCompress(t *testing.T) {
	bytes := MustMarshallAndCompress(msg, compressor)
	assert.Equal(t, compressedMsg, bytes)
}

func TestHashMany(t *testing.T) {
	input1 := &schedulerobjects.JobSchedulingInfo{Lifetime: 1}
	input2 := &schedulerobjects.JobSchedulingInfo{Lifetime: 2}
	input3 := &schedulerobjects.JobSchedulingInfo{Lifetime: 3}
	hash1, err := HashMany([]*schedulerobjects.JobSchedulingInfo{input1, input2})
	require.NoError(t, err)
	require.Equal(t, 20, len(hash1))
	hash2, err := HashMany([]*schedulerobjects.JobSchedulingInfo{input1, input2})
	require.NoError(t, err)
	require.Equal(t, 20, len(hash2))
	hash3, err := HashMany([]*schedulerobjects.JobSchedulingInfo{input1, input3})
	require.NoError(t, err)
	require.Equal(t, 20, len(hash3))
	assert.Equal(t, hash1, hash2)
	assert.NotEqual(t, hash1, hash3)
}

func TestHash_NoCollisions(t *testing.T) {
	hashes := make(map[string]bool)
	for i := 0; i < 1000000; i++ {
		intialHash, err := Hash(&schedulerobjects.JobSchedulingInfo{Lifetime: uint32(i)})
		require.NoError(t, err)
		hashStr := string(intialHash)
		_, ok := hashes[hashStr]
		require.False(t, ok, "Hash collision detected for %d", i)
		hashes[hashStr] = true
	}
}

func TestHash_TestConsistent(t *testing.T) {
	schedulingInfo := &schedulerobjects.JobSchedulingInfo{
		Lifetime:          1,
		AtMostOnce:        true,
		Preemptible:       true,
		ConcurrencySafe:   true,
		PriorityClassName: "armada-default",
		SubmitTime:        time.Now(),
		Priority:          10,
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						NodeSelector: map[string]string{
							"property1": "value1",
							"property3": "value3",
						},
						Affinity: &v1.Affinity{
							NodeAffinity: &v1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
									NodeSelectorTerms: []v1.NodeSelectorTerm{
										{
											MatchExpressions: []v1.NodeSelectorRequirement{
												{
													Key:      "k1",
													Operator: "o1",
													Values:   []string{"v1", "v2"},
												},
											},
											MatchFields: []v1.NodeSelectorRequirement{
												{
													Key:      "k2",
													Operator: "o2",
													Values:   []string{"v10", "v20"},
												},
											},
										},
									},
								},
							},
							PodAffinity: &v1.PodAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
									{
										LabelSelector: &metav1.LabelSelector{
											MatchLabels: map[string]string{
												"label1": "labelval1",
												"label2": "labelval2",
												"label3": "labelval3",
											},
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "k1",
													Operator: "o1",
													Values:   []string{"v1", "v2", "v3"},
												},
											},
										},
										Namespaces:  []string{"n1, n2, n3"},
										TopologyKey: "topkey1",
										NamespaceSelector: &metav1.LabelSelector{
											MatchLabels: map[string]string{
												"label10": "labelval1",
												"label20": "labelval2",
												"label30": "labelval3",
											},
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "k10",
													Operator: "o10",
													Values:   []string{"v10", "v20", "v30"},
												},
											},
										},
									},
								},
							},
							PodAntiAffinity: nil,
						},
						Tolerations: []v1.Toleration{{
							Key:               "a",
							Operator:          "b",
							Value:             "b",
							Effect:            "d",
							TolerationSeconds: pointer.Int64(1),
						}},
						Annotations: map[string]string{
							"foo":  "bar",
							"fish": "chips",
							"salt": "pepper",
						},
						Priority:         1,
						PreemptionPolicy: "abc",
						ResourceRequirements: v1.ResourceRequirements{
							Limits: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("1"),
								"memory": resource.MustParse("2"),
								"gpu":    resource.MustParse("3"),
							},
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("2"),
								"memory": resource.MustParse("2"),
								"gpu":    resource.MustParse("2"),
							},
						},
					},
				},
			},
		},
	}

	intialHash, err := Hash(schedulingInfo)
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		hash, err := Hash(schedulingInfo)
		require.NoError(t, err)
		assert.Equal(t, intialHash, hash)
	}
}
