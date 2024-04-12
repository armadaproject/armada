package label

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/scheduling/corev1"
)

func MatchNodeSelectorTerms(nodeLabels map[string]string, nodeSelector *v1.NodeSelector) (bool, error) {
	return corev1.MatchNodeSelectorTerms(
		&v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Labels: nodeLabels,
			},
		},
		nodeSelector,
	)
}
