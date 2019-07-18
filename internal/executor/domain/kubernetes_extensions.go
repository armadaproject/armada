package domain

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type Patch struct {
	MetaData metav1.ObjectMeta `json:"metadata"`
}
