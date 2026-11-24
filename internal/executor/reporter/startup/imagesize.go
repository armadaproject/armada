package startup

import (
	"github.com/gogo/protobuf/types"
	v1 "k8s.io/api/core/v1"
)

// nodeImageSizes maps every name the node reports for an image, both digest references and tags, to its size.
//
// Used to fill in image sizes the Pulled event doesn't provide, e.g. when the image was already present on the node.
type nodeImageSizes map[string]uint64

func newNodeImageSizes(node *v1.Node) nodeImageSizes {
	if node == nil {
		return nil
	}
	sizes := make(nodeImageSizes)
	for _, image := range node.Status.Images {
		if image.SizeBytes <= 0 {
			continue
		}
		for _, name := range image.Names {
			sizes[name] = uint64(image.SizeBytes)
		}
	}
	return sizes
}

// sizeOf returns the size of the container's image, or nil if the node doesn't report it.
func (s nodeImageSizes) sizeOf(status v1.ContainerStatus) *types.UInt64Value {
	for _, name := range []string{status.ImageID, status.Image} {
		if name == "" {
			continue
		}
		if size, ok := s[name]; ok {
			return &types.UInt64Value{Value: size}
		}
	}
	return nil
}
