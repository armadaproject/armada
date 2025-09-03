// This file is largely a copy of https://github.com/apache/pulsar-client-go/blob/230d11b82ba8b60c971013516c4922afea4a022d/pulsaradmin/pkg/admin/subscription.go#L1
//  With simplifications to only cover the functionality we need
//  It contains a bug fix in handleResp
// This is to work around a bug in the client which is tracked here:
//  https://github.com/apache/pulsar-client-go/pull/1419
// If pulsar-client-go fix the issue, we should move back to the standard pulsaradmin.Client

package pulsarclient

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/golang/protobuf/proto" //nolint:staticcheck
)

// Subscriptions is admin interface for subscriptions management
type Subscriptions interface {
	// PeekMessages peeks messages from a topic subscription
	PeekMessages(utils.TopicName, string, int) ([]*utils.Message, error)
}

type subscriptions struct {
	pulsarClient *PulsarClient
	basePath     string
	SubPath      string
}

func (c *PulsarClient) Subscriptions() Subscriptions {
	return &subscriptions{
		pulsarClient: c,
		basePath:     "",
		SubPath:      "subscription",
	}
}

func (s *subscriptions) PeekMessages(topic utils.TopicName, sName string, n int) ([]*utils.Message, error) {
	var msgs []*utils.Message

	count := 1
	for n > 0 {
		m, err := s.peekNthMessage(topic, sName, count)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m...)
		n -= len(m)
		count++
	}

	return msgs, nil
}

func (s *subscriptions) peekNthMessage(topic utils.TopicName, sName string, pos int) ([]*utils.Message, error) {
	endpoint := s.pulsarClient.endpoint(s.basePath, topic.GetRestPath(), "subscription", url.PathEscape(sName),
		"position", strconv.Itoa(pos))

	resp, err := s.pulsarClient.Client.MakeRequest(http.MethodGet, endpoint)
	if err != nil {
		return nil, err
	}
	defer safeRespClose(resp)

	return handleResp(topic, resp)
}

// safeRespClose is used to close a response body
func safeRespClose(resp *http.Response) {
	if resp != nil {
		// ignore error since it is closing a response body
		_ = resp.Body.Close()
	}
}

const (
	PublishTimeHeader = "X-Pulsar-Publish-Time"
	BatchHeader       = "X-Pulsar-Num-Batch-Message"

	// PropertyPrefix is part of the old protocol for message properties.
	PropertyPrefix = "X-Pulsar-Property-"

	// PropertyHeader is part of the new protocol introduced in SNIP-279
	// https://github.com/apache/pulsar/pull/20627
	// The value is a JSON string representing the properties.
	PropertyHeader = "X-Pulsar-Property"
)

func handleResp(topic utils.TopicName, resp *http.Response) ([]*utils.Message, error) {
	msgID := resp.Header.Get("X-Pulsar-Message-ID")
	ID, err := utils.ParseMessageIDWithPartitionIndex(msgID, topic.GetPartitionIndex())
	if err != nil {
		return nil, err
	}

	// read data
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	properties := make(map[string]string)

	isBatch := false
	for k := range resp.Header {
		switch {
		case k == PublishTimeHeader:
			h := resp.Header.Get(k)
			if h != "" {
				properties["publish-time"] = h
			}
		case k == PropertyHeader:
			propJSON := resp.Header.Get(k)
			if err := json.Unmarshal([]byte(propJSON), &properties); err != nil {
				return nil, err
			}
		case k == BatchHeader:
			h := resp.Header.Get(k)
			if h != "" {
				properties[BatchHeader] = h
			}
			isBatch = true

		case strings.Contains(k, PropertyPrefix):
			key := strings.TrimPrefix(k, PropertyPrefix)
			properties[key] = resp.Header.Get(k)
		}
	}

	if isBatch {
		return getIndividualMsgsFromBatch(topic, ID, payload, properties)
	} else {
		return []*utils.Message{utils.NewMessage(topic.String(), *ID, payload, properties)}, nil
	}
}

func getIndividualMsgsFromBatch(topic utils.TopicName, msgID *utils.MessageID, data []byte,
	properties map[string]string,
) ([]*utils.Message, error) {
	batchSize, err := strconv.Atoi(properties[BatchHeader])
	if err != nil {
		return nil, nil
	}

	msgs := make([]*utils.Message, 0, batchSize)

	// read all messages in batch
	buf32 := make([]byte, 4)
	rdBuf := bytes.NewReader(data)
	for i := 0; i < batchSize; i++ {
		msgID.BatchIndex = i
		// singleMetaSize
		if _, err := io.ReadFull(rdBuf, buf32); err != nil {
			return nil, err
		}
		singleMetaSize := binary.BigEndian.Uint32(buf32)

		// singleMeta
		singleMetaBuf := make([]byte, singleMetaSize)
		if _, err := io.ReadFull(rdBuf, singleMetaBuf); err != nil {
			return nil, err
		}

		singleMeta := new(utils.SingleMessageMetadata)
		if err := proto.Unmarshal(singleMetaBuf, singleMeta); err != nil {
			return nil, err
		}

		if len(singleMeta.Properties) > 0 {
			for _, v := range singleMeta.Properties {
				k := *v.Key
				property := *v.Value
				properties[k] = property
			}
		}

		// payload
		singlePayload := make([]byte, singleMeta.GetPayloadSize())
		if _, err := io.ReadFull(rdBuf, singlePayload); err != nil {
			return nil, err
		}

		msgs = append(msgs, &utils.Message{
			Topic:      topic.String(),
			MessageID:  *msgID,
			Payload:    singlePayload,
			Properties: properties,
		})
	}

	return msgs, nil
}
