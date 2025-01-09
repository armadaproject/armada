// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// GetSchedulingReportOKCode is the HTTP code returned for type GetSchedulingReportOK
const GetSchedulingReportOKCode int = 200

/*
GetSchedulingReportOK Success

swagger:response getSchedulingReportOK
*/
type GetSchedulingReportOK struct {

	/*
	  In: Body
	*/
	Payload string `json:"body,omitempty"`
}

// NewGetSchedulingReportOK creates GetSchedulingReportOK with default headers values
func NewGetSchedulingReportOK() *GetSchedulingReportOK {

	return &GetSchedulingReportOK{}
}

// WithPayload adds the payload to the get scheduling report o k response
func (o *GetSchedulingReportOK) WithPayload(payload string) *GetSchedulingReportOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get scheduling report o k response
func (o *GetSchedulingReportOK) SetPayload(payload string) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetSchedulingReportOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	payload := o.Payload
	if err := producer.Produce(rw, payload); err != nil {
		panic(err) // let the recovery middleware deal with this
	}
}

/*
GetSchedulingReportDefault Error response

swagger:response getSchedulingReportDefault
*/
type GetSchedulingReportDefault struct {
	_statusCode int

	/*
	  In: Body
	*/
	Payload string `json:"body,omitempty"`
}

// NewGetSchedulingReportDefault creates GetSchedulingReportDefault with default headers values
func NewGetSchedulingReportDefault(code int) *GetSchedulingReportDefault {
	if code <= 0 {
		code = 500
	}

	return &GetSchedulingReportDefault{
		_statusCode: code,
	}
}

// WithStatusCode adds the status to the get scheduling report default response
func (o *GetSchedulingReportDefault) WithStatusCode(code int) *GetSchedulingReportDefault {
	o._statusCode = code
	return o
}

// SetStatusCode sets the status to the get scheduling report default response
func (o *GetSchedulingReportDefault) SetStatusCode(code int) {
	o._statusCode = code
}

// WithPayload adds the payload to the get scheduling report default response
func (o *GetSchedulingReportDefault) WithPayload(payload string) *GetSchedulingReportDefault {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get scheduling report default response
func (o *GetSchedulingReportDefault) SetPayload(payload string) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetSchedulingReportDefault) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(o._statusCode)
	payload := o.Payload
	if err := producer.Produce(rw, payload); err != nil {
		panic(err) // let the recovery middleware deal with this
	}
}
