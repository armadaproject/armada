// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
)

// GetJobErrorOKCode is the HTTP code returned for type GetJobErrorOK
const GetJobErrorOKCode int = 200

/*
GetJobErrorOK Returns error for specific job (if present)

swagger:response getJobErrorOK
*/
type GetJobErrorOK struct {

	/*
	  In: Body
	*/
	Payload *GetJobErrorOKBody `json:"body,omitempty"`
}

// NewGetJobErrorOK creates GetJobErrorOK with default headers values
func NewGetJobErrorOK() *GetJobErrorOK {

	return &GetJobErrorOK{}
}

// WithPayload adds the payload to the get job error o k response
func (o *GetJobErrorOK) WithPayload(payload *GetJobErrorOKBody) *GetJobErrorOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get job error o k response
func (o *GetJobErrorOK) SetPayload(payload *GetJobErrorOKBody) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetJobErrorOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// GetJobErrorBadRequestCode is the HTTP code returned for type GetJobErrorBadRequest
const GetJobErrorBadRequestCode int = 400

/*
GetJobErrorBadRequest Error response

swagger:response getJobErrorBadRequest
*/
type GetJobErrorBadRequest struct {

	/*
	  In: Body
	*/
	Payload *models.Error `json:"body,omitempty"`
}

// NewGetJobErrorBadRequest creates GetJobErrorBadRequest with default headers values
func NewGetJobErrorBadRequest() *GetJobErrorBadRequest {

	return &GetJobErrorBadRequest{}
}

// WithPayload adds the payload to the get job error bad request response
func (o *GetJobErrorBadRequest) WithPayload(payload *models.Error) *GetJobErrorBadRequest {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get job error bad request response
func (o *GetJobErrorBadRequest) SetPayload(payload *models.Error) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetJobErrorBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

/*
GetJobErrorDefault Error response

swagger:response getJobErrorDefault
*/
type GetJobErrorDefault struct {
	_statusCode int

	/*
	  In: Body
	*/
	Payload *models.Error `json:"body,omitempty"`
}

// NewGetJobErrorDefault creates GetJobErrorDefault with default headers values
func NewGetJobErrorDefault(code int) *GetJobErrorDefault {
	if code <= 0 {
		code = 500
	}

	return &GetJobErrorDefault{
		_statusCode: code,
	}
}

// WithStatusCode adds the status to the get job error default response
func (o *GetJobErrorDefault) WithStatusCode(code int) *GetJobErrorDefault {
	o._statusCode = code
	return o
}

// SetStatusCode sets the status to the get job error default response
func (o *GetJobErrorDefault) SetStatusCode(code int) {
	o._statusCode = code
}

// WithPayload adds the payload to the get job error default response
func (o *GetJobErrorDefault) WithPayload(payload *models.Error) *GetJobErrorDefault {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get job error default response
func (o *GetJobErrorDefault) SetPayload(payload *models.Error) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetJobErrorDefault) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(o._statusCode)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
