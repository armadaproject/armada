// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"context"
	"net/http"
	"strconv"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"

	"github.com/armadaproject/armada/internal/lookoutv2/gen/models"
)

// GetJobsHandlerFunc turns a function with the right signature into a get jobs handler
type GetJobsHandlerFunc func(GetJobsParams) middleware.Responder

// Handle executing the request and returning a response
func (fn GetJobsHandlerFunc) Handle(params GetJobsParams) middleware.Responder {
	return fn(params)
}

// GetJobsHandler interface for that can handle valid get jobs params
type GetJobsHandler interface {
	Handle(GetJobsParams) middleware.Responder
}

// NewGetJobs creates a new http.Handler for the get jobs operation
func NewGetJobs(ctx *middleware.Context, handler GetJobsHandler) *GetJobs {
	return &GetJobs{Context: ctx, Handler: handler}
}

/*
	GetJobs swagger:route POST /api/v1/jobs getJobs

GetJobs get jobs API
*/
type GetJobs struct {
	Context *middleware.Context
	Handler GetJobsHandler
}

func (o *GetJobs) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewGetJobsParams()
	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request
	o.Context.Respond(rw, r, route.Produces, route, res)

}

// GetJobsBody get jobs body
//
// swagger:model GetJobsBody
type GetJobsBody struct {

	// Filters to apply to jobs.
	// Required: true
	Filters []*models.Filter `json:"filters"`

	// Ordering to apply to jobs.
	// Required: true
	Order *models.Order `json:"order"`

	// First elements to ignore from the full set of results. Used for pagination.
	// Required: true
	Skip *int64 `json:"skip"`

	// Number of jobs to fetch.
	// Required: true
	Take int64 `json:"take"`
}

// Validate validates this get jobs body
func (o *GetJobsBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateFilters(formats); err != nil {
		res = append(res, err)
	}

	if err := o.validateOrder(formats); err != nil {
		res = append(res, err)
	}

	if err := o.validateSkip(formats); err != nil {
		res = append(res, err)
	}

	if err := o.validateTake(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *GetJobsBody) validateFilters(formats strfmt.Registry) error {

	if err := validate.Required("getJobsRequest"+"."+"filters", "body", o.Filters); err != nil {
		return err
	}

	for i := 0; i < len(o.Filters); i++ {
		if swag.IsZero(o.Filters[i]) { // not required
			continue
		}

		if o.Filters[i] != nil {
			if err := o.Filters[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("getJobsRequest" + "." + "filters" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("getJobsRequest" + "." + "filters" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (o *GetJobsBody) validateOrder(formats strfmt.Registry) error {

	if err := validate.Required("getJobsRequest"+"."+"order", "body", o.Order); err != nil {
		return err
	}

	if o.Order != nil {
		if err := o.Order.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("getJobsRequest" + "." + "order")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("getJobsRequest" + "." + "order")
			}
			return err
		}
	}

	return nil
}

func (o *GetJobsBody) validateSkip(formats strfmt.Registry) error {

	if err := validate.Required("getJobsRequest"+"."+"skip", "body", o.Skip); err != nil {
		return err
	}

	return nil
}

func (o *GetJobsBody) validateTake(formats strfmt.Registry) error {

	if err := validate.Required("getJobsRequest"+"."+"take", "body", int64(o.Take)); err != nil {
		return err
	}

	return nil
}

// ContextValidate validate this get jobs body based on the context it is used
func (o *GetJobsBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := o.contextValidateFilters(ctx, formats); err != nil {
		res = append(res, err)
	}

	if err := o.contextValidateOrder(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *GetJobsBody) contextValidateFilters(ctx context.Context, formats strfmt.Registry) error {

	for i := 0; i < len(o.Filters); i++ {

		if o.Filters[i] != nil {
			if err := o.Filters[i].ContextValidate(ctx, formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("getJobsRequest" + "." + "filters" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("getJobsRequest" + "." + "filters" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (o *GetJobsBody) contextValidateOrder(ctx context.Context, formats strfmt.Registry) error {

	if o.Order != nil {
		if err := o.Order.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("getJobsRequest" + "." + "order")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("getJobsRequest" + "." + "order")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (o *GetJobsBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *GetJobsBody) UnmarshalBinary(b []byte) error {
	var res GetJobsBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}

// GetJobsOKBody get jobs o k body
//
// swagger:model GetJobsOKBody
type GetJobsOKBody struct {

	// Total number of jobs
	Count int64 `json:"count,omitempty"`

	// List of jobs found
	Jobs []*models.Job `json:"jobs"`
}

// Validate validates this get jobs o k body
func (o *GetJobsOKBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateJobs(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *GetJobsOKBody) validateJobs(formats strfmt.Registry) error {
	if swag.IsZero(o.Jobs) { // not required
		return nil
	}

	for i := 0; i < len(o.Jobs); i++ {
		if swag.IsZero(o.Jobs[i]) { // not required
			continue
		}

		if o.Jobs[i] != nil {
			if err := o.Jobs[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("getJobsOK" + "." + "jobs" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("getJobsOK" + "." + "jobs" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// ContextValidate validate this get jobs o k body based on the context it is used
func (o *GetJobsOKBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := o.contextValidateJobs(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *GetJobsOKBody) contextValidateJobs(ctx context.Context, formats strfmt.Registry) error {

	for i := 0; i < len(o.Jobs); i++ {

		if o.Jobs[i] != nil {
			if err := o.Jobs[i].ContextValidate(ctx, formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("getJobsOK" + "." + "jobs" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("getJobsOK" + "." + "jobs" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (o *GetJobsOKBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *GetJobsOKBody) UnmarshalBinary(b []byte) error {
	var res GetJobsOKBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
