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

	"github.com/G-Research/armada/internal/lookoutv2/gen/models"
)

// GroupJobsHandlerFunc turns a function with the right signature into a group jobs handler
type GroupJobsHandlerFunc func(GroupJobsParams) middleware.Responder

// Handle executing the request and returning a response
func (fn GroupJobsHandlerFunc) Handle(params GroupJobsParams) middleware.Responder {
	return fn(params)
}

// GroupJobsHandler interface for that can handle valid group jobs params
type GroupJobsHandler interface {
	Handle(GroupJobsParams) middleware.Responder
}

// NewGroupJobs creates a new http.Handler for the group jobs operation
func NewGroupJobs(ctx *middleware.Context, handler GroupJobsHandler) *GroupJobs {
	return &GroupJobs{Context: ctx, Handler: handler}
}

/* GroupJobs swagger:route POST /api/v1/jobGroups groupJobs

GroupJobs group jobs API

*/
type GroupJobs struct {
	Context *middleware.Context
	Handler GroupJobsHandler
}

func (o *GroupJobs) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewGroupJobsParams()
	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request
	o.Context.Respond(rw, r, route.Produces, route, res)

}

// GroupJobsBody group jobs body
//
// swagger:model GroupJobsBody
type GroupJobsBody struct {

	// Additional fields to compute aggregates on
	// Required: true
	Aggregates []string `json:"aggregates"`

	// Filters to apply to jobs before grouping.
	// Required: true
	Filters []*models.Filter `json:"filters"`

	// Field to group jobs by
	// Required: true
	// Min Length: 1
	GroupedField string `json:"groupedField"`

	// Ordering to apply to job groups.
	// Required: true
	Order *models.Order `json:"order"`

	// First elements to ignore from the full set of results. Used for pagination.
	// Required: true
	Skip *int64 `json:"skip"`

	// Number of job groups to fetch.
	// Required: true
	Take int64 `json:"take"`
}

// Validate validates this group jobs body
func (o *GroupJobsBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateAggregates(formats); err != nil {
		res = append(res, err)
	}

	if err := o.validateFilters(formats); err != nil {
		res = append(res, err)
	}

	if err := o.validateGroupedField(formats); err != nil {
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

func (o *GroupJobsBody) validateAggregates(formats strfmt.Registry) error {

	if err := validate.Required("groupJobsRequest"+"."+"aggregates", "body", o.Aggregates); err != nil {
		return err
	}

	return nil
}

func (o *GroupJobsBody) validateFilters(formats strfmt.Registry) error {

	if err := validate.Required("groupJobsRequest"+"."+"filters", "body", o.Filters); err != nil {
		return err
	}

	for i := 0; i < len(o.Filters); i++ {
		if swag.IsZero(o.Filters[i]) { // not required
			continue
		}

		if o.Filters[i] != nil {
			if err := o.Filters[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("groupJobsRequest" + "." + "filters" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("groupJobsRequest" + "." + "filters" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (o *GroupJobsBody) validateGroupedField(formats strfmt.Registry) error {

	if err := validate.RequiredString("groupJobsRequest"+"."+"groupedField", "body", o.GroupedField); err != nil {
		return err
	}

	if err := validate.MinLength("groupJobsRequest"+"."+"groupedField", "body", o.GroupedField, 1); err != nil {
		return err
	}

	return nil
}

func (o *GroupJobsBody) validateOrder(formats strfmt.Registry) error {

	if err := validate.Required("groupJobsRequest"+"."+"order", "body", o.Order); err != nil {
		return err
	}

	if o.Order != nil {
		if err := o.Order.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("groupJobsRequest" + "." + "order")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("groupJobsRequest" + "." + "order")
			}
			return err
		}
	}

	return nil
}

func (o *GroupJobsBody) validateSkip(formats strfmt.Registry) error {

	if err := validate.Required("groupJobsRequest"+"."+"skip", "body", o.Skip); err != nil {
		return err
	}

	return nil
}

func (o *GroupJobsBody) validateTake(formats strfmt.Registry) error {

	if err := validate.Required("groupJobsRequest"+"."+"take", "body", int64(o.Take)); err != nil {
		return err
	}

	return nil
}

// ContextValidate validate this group jobs body based on the context it is used
func (o *GroupJobsBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
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

func (o *GroupJobsBody) contextValidateFilters(ctx context.Context, formats strfmt.Registry) error {

	for i := 0; i < len(o.Filters); i++ {

		if o.Filters[i] != nil {
			if err := o.Filters[i].ContextValidate(ctx, formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("groupJobsRequest" + "." + "filters" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("groupJobsRequest" + "." + "filters" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (o *GroupJobsBody) contextValidateOrder(ctx context.Context, formats strfmt.Registry) error {

	if o.Order != nil {
		if err := o.Order.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("groupJobsRequest" + "." + "order")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("groupJobsRequest" + "." + "order")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (o *GroupJobsBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *GroupJobsBody) UnmarshalBinary(b []byte) error {
	var res GroupJobsBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}

// GroupJobsOKBody group jobs o k body
//
// swagger:model GroupJobsOKBody
type GroupJobsOKBody struct {

	// Total number of groups
	Count int64 `json:"count,omitempty"`

	// List of Job groups
	// Required: true
	Groups []*models.Group `json:"groups"`
}

// Validate validates this group jobs o k body
func (o *GroupJobsOKBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateGroups(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *GroupJobsOKBody) validateGroups(formats strfmt.Registry) error {

	if err := validate.Required("groupJobsOK"+"."+"groups", "body", o.Groups); err != nil {
		return err
	}

	for i := 0; i < len(o.Groups); i++ {
		if swag.IsZero(o.Groups[i]) { // not required
			continue
		}

		if o.Groups[i] != nil {
			if err := o.Groups[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("groupJobsOK" + "." + "groups" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("groupJobsOK" + "." + "groups" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// ContextValidate validate this group jobs o k body based on the context it is used
func (o *GroupJobsOKBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := o.contextValidateGroups(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *GroupJobsOKBody) contextValidateGroups(ctx context.Context, formats strfmt.Registry) error {

	for i := 0; i < len(o.Groups); i++ {

		if o.Groups[i] != nil {
			if err := o.Groups[i].ContextValidate(ctx, formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("groupJobsOK" + "." + "groups" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("groupJobsOK" + "." + "groups" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (o *GroupJobsOKBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *GroupJobsOKBody) UnmarshalBinary(b []byte) error {
	var res GroupJobsOKBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
