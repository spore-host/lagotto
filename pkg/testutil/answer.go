package testutil

import (
	"context"
	"fmt"
	"reflect"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/smithy-go/middleware"
)

// AnswerFrom returns an SDK client APIOption that answers every operation from
// fake, a value with the client's method set (an in-package test fake of a
// narrow client interface, say): the operation named X is answered by calling
// fake.X(ctx, input), and the request never reaches the network, the
// credential chain, or a signer.
//
// It lets a test build REAL SDK clients, so middleware that inspects what the
// code under test sends (the tagtest untagged-create detector, lagotto#324)
// sees exactly the inputs production sends, while the fakes keep scripting the
// answers. It runs at the end of the Initialize step, after the SDK's own
// input validation and after any detector inserted there.
//
// An operation fake has no method for fails the call, naming it.
func AnswerFrom(fake any) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("lagotto/testutil.AnswerFrom",
			func(ctx context.Context, in middleware.InitializeInput, _ middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
				op := middleware.GetOperationName(ctx)
				if op == "" {
					op = awsmiddleware.GetOperationName(ctx)
				}
				// nosemgrep: go.lang.security.audit.unsafe-reflect-by-name.unsafe-reflect-by-name -- test helper; op is the SDK's own operation name, not input
				m := reflect.ValueOf(fake).MethodByName(op)
				if !m.IsValid() {
					return middleware.InitializeOutput{}, middleware.Metadata{}, fmt.Errorf("testutil.AnswerFrom: %T has no %s", fake, op)
				}
				res := m.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(in.Parameters)})
				var err error
				if e := res[1].Interface(); e != nil {
					err = e.(error)
				}
				return middleware.InitializeOutput{Result: res[0].Interface()}, middleware.Metadata{}, err
			}), middleware.After)
	}
}
