package model

import (
	"bytes"
	"errors"
)

// ErrInvalidWorkflowDefinition identifies Workflow YAML, job-name,
// event-filter and DAG definition failures. In-memory parsing uses this
// classification without wrapping arbitrary reader or filesystem errors.
var ErrInvalidWorkflowDefinition = errors.New("invalid workflow definition")

type workflowDefinitionError struct {
	err error
}

func (err *workflowDefinitionError) Error() string {
	return err.err.Error()
}

func (err *workflowDefinitionError) Unwrap() []error {
	return []error{ErrInvalidWorkflowDefinition, err.err}
}

func markWorkflowDefinitionError(err error) error {
	if err == nil || errors.Is(err, ErrInvalidWorkflowDefinition) {
		return err
	}
	return &workflowDefinitionError{err: err}
}

// ReadWorkflowContent parses an immutable in-memory Workflow. Its errors are
// definition failures by construction, unlike errors from an arbitrary Reader.
func ReadWorkflowContent(content []byte, strict bool) (*Workflow, error) {
	workflow, err := ReadWorkflow(bytes.NewReader(content), strict)
	if err != nil {
		return workflow, markWorkflowDefinitionError(err)
	}
	return workflow, nil
}
