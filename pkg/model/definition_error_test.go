package model

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadWorkflowContentClassifiesDefinitionErrorWithoutChangingText(t *testing.T) {
	content := []byte("jobs: [")
	_, originalErr := ReadWorkflow(bytes.NewReader(content), false)
	_, err := ReadWorkflowContent(content, false)
	assert.EqualError(t, err, originalErr.Error())
	assert.ErrorIs(t, err, ErrInvalidWorkflowDefinition)
}

func TestSingleWorkflowPlannerFromContentClassifiesEmptyWorkflow(t *testing.T) {
	planner, err := NewSingleWorkflowPlannerFromContent("empty.yml", nil, false)
	assert.Nil(t, planner)
	assert.EqualError(t, err, "unable to read workflow 'empty.yml': file is empty: EOF")
	assert.ErrorIs(t, err, ErrInvalidWorkflowDefinition)
	assert.ErrorIs(t, err, io.EOF)
}

func TestSingleWorkflowPlannerFromContentClassifiesJobName(t *testing.T) {
	planner, err := NewSingleWorkflowPlannerFromContent("jobs.yml", []byte(`jobs:
  invalid.job:
    runs-on: self-hosted
    steps:
      - run: echo invalid
`), false)
	assert.Nil(t, planner)
	assert.ErrorIs(t, err, ErrInvalidWorkflowDefinition)
	assert.ErrorContains(t, err, "Job name 'invalid.job' is invalid")
}

func TestPlanEventClassifiesDependencyDefinitionError(t *testing.T) {
	planner, err := NewSingleWorkflowPlannerFromContent("cycle.yml", []byte(`on: push
jobs:
  first:
    needs: second
    runs-on: self-hosted
    steps:
      - run: echo first
  second:
    needs: first
    runs-on: self-hosted
    steps:
      - run: echo second
`), false)
	assert.NoError(t, err)

	plan, err := planner.PlanEvent("push")
	assert.NotNil(t, plan)
	assert.ErrorIs(t, err, ErrInvalidWorkflowDefinition)
	assert.ErrorContains(t, err, "unable to build dependency graph")
}

func TestNewSingleWorkflowPlannerRetainsReaderErrorClassification(t *testing.T) {
	readerErr := errors.New("reader failed")
	_, err := NewSingleWorkflowPlanner("reader.yml", &failingReader{err: readerErr})
	assert.ErrorIs(t, err, readerErr)
	assert.NotErrorIs(t, err, ErrInvalidWorkflowDefinition)
}

type failingReader struct {
	err error
}

func (reader *failingReader) Read([]byte) (int, error) {
	return 0, reader.err
}

func TestSingleWorkflowPlannerFromContentKeepsFullFileAndBaseName(t *testing.T) {
	name := filepath.Join(string(filepath.Separator), "tmp", "workflow-root", ".github", "workflows", "ci.yml")
	planner, err := NewSingleWorkflowPlannerFromContent(name, []byte(`on: push
jobs:
  build:
    runs-on: self-hosted
    steps:
      - run: echo build
`), false)
	assert.NoError(t, err)
	workflow := planner.(*workflowPlanner).workflows[0]
	assert.Equal(t, name, workflow.File)
	assert.Equal(t, "ci.yml", workflow.Name)
}

func TestReadWorkflowContentAcceptsValidWorkflow(t *testing.T) {
	workflow, err := ReadWorkflowContent([]byte(`on: push
jobs:
  build:
    runs-on: self-hosted
    steps:
      - run: echo build
`), false)
	assert.NoError(t, err)
	assert.Equal(t, "build", workflow.GetJob("build").Name)
}
