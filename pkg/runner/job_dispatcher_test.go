package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nektos/act/pkg/container"
	"github.com/nektos/act/pkg/model"
	assert "github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestDispatchedJobExecutorResult(t *testing.T) {
	dispatchFailure := errors.New("dispatch failed")
	tests := []struct {
		name        string
		result      JobResult
		dispatchErr error
		wantResult  string
		wantOutputs map[string]string
		wantErr     error
		wantErrText string
	}{
		{name: "success", result: JobResult{Conclusion: JobConclusionSuccess, Outputs: map[string]string{"value": "remote"}}, wantResult: "success", wantOutputs: map[string]string{"value": "remote"}},
		{name: "failure", result: JobResult{Conclusion: JobConclusionFailure, Outputs: map[string]string{"value": "partial"}}, wantResult: "failure", wantOutputs: map[string]string{"value": "partial"}},
		{name: "canceled", result: JobResult{Conclusion: JobConclusionCanceled}, wantResult: "failure", wantOutputs: map[string]string{}, wantErr: context.Canceled},
		{name: "invalid conclusion", result: JobResult{Conclusion: "skipped"}, wantResult: "failure", wantErrText: "remote Job returned invalid conclusion"},
		{name: "dispatch error", dispatchErr: dispatchFailure, wantResult: "failure", wantErr: dispatchFailure},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workflow := &model.Workflow{Jobs: map[string]*model.Job{
				"build": {Outputs: map[string]string{"value": "${{ steps.produce.outputs.value }}"}, Steps: []*model.Step{{Run: "echo remote"}}},
			}}
			rc := &RunContext{Run: &model.Run{Workflow: workflow, JobID: "build"}, Config: &Config{}}
			calls := 0
			dispatcher := JobDispatcherFunc(func(_ context.Context, got *RunContext) (JobResult, error) {
				calls++
				assert.Same(t, rc, got)
				return test.result, test.dispatchErr
			})

			err := newDispatchedJobExecutor(dispatcher, rc, rc)(t.Context())

			assert.Equal(t, 1, calls)
			assert.Equal(t, test.wantResult, rc.Run.Job().Result)
			if test.wantOutputs != nil {
				assert.Equal(t, test.wantOutputs, rc.Run.Job().Outputs)
			}
			if test.wantErr != nil {
				assert.ErrorIs(t, err, test.wantErr)
			} else if test.wantErrText != "" {
				assert.ErrorContains(t, err, test.wantErrText)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDispatchedJobExecutorContinueOnError(t *testing.T) {
	for _, test := range []struct {
		name       string
		expression string
		wantResult string
		wantErr    bool
	}{
		{name: "literal", expression: "true", wantResult: "success"},
		{name: "expression", expression: "${{ true }}", wantResult: "success"},
		{name: "false", expression: "false", wantResult: "failure"},
		{name: "invalid", expression: "${{ missing.function() }}", wantResult: "failure", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			workflow := &model.Workflow{Jobs: map[string]*model.Job{
				"build": {RawContinueOnError: test.expression, Steps: []*model.Step{{Run: "echo remote"}}},
			}}
			rc := &RunContext{Run: &model.Run{Workflow: workflow, JobID: "build"}, Config: &Config{}}
			dispatcher := JobDispatcherFunc(func(context.Context, *RunContext) (JobResult, error) {
				return JobResult{Conclusion: JobConclusionFailure, Outputs: map[string]string{"value": "partial"}}, nil
			})
			err := newDispatchedJobExecutor(dispatcher, rc, rc)(t.Context())
			assert.Equal(t, test.wantResult, rc.Run.Job().Result)
			assert.Equal(t, map[string]string{"value": "partial"}, rc.Run.Job().Outputs)
			if test.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDispatchedJobTimeout(t *testing.T) {
	workflow := &model.Workflow{Jobs: map[string]*model.Job{
		"build": {TimeoutMinutes: "${{ 5 }}", Steps: []*model.Step{{Run: "echo remote"}}},
	}}
	rc := &RunContext{Run: &model.Run{Workflow: workflow, JobID: "build"}, Config: &Config{}}
	timeout, err := dispatchedJobTimeout(t.Context(), rc)
	assert.NoError(t, err)
	assert.Equal(t, 5*time.Minute, timeout)
	for _, raw := range []string{"0", "-1", "invalid", "${{ missing.function() }}"} {
		rc.Run.Job().TimeoutMinutes = raw
		_, err = dispatchedJobTimeout(t.Context(), rc)
		assert.Error(t, err, raw)
	}
}

func TestJobDispatcherRunsPlan(t *testing.T) {
	directory := t.TempDir()
	workflow := []byte(`name: remote
on: push
jobs:
  first:
    runs-on: ubuntu-latest
    outputs:
      value: ${{ steps.produce.outputs.value }}
    steps:
      - id: produce
        run: echo "local execution must not happen"
  second:
    needs: first
    runs-on: ubuntu-latest
    steps:
      - run: echo "${{ needs.first.outputs.value }}"
`)
	assert.NoError(t, os.WriteFile(filepath.Join(directory, "push.yml"), workflow, 0o600))
	planner, err := model.NewWorkflowPlanner(directory, true, false)
	assert.NoError(t, err)
	plan, err := planner.PlanEvent("push")
	assert.NoError(t, err)

	var calls []string
	dispatcher := JobDispatcherFunc(func(ctx context.Context, rc *RunContext) (JobResult, error) {
		calls = append(calls, rc.Run.JobID)
		if rc.Run.JobID == "second" {
			script := rc.NewExpressionEvaluator(ctx).Interpolate(ctx, rc.Run.Job().Steps[0].Run)
			assert.Equal(t, `echo "remote"`, script)
		}
		return JobResult{Conclusion: JobConclusionSuccess, Outputs: map[string]string{"value": "remote"}}, nil
	})
	runner, err := New(&Config{Workdir: directory, EventName: "push", JobDispatcher: dispatcher})
	assert.NoError(t, err)

	err = runner.NewPlanExecutor(plan)(t.Context())

	assert.NoError(t, err)
	assert.Equal(t, []string{"first", "second"}, calls)
}

func TestJobDispatcherPreservesDeploymentEnvironment(t *testing.T) {
	workflow, err := model.ReadWorkflow(strings.NewReader(`name: environment
on: push
jobs:
  deploy:
    environment: production
    runs-on: self-hosted
    steps:
      - run: echo deploy
`), false)
	assert.NoError(t, err)
	assert.Equal(t, "production", workflow.GetJob("deploy").RawEnvironment.Value)
}

func TestWorkflowCallConfigPreservesSecrets(t *testing.T) {
	workflow, err := model.ReadWorkflow(strings.NewReader(`on:
  workflow_call:
    secrets:
      DEPLOY_TOKEN:
        required: true
jobs: {}
`), false)
	assert.NoError(t, err)
	call := workflow.WorkflowCallConfig()
	if assert.NotNil(t, call) {
		assert.True(t, call.Secrets["DEPLOY_TOKEN"].Required)
	}
}

func TestJobInvocationPath(t *testing.T) {
	root := &RunContext{
		Run:    &model.Run{Workflow: &model.Workflow{File: "root.yml"}, JobID: "call"},
		Matrix: map[string]any{"os": "linux"},
	}
	called := &RunContext{
		Run:    &model.Run{Workflow: &model.Workflow{File: "called.yml"}, JobID: "build"},
		Matrix: map[string]any{"version": "1.0"},
		caller: &caller{runContext: root},
	}

	path := called.JobInvocationPath()

	assert.Equal(t, []JobInvocation{
		{Workflow: "root.yml", JobID: "call", Matrix: map[string]any{"os": "linux"}},
		{Workflow: "called.yml", JobID: "build", Matrix: map[string]any{"version": "1.0"}},
	}, path)
	path[0].Matrix["os"] = "changed"
	assert.Equal(t, "linux", root.Matrix["os"])
}

func TestJobDispatchContextEvaluatesEnvironment(t *testing.T) {
	var jobEnv yaml.Node
	assert.NoError(t, jobEnv.Encode(map[string]string{"JOB_VALUE": "job"}))
	workflow := &model.Workflow{
		File: "remote.yml",
		Env:  map[string]string{"WORKFLOW_VALUE": "workflow", "FROZEN": "${{ github.sha }}"},
		Jobs: map[string]*model.Job{
			"build": {Env: jobEnv, Steps: []*model.Step{{Run: "echo remote"}}},
		},
	}
	rc := &RunContext{
		Run:          &model.Run{Workflow: workflow, JobID: "build"},
		Config:       &Config{Env: map[string]string{"CONFIG_VALUE": "config", "WORKFLOW_VALUE": "config-override", "SHA_REF": "0123456789abcdef", "GITHUB_WORKSPACE": "/act/workspace"}, EventName: "push", Workdir: "/work"},
		JobContainer: &container.HostEnvironment{ActPath: "/act", Workdir: "/work", Path: "/container"},
	}

	dispatch := rc.JobDispatchContext(t.Context())

	assert.Equal(t, "workflow", dispatch.Env["WORKFLOW_VALUE"])
	assert.Equal(t, "job", dispatch.Env["JOB_VALUE"])
	assert.NotContains(t, dispatch.Env, "CONFIG_VALUE")
	assert.NotContains(t, dispatch.Env, "SHA_REF")
	assert.Equal(t, "0123456789abcdef", dispatch.Env["FROZEN"])
	assert.NotContains(t, dispatch.Env, "ACT")
	assert.Equal(t, "true", rc.GetEnv()["ACT"])
	assert.Equal(t, "config", rc.GetEnv()["CONFIG_VALUE"])
	assert.Empty(t, dispatch.GitHub.EventPath)
	assert.Empty(t, dispatch.GitHub.Workspace)
}
