package runner

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/nektos/act/pkg/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReusableWorkflowPathsStayInRepository(t *testing.T) {
	base := t.TempDir()
	repo := filepath.Join(base, "a", "repo")
	workflows := filepath.Join(repo, ".github", "workflows")
	require.NoError(t, os.MkdirAll(workflows, 0o755))
	body := []byte(`name: called
on:
  workflow_call:
jobs:
  called:
    if: "false"
    runs-on: ubuntu-latest
    steps:
      - run: "true"
`)
	require.NoError(t, os.WriteFile(filepath.Join(workflows, "called.yml"), body, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(base, "outside.yml"), body, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(base, "a", "outside.yml"), body, 0o600))

	rc := &RunContext{
		Name: "call",
		Config: &Config{
			Workdir: repo,
			Env:     map[string]string{},
		},
		Run: &model.Run{
			JobID: "call",
			Workflow: &model.Workflow{
				Name: "caller",
				Jobs: map[string]*model.Job{
					"call": {},
				},
			},
		},
	}

	tests := []struct {
		name    string
		uses    string
		remote  bool
		wantErr string
	}{
		{
			name:    "local uses escapes",
			uses:    "./../../outside.yml",
			wantErr: "resolves outside",
		},
		{
			name:    "remote filename escapes",
			uses:    "org/repo/.github/workflows/../../../outside.yml@ref",
			remote:  true,
			wantErr: "resolves outside",
		},
		{
			name: "local workflow",
			uses: "./.github/workflows/called.yml",
		},
		{
			name:   "remote workflow",
			uses:   "org/repo/.github/workflows/called.yml@ref",
			remote: true,
		},
		{
			name: "local parent stays inside",
			uses: "./.github/workflows/../workflows/called.yml",
		},
		{
			name:   "remote parent stays inside",
			uses:   "org/repo/.github/workflows/../workflows/called.yml@ref",
			remote: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workflow := tt.uses
			if tt.remote {
				remote := newRemoteReusableWorkflow(tt.uses)
				require.NotNil(t, remote)
				workflow = fmt.Sprintf("./.github/workflows/%s", remote.Filename)
			}
			err := newReusableWorkflowExecutor(rc, repo, workflow)(context.Background())
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestRemoteReusableWorkflowCanBeDisabled(t *testing.T) {
	rc := &RunContext{
		Config: &Config{DisableRemoteReusableWorkflows: true},
		Run: &model.Run{JobID: "call", Workflow: &model.Workflow{Jobs: map[string]*model.Job{
			"call": {Uses: "org/repo/.github/workflows/called.yml@main"},
		}}},
	}

	err := newRemoteReusableWorkflowExecutor(rc)(t.Context())

	require.ErrorIs(t, err, ErrRemoteReusableWorkflowDisabled)
}

func TestReusableWorkflowDispatchUsesContainingWorkflowSource(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		source     *model.WorkflowSource
		repository string
		ref        string
		sha        string
	}{
		{name: "local", repository: "caller/repo", ref: "refs/heads/main", sha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{name: "remote", source: &model.WorkflowSource{Repository: "called/repo", Ref: "v1", SHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
			repository: "called/repo", ref: "v1", sha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			callerDirectory := t.TempDir()
			calledDirectory := t.TempDir()
			workflows := filepath.Join(calledDirectory, ".github", "workflows")
			require.NoError(t, os.MkdirAll(workflows, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(workflows, "called.yml"), []byte(`name: called
on: workflow_call
jobs:
  called:
    runs-on: self-hosted
    steps:
      - run: echo called
`), 0o600))

			var dispatched *RunContext
			rc := &RunContext{
				Name: "call",
				Config: &Config{
					Workdir: callerDirectory, WorkflowPath: ".github/workflows/caller.yml",
					Env: map[string]string{"GITHUB_REPOSITORY": "caller/repo", "GITHUB_REF": "refs/heads/main", "SHA_REF": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
					JobDispatcher: JobDispatcherFunc(func(_ context.Context, current *RunContext) (JobResult, error) {
						dispatched = current
						return JobResult{Conclusion: JobConclusionSuccess}, nil
					}),
				},
				Run: &model.Run{JobID: "call", Workflow: &model.Workflow{Name: "caller", Jobs: map[string]*model.Job{"call": {}}}},
			}
			require.NoError(t, newReusableWorkflowExecutorWithSource(rc, calledDirectory, "./.github/workflows/called.yml", scenario.source)(t.Context()))
			require.NotNil(t, dispatched)
			require.Equal(t, calledDirectory, dispatched.Config.Workdir)
			invocation := dispatched.JobInvocationPath()
			require.NotEmpty(t, invocation)
			leaf := invocation[len(invocation)-1]
			require.Equal(t, scenario.repository, leaf.WorkflowRepository)
			require.Equal(t, ".github/workflows/called.yml", leaf.WorkflowFilePath)
			require.Equal(t, scenario.sha, leaf.WorkflowSHA)
			require.Equal(t, scenario.repository+"/.github/workflows/called.yml@"+scenario.ref, leaf.WorkflowRef)
			github := dispatched.JobDispatchContext(t.Context()).GitHub
			require.Equal(t, leaf.WorkflowRef, github.WorkflowRef)
			require.Equal(t, scenario.sha, github.WorkflowSHA)
			require.Equal(t, scenario.repository, github.WorkflowRepository)
		})
	}
}

func TestExtractRemoteWorkflowArchive(t *testing.T) {
	var content bytes.Buffer
	writer := tar.NewWriter(&content)
	require.NoError(t, writer.WriteHeader(&tar.Header{Name: ".github/workflows/called.yml", Mode: 0o600, Size: 4, Typeflag: tar.TypeReg}))
	_, err := writer.Write([]byte("test"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	directory := filepath.Join(t.TempDir(), "remote")
	require.NoError(t, extractRemoteWorkflowArchive(t.Context(), io.NopCloser(bytes.NewReader(content.Bytes())), directory))
	extracted, err := os.ReadFile(filepath.Join(directory, ".github", "workflows", "called.yml"))
	require.NoError(t, err)
	require.Equal(t, "test", string(extracted))

	content.Reset()
	writer = tar.NewWriter(&content)
	require.NoError(t, writer.WriteHeader(&tar.Header{Name: "../outside", Mode: 0o600, Size: 1, Typeflag: tar.TypeReg}))
	_, err = writer.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	unsafeDirectory := filepath.Join(t.TempDir(), "remote")
	err = extractRemoteWorkflowArchive(t.Context(), io.NopCloser(bytes.NewReader(content.Bytes())), unsafeDirectory)
	require.ErrorContains(t, err, "unsafe path")
	_, err = os.Stat(unsafeDirectory)
	require.ErrorIs(t, err, os.ErrNotExist)
}
