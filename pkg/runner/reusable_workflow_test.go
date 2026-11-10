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
			require.Len(t, invocation, 2)
			require.Equal(t, ".github/workflows/caller.yml", invocation[0].WorkflowFilePath)
			require.Equal(t, "caller/repo", invocation[0].WorkflowRepository)
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

type remoteWorkflowArchiveCache struct {
	sha      string
	archive  []byte
	fetchRef string
}

func (cache *remoteWorkflowArchiveCache) Fetch(_ context.Context, _, _, ref, _ string) (string, error) {
	cache.fetchRef = ref
	return cache.sha, nil
}

func (cache *remoteWorkflowArchiveCache) GetTarArchive(_ context.Context, _, sha, _ string) (io.ReadCloser, error) {
	if sha != cache.sha {
		return nil, fmt.Errorf("archive SHA %q does not match frozen SHA %q", sha, cache.sha)
	}
	return io.NopCloser(bytes.NewReader(cache.archive)), nil
}

func TestActionCacheRemoteReusableWorkflowDispatchesNestedLocalJob(t *testing.T) {
	sha := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for _, workflow := range []struct {
		path    string
		content string
	}{
		{path: ".github/workflows/remote.yml", content: `name: remote
on: workflow_call
jobs:
  local:
    uses: ./.github/workflows/local.yml
`},
		{path: ".github/workflows/local.yml", content: `name: local
on: workflow_call
jobs:
  leaf:
    runs-on: self-hosted
    steps:
      - run: echo leaf
`},
	} {
		require.NoError(t, writer.WriteHeader(&tar.Header{Name: workflow.path, Mode: 0o600, Size: int64(len(workflow.content)), Typeflag: tar.TypeReg}))
		_, err := writer.Write([]byte(workflow.content))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())

	cache := &remoteWorkflowArchiveCache{sha: sha, archive: archive.Bytes()}
	cacheDirectory := filepath.Join(t.TempDir(), "action-cache")
	var dispatched *RunContext
	rc := &RunContext{
		Name: "call",
		Config: &Config{
			Workdir: t.TempDir(), WorkflowPath: ".github/workflows/caller.yml", ActionCacheDir: cacheDirectory, ActionCache: cache,
			Env: map[string]string{
				"GITHUB_REPOSITORY": "caller/repo", "GITHUB_REF": "refs/heads/main",
				"SHA_REF": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "GITHUB_SERVER_URL": "https://github.com",
			},
			JobDispatcher: JobDispatcherFunc(func(_ context.Context, current *RunContext) (JobResult, error) {
				dispatched = current
				return JobResult{Conclusion: JobConclusionSuccess}, nil
			}),
		},
		Run: &model.Run{JobID: "call", Workflow: &model.Workflow{Name: "caller", Jobs: map[string]*model.Job{
			"call": {Uses: "octo/reusable/.github/workflows/remote.yml@v1"},
		}}},
	}
	remote := newRemoteReusableWorkflow(rc.Run.Job().Uses)
	require.NotNil(t, remote)

	require.NoError(t, newActionCacheReusableWorkflowExecutor(rc, "octo/reusable@v1", remote)(t.Context()))
	require.Equal(t, "v1", cache.fetchRef)
	require.NotNil(t, dispatched)
	require.Equal(t, filepath.Join(cacheDirectory, safeFilename("octo/reusable@v1@"+sha)), dispatched.Config.Workdir)
	invocation := dispatched.JobInvocationPath()
	require.Len(t, invocation, 3)
	require.Equal(t, ".github/workflows/caller.yml", invocation[0].WorkflowFilePath)
	require.Equal(t, ".github/workflows/remote.yml", invocation[1].WorkflowFilePath)
	require.Equal(t, "octo/reusable", invocation[1].WorkflowRepository)
	leaf := invocation[len(invocation)-1]
	require.Equal(t, "octo/reusable", leaf.WorkflowRepository)
	require.Equal(t, ".github/workflows/local.yml", leaf.WorkflowFilePath)
	require.Equal(t, sha, leaf.WorkflowSHA)
	require.Equal(t, "octo/reusable/.github/workflows/local.yml@v1", leaf.WorkflowRef)
	github := dispatched.JobDispatchContext(t.Context()).GitHub
	require.Equal(t, leaf.WorkflowRef, github.WorkflowRef)
	require.Equal(t, sha, github.WorkflowSHA)
	require.Equal(t, "octo/reusable", github.WorkflowRepository)

	marker := filepath.Join(dispatched.Config.Workdir, ".extracted")
	require.NoError(t, os.WriteFile(marker, []byte("immutable"), 0o600))
	require.NoError(t, newActionCacheReusableWorkflowExecutor(rc, "octo/reusable@v1", remote)(t.Context()))
	content, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, "immutable", string(content), "another invocation must not remove an extracted repository in use")
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
