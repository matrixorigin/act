package runner

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	gogit "github.com/go-git/go-git/v5"
	"github.com/nektos/act/pkg/common"
	"github.com/nektos/act/pkg/common/git"
	"github.com/nektos/act/pkg/model"
)

func newLocalReusableWorkflowExecutor(rc *RunContext) common.Executor {
	return newReusableWorkflowExecutor(rc, rc.Config.Workdir, rc.Run.Job().Uses)
}

var ErrRemoteReusableWorkflowDisabled = errors.New("remote reusable workflows are disabled by the runner configuration")

func newRemoteReusableWorkflowExecutor(rc *RunContext) common.Executor {
	if rc.Config.DisableRemoteReusableWorkflows {
		return common.NewErrorExecutor(ErrRemoteReusableWorkflowDisabled)
	}
	uses := rc.Run.Job().Uses

	remoteReusableWorkflow := newRemoteReusableWorkflow(uses)
	if remoteReusableWorkflow == nil {
		return common.NewErrorExecutor(fmt.Errorf("expected format {owner}/{repo}/.github/workflows/{filename}@{ref}. Actual '%s' Input string was not in a correct format", uses))
	}

	// uses with safe filename makes the target directory look something like this {owner}-{repo}-.github-workflows-{filename}@{ref}
	// instead we will just use {owner}-{repo}@{ref} as our target directory. This should also improve performance when we are using
	// multiple reusable workflows from the same repository and ref since for each workflow we won't have to clone it again
	filename := fmt.Sprintf("%s/%s@%s", remoteReusableWorkflow.Org, remoteReusableWorkflow.Repo, remoteReusableWorkflow.Ref)
	workflowDir := fmt.Sprintf("%s/%s", rc.ActionCacheDir(), safeFilename(filename))

	if rc.Config.ActionCache != nil {
		return newActionCacheReusableWorkflowExecutor(rc, filename, remoteReusableWorkflow)
	}

	source := &model.WorkflowSource{Repository: remoteReusableWorkflow.Org + "/" + remoteReusableWorkflow.Repo,
		Path: fmt.Sprintf(".github/workflows/%s", remoteReusableWorkflow.Filename), Ref: remoteReusableWorkflow.Ref}
	return common.NewPipelineExecutor(
		newMutexExecutor(cloneIfRequired(rc, *remoteReusableWorkflow, workflowDir)),
		newReusableWorkflowExecutorWithSource(rc, workflowDir, fmt.Sprintf("./.github/workflows/%s", remoteReusableWorkflow.Filename), source),
	)
}

func newActionCacheReusableWorkflowExecutor(rc *RunContext, filename string, remoteReusableWorkflow *remoteReusableWorkflow) common.Executor {
	return func(ctx context.Context) error {
		ghctx := rc.getGithubContext(ctx)
		remoteReusableWorkflow.URL = ghctx.ServerURL
		executorLock.Lock()
		sha, err := rc.Config.ActionCache.Fetch(ctx, filename, remoteReusableWorkflow.CloneURL(), remoteReusableWorkflow.Ref, ghctx.Token)
		if err != nil {
			executorLock.Unlock()
			return err
		}
		workflowDir := fmt.Sprintf("%s/%s", rc.ActionCacheDir(), safeFilename(filename+"@"+sha))
		if _, err := os.Stat(workflowDir); errors.Is(err, fs.ErrNotExist) {
			archive, err := rc.Config.ActionCache.GetTarArchive(ctx, filename, sha, ".")
			if err != nil {
				executorLock.Unlock()
				return err
			}
			if err := extractRemoteWorkflowArchive(ctx, archive, workflowDir); err != nil {
				executorLock.Unlock()
				return err
			}
		} else if err != nil {
			executorLock.Unlock()
			return err
		}
		executorLock.Unlock()
		source := &model.WorkflowSource{Repository: remoteReusableWorkflow.Org + "/" + remoteReusableWorkflow.Repo,
			Path: fmt.Sprintf(".github/workflows/%s", remoteReusableWorkflow.Filename), Ref: remoteReusableWorkflow.Ref, SHA: sha}
		return newReusableWorkflowExecutorWithSource(rc, workflowDir, fmt.Sprintf("./.github/workflows/%s", remoteReusableWorkflow.Filename), source)(ctx)
	}
}

const remoteWorkflowArchiveLimit = 128 << 20

func extractRemoteWorkflowArchive(ctx context.Context, archive io.ReadCloser, directory string) (retErr error) {
	defer archive.Close()
	if err := os.RemoveAll(directory); err != nil {
		return err
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, os.RemoveAll(directory))
		}
	}()
	reader := tar.NewReader(archive)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		name := path.Clean(header.Name)
		if name == "." {
			continue
		}
		if path.IsAbs(name) || strings.HasPrefix(name, "../") || strings.Contains(name, "\\") {
			return fmt.Errorf("remote workflow archive contains unsafe path %q", header.Name)
		}
		target, err := joinUnder(directory, name)
		if err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if header.Size < 0 || total > remoteWorkflowArchiveLimit-header.Size {
				return fmt.Errorf("remote workflow archive exceeds %d bytes", remoteWorkflowArchiveLimit)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fs.FileMode(header.Mode)&0o777)
			if err != nil {
				return err
			}
			_, copyErr := io.CopyN(file, reader, header.Size)
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			total += header.Size
		default:
			return fmt.Errorf("remote workflow archive contains unsupported entry %q", header.Name)
		}
	}
}

var (
	executorLock sync.Mutex
)

func newMutexExecutor(executor common.Executor) common.Executor {
	return func(ctx context.Context) error {
		executorLock.Lock()
		defer executorLock.Unlock()

		return executor(ctx)
	}
}

func cloneIfRequired(rc *RunContext, remoteReusableWorkflow remoteReusableWorkflow, targetDirectory string) common.Executor {
	return common.NewConditionalExecutor(
		func(_ context.Context) bool {
			_, err := os.Stat(targetDirectory)
			notExists := errors.Is(err, fs.ErrNotExist)
			return notExists
		},
		func(ctx context.Context) error {
			remoteReusableWorkflow.URL = rc.getGithubContext(ctx).ServerURL
			return git.NewGitCloneExecutor(git.NewGitCloneExecutorInput{
				URL:         remoteReusableWorkflow.CloneURL(),
				Ref:         remoteReusableWorkflow.Ref,
				Dir:         targetDirectory,
				Token:       rc.Config.Token,
				OfflineMode: rc.Config.ActionOfflineMode,
			})(ctx)
		},
		nil,
	)
}

func newReusableWorkflowExecutor(rc *RunContext, directory string, workflow string) common.Executor {
	return newReusableWorkflowExecutorWithSource(rc, directory, workflow, nil)
}

func newReusableWorkflowExecutorWithSource(rc *RunContext, directory string, workflow string, source *model.WorkflowSource) common.Executor {
	return func(ctx context.Context) error {
		workflowPath, err := joinUnder(directory, strings.TrimPrefix(workflow, "./"))
		if err != nil {
			return err
		}
		content, err := os.ReadFile(workflowPath)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(directory, workflowPath)
		if err != nil {
			return err
		}
		resolveSHA := source != nil && source.SHA == ""
		if source == nil {
			inherited := rc.workflowSource()
			source = &inherited
		}
		source.Path = filepath.ToSlash(relative)
		if resolveSHA {
			repository, err := gogit.PlainOpen(directory)
			if err != nil {
				return err
			}
			head, err := repository.Head()
			if err != nil {
				return err
			}
			source.SHA = head.Hash().String()
		}
		planner, err := model.NewSingleWorkflowPlannerFromContent(workflowPath, content, false)
		if err != nil {
			return err
		}
		if err := model.SetWorkflowSource(planner, source); err != nil {
			return err
		}

		plan, err := planner.PlanEvent("workflow_call")
		if err != nil {
			return err
		}

		childConfig := *rc.Config
		childConfig.Workdir = directory
		childConfig.WorkflowPath = source.Path
		childContext := *rc
		childContext.Config = &childConfig
		runner, err := NewReusableWorkflowRunner(&childContext)
		if err != nil {
			return err
		}

		return runner.NewPlanExecutor(plan)(ctx)
	}
}

func NewReusableWorkflowRunner(rc *RunContext) (Runner, error) {
	runner := &runnerImpl{
		config:    rc.Config,
		eventJSON: rc.EventJSON,
		caller: &caller{
			runContext: rc,
		},
	}

	return runner.configure()
}

type remoteReusableWorkflow struct {
	URL      string
	Org      string
	Repo     string
	Filename string
	Ref      string
}

func (r *remoteReusableWorkflow) CloneURL() string {
	return fmt.Sprintf("%s/%s/%s", r.URL, r.Org, r.Repo)
}

func newRemoteReusableWorkflow(uses string) *remoteReusableWorkflow {
	// GitHub docs:
	// https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions#jobsjob_iduses
	r := regexp.MustCompile(`^([^/]+)/([^/]+)/.github/workflows/([^@]+)@(.*)$`)
	matches := r.FindStringSubmatch(uses)
	if len(matches) != 5 {
		return nil
	}
	return &remoteReusableWorkflow{
		Org:      matches[1],
		Repo:     matches[2],
		Filename: matches[3],
		Ref:      matches[4],
		URL:      "https://github.com",
	}
}
