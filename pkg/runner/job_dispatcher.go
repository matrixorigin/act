package runner

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/nektos/act/pkg/common"
	"github.com/nektos/act/pkg/exprparser"
	"github.com/nektos/act/pkg/model"
	"gopkg.in/yaml.v3"
)

// JobConclusion is the durable outcome of a remotely dispatched Job.
type JobConclusion string

const (
	JobConclusionSuccess  JobConclusion = "success"
	JobConclusionFailure  JobConclusion = "failure"
	JobConclusionCanceled JobConclusion = "canceled"
)

// JobResult contains final Runner values. Outputs must already be evaluated;
// act does not interpolate them a second time.
type JobResult struct {
	Conclusion JobConclusion
	Outputs    map[string]string
}

// JobDispatcher executes one fully expanded Job and waits for its durable
// result. Implementations must be safe for concurrent matrix and parallel Job
// calls, and must not retain RunContext after DispatchJob returns.
type JobDispatcher interface {
	DispatchJob(ctx context.Context, rc *RunContext) (JobResult, error)
}

// JobDispatcherFunc adapts a function to JobDispatcher.
type JobDispatcherFunc func(context.Context, *RunContext) (JobResult, error)

// DispatchJob calls f(ctx, rc).
func (f JobDispatcherFunc) DispatchJob(ctx context.Context, rc *RunContext) (JobResult, error) {
	return f(ctx, rc)
}

// JobInvocation identifies one Job in a reusable-workflow call chain. Matrix
// contains the fully expanded values for that invocation.
type JobInvocation struct {
	Workflow            string
	WorkflowRepository  string
	WorkflowRef         string
	WorkflowSHA         string
	WorkflowFilePath    string
	JobID               string
	Matrix              map[string]any
	Permissions         yaml.Node `json:"-"`
	WorkflowConcurrency *ConcurrencySpec
	JobConcurrency      *ConcurrencySpec
}

// ConcurrencySpec is the evaluated concurrency configuration for one workflow
// or Job invocation. Each reusable-workflow level is evaluated by its own
// RunContext so inputs and matrix values cannot leak between callers.
type ConcurrencySpec struct {
	Group            string
	CancelInProgress bool
}

// JobInvocationPath returns the outer-to-inner reusable-workflow call chain,
// including rc itself. Callers can combine this path with their run/attempt ID
// to resume a dispatched Job without duplicating it. Returned matrices and
// permission nodes must be treated as immutable. Permissions is the effective
// Job-over-Workflow declaration at each reusable-workflow boundary.
func (rc *RunContext) JobInvocationPath() []JobInvocation {
	contexts := rc.jobInvocationContexts()
	path := make([]JobInvocation, 0, len(contexts))
	for _, current := range contexts {
		path = append(path, current.jobInvocation())
	}
	return path
}

// JobInvocationPathWithConcurrency returns the invocation path with every
// workflow and Job concurrency expression evaluated in its original context.
// The source YAML nodes remain immutable for concurrent matrix dispatches.
func (rc *RunContext) JobInvocationPathWithConcurrency(ctx context.Context) ([]JobInvocation, error) {
	contexts := rc.jobInvocationContexts()
	path := make([]JobInvocation, 0, len(contexts))
	for _, current := range contexts {
		workflowConcurrency, err := current.evaluateConcurrency(ctx, current.Run.Workflow.RawConcurrency)
		if err != nil {
			return nil, fmt.Errorf("evaluate workflow %q concurrency: %w", current.Run.Workflow.File, err)
		}
		jobConcurrency, err := current.evaluateConcurrency(ctx, current.Run.Job().RawConcurrency)
		if err != nil {
			return nil, fmt.Errorf("evaluate Job %q concurrency: %w", current.Run.JobID, err)
		}
		invocation := current.jobInvocation()
		invocation.WorkflowConcurrency = workflowConcurrency
		invocation.JobConcurrency = jobConcurrency
		path = append(path, invocation)
	}
	return path, nil
}

func (rc *RunContext) jobInvocation() JobInvocation {
	var permissions yaml.Node
	if job := rc.Run.Job(); job != nil {
		permissions = job.RawPermissions
	}
	if permissions.Kind == 0 {
		permissions = rc.Run.Workflow.RawPermissions
	}
	source := rc.workflowSource()
	workflowRef := ""
	if source.Repository != "" && source.Path != "" && source.Ref != "" {
		workflowRef = source.Repository + "/" + source.Path + "@" + source.Ref
	}
	return JobInvocation{Workflow: rc.Run.Workflow.File, WorkflowRepository: source.Repository, WorkflowRef: workflowRef, WorkflowSHA: source.SHA,
		WorkflowFilePath: source.Path, JobID: rc.Run.JobID, Matrix: maps.Clone(rc.Matrix), Permissions: permissions}
}

func (rc *RunContext) jobInvocationContexts() []*RunContext {
	var contexts []*RunContext
	for current := rc; current != nil; {
		if current.Run != nil && current.Run.Workflow != nil {
			contexts = append(contexts, current)
		}
		if current.caller == nil {
			break
		}
		current = current.caller.runContext
	}
	slices.Reverse(contexts)
	return contexts
}

func (rc *RunContext) evaluateConcurrency(ctx context.Context, source yaml.Node) (*ConcurrencySpec, error) {
	if source.Kind == 0 {
		return nil, nil
	}
	node := cloneConcurrencyNode(source)
	if err := rc.NewExpressionEvaluator(ctx).EvaluateYamlNode(ctx, &node); err != nil {
		return nil, err
	}
	spec := &ConcurrencySpec{}
	switch node.Kind {
	case yaml.ScalarNode:
		if err := node.Decode(&spec.Group); err != nil {
			return nil, err
		}
	case yaml.MappingNode:
		var raw struct {
			Group            string `yaml:"group"`
			CancelInProgress bool   `yaml:"cancel-in-progress"`
		}
		if err := node.Decode(&raw); err != nil {
			return nil, err
		}
		spec.Group, spec.CancelInProgress = raw.Group, raw.CancelInProgress
	default:
		return nil, fmt.Errorf("concurrency must be a group or mapping")
	}
	if spec.Group == "" {
		return nil, fmt.Errorf("concurrency group must not be empty")
	}
	return spec, nil
}

func cloneConcurrencyNode(source yaml.Node) yaml.Node {
	cloned := source
	cloned.Content = make([]*yaml.Node, len(source.Content))
	for index, child := range source.Content {
		if child == nil {
			continue
		}
		node := cloneConcurrencyNode(*child)
		cloned.Content[index] = &node
	}
	if source.Alias != nil {
		node := cloneConcurrencyNode(*source.Alias)
		cloned.Alias = &node
	}
	return cloned
}

// JobDispatchContext contains the act-evaluated context needed by an external
// executor. GitHub.Token and Secrets are sensitive and must only be used at the
// executor's credential-delivery boundary. All returned values must be treated
// as immutable for the duration of DispatchJob.
type JobDispatchContext struct {
	GitHub  *model.GithubContext
	Inputs  map[string]any
	Secrets map[string]string
	Vars    map[string]string
	Env     map[string]string
	// Strategy contains the same expanded values used by act's evaluator.
	Strategy map[string]any
}

// JobDispatchContext resolves the same expression contexts act uses for the
// current Job, including inputs and secrets inherited through reusable calls.
// Env contains only Workflow and Job env. Config.Env belongs to act's local
// process and must not leak into an external Runner's workflow env context.
func (rc *RunContext) JobDispatchContext(ctx context.Context) JobDispatchContext {
	github := rc.getGithubContext(ctx)
	env := mergeMaps(rc.Run.Workflow.Env, rc.Run.Job().Environment())
	evaluator := rc.NewExpressionEvaluator(ctx)
	for name, value := range env {
		env[name] = evaluator.Interpolate(ctx, value)
	}
	// The external executor owns its local workspace and materializes the
	// webhook payload. Never export act's process-local paths.
	github.EventPath = ""
	github.Workspace = ""
	vars := maps.Clone(getWorkflowVars(ctx, rc))
	// External executors serialize Vars as a ContextData dictionary, not null.
	if vars == nil {
		vars = map[string]string{}
	}
	return JobDispatchContext{
		GitHub:   github,
		Inputs:   getEvaluatorInputs(ctx, rc, nil, github),
		Secrets:  maps.Clone(getWorkflowSecrets(ctx, rc)),
		Vars:     vars,
		Env:      env,
		Strategy: rc.jobStrategy(),
	}
}

func newDispatchedJobExecutor(dispatcher JobDispatcher, info jobInfo, rc *RunContext) common.Executor {
	if len(info.steps()) == 0 {
		return common.NewDebugExecutor("No steps found")
	}
	return func(ctx context.Context) error {
		if len(info.matrix()) > 0 {
			common.Logger(ctx).Infof("\U0001F9EA  Matrix: %v", info.matrix())
		}
		timeout, err := dispatchedJobTimeout(ctx, rc)
		if err != nil {
			return err
		}
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		result, err := dispatcher.DispatchJob(ctx, rc)
		success := false
		if err == nil {
			switch result.Conclusion {
			case JobConclusionSuccess:
				success = true
			case JobConclusionFailure:
				if rc.Run.Job().RawContinueOnError != "" {
					continueOnError, evalErr := EvalBool(ctx, rc.NewExpressionEvaluator(ctx), rc.Run.Job().RawContinueOnError, exprparser.DefaultStatusCheckNone)
					if evalErr != nil {
						err = evalErr
					} else if continueOnError {
						success = true
					}
				}
			case JobConclusionCanceled:
				err = context.Cause(ctx)
				if err == nil {
					err = context.Canceled
				}
			default:
				err = fmt.Errorf("remote Job returned invalid conclusion %q", result.Conclusion)
			}
			if result.Conclusion == JobConclusionSuccess || result.Conclusion == JobConclusionFailure || result.Conclusion == JobConclusionCanceled {
				rc.Run.Job().Outputs = maps.Clone(result.Outputs)
				if rc.Run.Job().Outputs == nil {
					rc.Run.Job().Outputs = map[string]string{}
				}
			}
		}
		setJobResult(ctx, info, rc, success)
		setJobOutputs(ctx, rc)
		return err
	}
}

func dispatchedJobTimeout(ctx context.Context, rc *RunContext) (time.Duration, error) {
	raw := rc.Run.Job().TimeoutMinutes
	if raw == "" {
		return 0, nil
	}
	value := rc.NewExpressionEvaluator(ctx).Interpolate(ctx, raw)
	minutes, err := strconv.ParseInt(value, 10, 64)
	if err != nil || minutes <= 0 || minutes > math.MaxInt64/int64(time.Minute) {
		return 0, fmt.Errorf("remote Job timeout-minutes must resolve to a positive integer: %q", raw)
	}
	return time.Duration(minutes) * time.Minute, nil
}
