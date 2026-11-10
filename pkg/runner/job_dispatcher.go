package runner

import (
	"context"
	"fmt"
	"maps"

	"github.com/nektos/act/pkg/common"
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

func newDispatchedJobExecutor(dispatcher JobDispatcher, info jobInfo, rc *RunContext) common.Executor {
	if len(info.steps()) == 0 {
		return common.NewDebugExecutor("No steps found")
	}
	return func(ctx context.Context) error {
		if len(info.matrix()) > 0 {
			common.Logger(ctx).Infof("\U0001F9EA  Matrix: %v", info.matrix())
		}
		result, err := dispatcher.DispatchJob(ctx, rc)
		success := false
		if err == nil {
			switch result.Conclusion {
			case JobConclusionSuccess:
				success = true
			case JobConclusionFailure:
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
