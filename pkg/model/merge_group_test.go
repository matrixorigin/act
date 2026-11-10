package model

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlanMergeGroupEventFilters(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		on         string
		baseBranch string
		wantJob    bool
	}{
		{name: "scalar event", on: "merge_group", baseBranch: "dev", wantJob: true},
		{name: "event list", on: "[pull_request, merge_group]", baseBranch: "dev", wantJob: true},
		{name: "null filter", on: "{merge_group:}", baseBranch: "dev", wantJob: true},
		{name: "scalar activity", on: "{merge_group: {types: checks_requested}}", baseBranch: "dev", wantJob: true},
		{name: "activity list", on: "{merge_group: {types: [checks_requested]}}", baseBranch: "dev", wantJob: true},
		{name: "scalar matching branch", on: "{merge_group: {branches: dev}}", baseBranch: "dev", wantJob: true},
		{name: "matching branch", on: "{merge_group: {branches: [main, 'dev']}}", baseBranch: "dev", wantJob: true},
		{name: "other branch", on: "{merge_group: {branches: [main, 'releases/**']}}", baseBranch: "dev"},
		{name: "matching ignored branch", on: "{merge_group: {branches-ignore: [main, 'dev']}}", baseBranch: "dev"},
		{name: "other ignored branch", on: "{merge_group: {branches-ignore: [main, 'releases/**']}}", baseBranch: "dev", wantJob: true},
		{name: "negated branch pattern", on: "{merge_group: {branches: ['releases/**', '!releases/**-alpha']}}", baseBranch: "releases/beta", wantJob: true},
		{name: "excluded branch pattern", on: "{merge_group: {branches: ['releases/**', '!releases/**-alpha']}}", baseBranch: "releases/one-alpha"},
		{name: "different event", on: "pull_request", baseBranch: "dev"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			planner, err := NewSingleWorkflowPlanner("merge.yml", strings.NewReader("on: "+scenario.on+`
jobs:
  build:
    runs-on: self-hosted
    steps:
      - run: echo build
`))
			assert.NoError(t, err)

			plan, err := PlanMergeGroupEvent(planner, scenario.baseBranch)
			assert.NoError(t, err)
			var jobs []string
			for _, stage := range plan.Stages {
				for _, run := range stage.Runs {
					jobs = append(jobs, run.JobID)
				}
			}
			if scenario.wantJob {
				assert.Equal(t, []string{"build"}, jobs)
			} else {
				assert.Empty(t, jobs)
			}
		})
	}
}

func TestPlanMergeGroupEventRejectsInvalidFilters(t *testing.T) {
	for _, on := range []string{
		"{merge_group: {branches: [dev], branches-ignore: [main]}}",
		"{merge_group: {branches: ['[']}}",
		"{merge_group: {types: []}}",
	} {
		t.Run(on, func(t *testing.T) {
			planner, err := NewSingleWorkflowPlanner("merge.yml", strings.NewReader("on: "+on+`
jobs:
  build:
    runs-on: self-hosted
    steps:
      - run: echo build
`))
			assert.NoError(t, err)

			plan, err := PlanMergeGroupEvent(planner, "dev")
			assert.ErrorIs(t, err, ErrMergeGroupEventFilterUnsupported)
			assert.ErrorIs(t, err, ErrInvalidWorkflowDefinition)
			assert.NotNil(t, plan)
		})
	}
}

func TestPlanEventRetainsEventNameOnlyBehavior(t *testing.T) {
	planner, err := NewSingleWorkflowPlanner("merge.yml", strings.NewReader(`on: {merge_group: {branches: [main]}}
jobs:
  build:
    runs-on: self-hosted
    steps:
      - run: echo build
`))
	assert.NoError(t, err)

	plan, err := planner.PlanEvent("merge_group")
	assert.NoError(t, err)
	assert.Len(t, plan.Stages, 1)
	assert.Equal(t, "build", plan.Stages[0].Runs[0].JobID)
}
