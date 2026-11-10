package model

import (
	"errors"
	"fmt"
	"slices"

	"github.com/nektos/act/pkg/workflowpattern"
	"gopkg.in/yaml.v3"
)

// ErrMergeGroupEventFilterUnsupported identifies merge_group filters that
// cannot be evaluated against the frozen event.

var ErrMergeGroupEventFilterUnsupported = errors.New("merge_group event filters are unsupported")

func (w *Workflow) mergeGroupEventSelected(baseBranch string) (bool, error) {
	if w.RawOn.Kind != yaml.MappingNode {
		return true, nil
	}
	for index := 0; index < len(w.RawOn.Content); index += 2 {
		if w.RawOn.Content[index].Value != "merge_group" {
			continue
		}
		filter := w.RawOn.Content[index+1]
		if filter.Tag == "!!null" {
			return true, nil
		}
		if filter.Kind != yaml.MappingNode {
			return false, ErrMergeGroupEventFilterUnsupported
		}
		selected := true
		var branches, ignored []string
		for index := 0; index < len(filter.Content); index += 2 {
			name := filter.Content[index].Value
			values, err := mergeGroupFilterValues(filter.Content[index+1])
			if err != nil {
				return false, err
			}
			switch name {
			case "types":
				selected = slices.Contains(values, "checks_requested")
			case "branches":
				branches = values
			case "branches-ignore":
				ignored = values
			default:
				return false, ErrMergeGroupEventFilterUnsupported
			}
		}
		if len(branches) > 0 && len(ignored) > 0 {
			return false, fmt.Errorf("%w: branches and branches-ignore cannot be combined", ErrMergeGroupEventFilterUnsupported)
		}
		if len(branches) == 0 && len(ignored) == 0 {
			return selected, nil
		}
		patterns := branches
		if len(ignored) > 0 {
			patterns = ignored
		}
		compiled, err := workflowpattern.CompilePatterns(patterns...)
		if err != nil {
			return false, fmt.Errorf("%w: branch filter: %w", ErrMergeGroupEventFilterUnsupported, err)
		}
		trace := &workflowpattern.EmptyTraceWriter{}
		if len(branches) > 0 {
			selected = selected && !workflowpattern.Skip(compiled, []string{baseBranch}, trace)
		} else {
			selected = selected && !workflowpattern.Filter(compiled, []string{baseBranch}, trace)
		}
		return selected, nil
	}
	return false, nil
}

func mergeGroupFilterValues(node *yaml.Node) ([]string, error) {
	if node.Kind == yaml.ScalarNode {
		if node.Tag != "!!str" || node.Value == "" {
			return nil, ErrMergeGroupEventFilterUnsupported
		}
		return []string{node.Value}, nil
	}
	if node.Kind != yaml.SequenceNode || len(node.Content) == 0 {
		return nil, ErrMergeGroupEventFilterUnsupported
	}
	values := make([]string, 0, len(node.Content))
	for _, item := range node.Content {
		if item.Kind != yaml.ScalarNode || item.Tag != "!!str" || item.Value == "" {
			return nil, ErrMergeGroupEventFilterUnsupported
		}
		values = append(values, item.Value)
	}
	return values, nil
}
