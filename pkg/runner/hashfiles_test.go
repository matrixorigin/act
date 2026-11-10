package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/nektos/act/pkg/model"
	"github.com/stretchr/testify/require"
)

func TestHashFilesUsesRepositoryContent(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(directory, "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "a.txt"), []byte("first"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "nested", "b.txt"), []byte("second"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "ignored.go"), []byte("ignored"), 0o600))
	first := sha256.Sum256([]byte("first"))
	second := sha256.Sum256([]byte("second"))
	aggregate := sha256.New()
	aggregate.Write(first[:])
	aggregate.Write(second[:])

	actual, err := HashFiles(t.Context(), directory, []string{"**/*.txt"}, false)
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(aggregate.Sum(nil)), actual)
	empty, err := HashFiles(t.Context(), directory, []string{"**/*.missing"}, false)
	require.NoError(t, err)
	require.Empty(t, empty)
}

func TestHashFilesFunctionUsesConfiguredCallback(t *testing.T) {
	var patterns []string
	follow := false
	rc := &RunContext{
		Config: &Config{Env: map[string]string{}, Secrets: map[string]string{}, HashFiles: func(_ context.Context, actual []string, followSymbolicLinks bool) (string, error) {
			patterns = actual
			follow = followSymbolicLinks
			return "frozen-digest", nil
		}},
		Run: &model.Run{JobID: "build", Workflow: &model.Workflow{Jobs: map[string]*model.Job{"build": {}}}},
	}

	actual := rc.NewExpressionEvaluator(t.Context()).Interpolate(t.Context(), "${{ hashFiles('--follow-symbolic-links', '**/*.txt') }}")
	require.Equal(t, "frozen-digest", actual)
	require.Equal(t, []string{"**/*.txt"}, patterns)
	require.True(t, follow)
}
