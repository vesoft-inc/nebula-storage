package storage

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestStorage(t *testing.T) {
	assert := assert.New(t)
	logger, _ := zap.NewProduction()
	s, err := NewExternalStorage("local:///tmp/backup", logger)
	assert.NoError(err)
	assert.Equal(reflect.TypeOf(s).String(), "*storage.LocalBackedStore")

	assert.Equal(s.URI(), "/tmp/backup")
}
