package vfs

import (
	"context"
	"testing"
	"time"

	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/rclone/rclone/vfs/vfsmeta"
	"github.com/stretchr/testify/require"
)

func TestMetadataSidecar(t *testing.T) {
	opt := vfscommon.Opt
	opt.PersistMetadata = true
	opt.MetadataStore = "sidecar"
	r, v := newTestVFSOpt(t, &opt)
	defer r.Finalise()
	r.WriteObject(context.Background(), "file", "", time.Now())
	m := uint32(0o100600)
	require.NoError(t, v.SaveMetadata(context.Background(), "file", false, vfsmeta.Meta{Mode: &m}))
	got, err := v.LoadMetadata(context.Background(), "file", false)
	require.NoError(t, err)
	require.NotNil(t, got.Mode)
	require.Equal(t, m, *got.Mode)
}

