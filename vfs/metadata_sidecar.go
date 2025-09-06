package vfs

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/rclone/rclone/vfs/vfsmeta"
)

type sidecarStore struct {
	vfs *VFS
	ext string
}

func newSidecarStore(vfs *VFS, ext string) *sidecarStore {
	return &sidecarStore{vfs: vfs, ext: ext}
}

func (s *sidecarStore) name(p string) string {
	return strings.TrimSuffix(p, "/") + s.ext
}

func (s *sidecarStore) Load(ctx context.Context, p string, isDir bool) (vfsmeta.Meta, error) {
	b, err := s.vfs.ReadFile(s.name(p))
	if err != nil {
		return vfsmeta.Meta{}, err
	}
	var m vfsmeta.Meta
	err = json.Unmarshal(b, &m)
	if err != nil {
		return vfsmeta.Meta{}, err
	}
	return m, nil
}

func (s *sidecarStore) Save(ctx context.Context, p string, isDir bool, m vfsmeta.Meta) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return s.vfs.WriteFile(s.name(p), b, 0o600)
}

func (s *sidecarStore) Rename(ctx context.Context, oldPath, newPath string, isDir bool) error {
	return s.vfs.Rename(s.name(oldPath), s.name(newPath))
}

func (s *sidecarStore) Delete(ctx context.Context, p string, isDir bool) error {
	return s.vfs.Remove(s.name(p))
}
