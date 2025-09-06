package vfs

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
)

// PosixMeta represents persisted POSIX metadata as strings
type PosixMeta struct {
	Mode  *string `json:"mode,omitempty"`
	UID   *string `json:"uid,omitempty"`
	GID   *string `json:"gid,omitempty"`
	Mtime *string `json:"mtime,omitempty"`
	Atime *string `json:"atime,omitempty"`
	Btime *string `json:"btime,omitempty"`
}

// PosixMetaStore is a minimal sidecar-based metadata store bound to a VFS
type PosixMetaStore struct {
	Vfs *VFS
	Ext string
}

// IsSidecarPath reports whether p is a sidecar object path
func (s *PosixMetaStore) IsSidecarPath(p string) bool {
	if s.Ext == "" {
		return false
	}
	return strings.HasSuffix(p, s.Ext)
}

// metaPath returns the sidecar path for p
func (s *PosixMetaStore) metaPath(p string) string {
	e := s.Ext
	if e == "" {
		e = ".posixmeta"
	}
	return p + e
}

// Load reads meta from sidecar JSON
func (s *PosixMetaStore) Load(ctx context.Context, path string) (PosixMeta, error) {
	if s.IsSidecarPath(path) {
		return PosixMeta{}, fs.ErrorObjectNotFound
	}
	b, err := s.Vfs.ReadFile(s.metaPath(path))
	if err != nil {
		return PosixMeta{}, err
	}
	var m PosixMeta
	if len(b) == 0 {
		return PosixMeta{}, fs.ErrorObjectNotFound
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return PosixMeta{}, err
	}
	return m, nil
}

// Save writes meta to a temporary file then renames over the sidecar
func (s *PosixMetaStore) Save(ctx context.Context, path string, m PosixMeta) error {
	if s.IsSidecarPath(path) {
		return nil
	}
	cur, _ := s.Load(ctx, path)
	if m.Mode != nil { cur.Mode = m.Mode }
	if m.UID != nil { cur.UID = m.UID }
	if m.GID != nil { cur.GID = m.GID }
	if m.Mtime != nil { cur.Mtime = m.Mtime }
	if m.Atime != nil { cur.Atime = m.Atime }
	if m.Btime != nil { cur.Btime = m.Btime }
	p := s.metaPath(path)
	tmp := p + ".tmp"
	w, err := s.Vfs.Create(tmp)
	if err != nil { return err }
	enc, err := json.Marshal(cur)
	if err != nil { _ = w.Close(); _ = s.Vfs.Remove(tmp); return err }
	if _, err = w.Write(enc); err != nil { _ = w.Close(); _ = s.Vfs.Remove(tmp); return err }
	if err = w.Close(); err != nil { _ = s.Vfs.Remove(tmp); return err }
	if err = s.Vfs.Rename(tmp, p); err != nil { _ = s.Vfs.Remove(tmp); return err }
	return nil
}

// Delete removes sidecar if present
func (s *PosixMetaStore) Delete(ctx context.Context, path string) error {
	if s.IsSidecarPath(path) {
		return nil
	}
	return s.Vfs.Remove(s.metaPath(path))
}

// Rename moves the sidecar along with the object if exists
func (s *PosixMetaStore) Rename(ctx context.Context, oldPath, newPath string) error {
	if s.IsSidecarPath(oldPath) || s.IsSidecarPath(newPath) {
		return nil
	}
	oldM := s.metaPath(oldPath)
	newM := s.metaPath(newPath)
	if _, err := s.Vfs.Stat(oldM); err != nil {
		return nil
	}
	return s.Vfs.Rename(oldM, newM)
}

// FormatPosixMode returns a string like 100644/040755 including type bits
func FormatPosixMode(m os.FileMode, isDir bool) string {
	// Use os.FileMode's underlying bits
	mode := uint32(m)
	if isDir {
		// S_IFDIR
		mode &^= 0xF000
		mode |= 0x4000
	} else {
		// S_IFREG
		mode &^= 0xF000
		mode |= 0x8000
	}
	return strconv.FormatUint(uint64(mode), 8)
}

// ParsePosixMode parses an octal string with type bits to FileMode
func ParsePosixMode(s string) os.FileMode {
	u, err := strconv.ParseUint(s, 8, 32)
	if err != nil {
		return 0
	}
	return os.FileMode(u)
}

// ParsePosixTime parses RFC3339 in UTC
func ParsePosixTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

// PosixAnyFieldSet returns true if any field is set
func PosixAnyFieldSet(m PosixMeta) bool {
	return m.Mode != nil || m.UID != nil || m.GID != nil || m.Mtime != nil || m.Atime != nil || m.Btime != nil
}
