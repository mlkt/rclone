package kopia

import (
	"context"
	"fmt"
	"github.com/rclone/rclone/lib/rest"
	"io"
	"net/http"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
)

type DirEntry interface {
	Name() string
}

type ObjectInfo struct {
	fs      *Fs
	id      string
	name    string
	remote  string
	size    int64
	modTime time.Time
}

type Object struct {
	ObjectInfo
}

type Directory struct {
	ObjectInfo
	entries *fs.DirEntries
}

func (o *Directory) Items() int64 {
	if o.entries == nil {
		return -1
	}
	return int64(len(*o.entries))
}

func (o *ObjectInfo) Name() string {
	return o.name
}

// Return a string version
func (o *ObjectInfo) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote string
func (o *ObjectInfo) Remote() string {
	s := len(o.fs.root)
	if s == 0 {
		return o.remote
	} else {
		return o.remote[s+1:]
	}
}

// ModTime returns last modified time
func (o *ObjectInfo) ModTime(context.Context) time.Time {
	return o.modTime
}

// Size returns the size of an object in bytes
func (o *ObjectInfo) Size() int64 {
	return o.size
}

// ==================== Interface fs.ObjectInfo ====================

// Fs returns the parent Fs
func (o *ObjectInfo) Fs() fs.Info {
	return o.fs
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (o *ObjectInfo) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Storable says whether this object can be stored
func (o *ObjectInfo) Storable() bool {
	return true
}

// ==================== Interface fs.Object ====================

// SetModTime sets the metadata on the object to set the modification date
func (o *ObjectInfo) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (reader io.ReadCloser, err error) {
	var resp *http.Response
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.CallJSON(ctx, &rest.Opts{
			Method: "GET",
			Path:   fmt.Sprintf("/api/v1/objects/%s", o.id),
		}, nil, nil)
		return o.fs.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *ObjectInfo) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return fs.ErrorPermissionDenied
}

// Remove this object
func (o *ObjectInfo) Remove(ctx context.Context) error {
	return fs.ErrorPermissionDenied
}

// ==================== Optional Interface fs.IDer ====================

// ID returns the ID of the Object if known, or "" if not
func (o *ObjectInfo) ID() string {
	return o.id
}
