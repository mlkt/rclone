package kopia

import (
	"context"
	"errors"
	"fmt"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"
	"sync"
	"time"
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "kopia",
		Description: "kopia",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "url",
			Help:     "URL of kopia host to connect to.",
			Required: true,
			Examples: []fs.OptionExample{{
				Value: "https://127.0.0.1:51515",
			}},
			Sensitive: true,
		}, {
			Name:      "user",
			Required:  true,
			Sensitive: true,
		}, {
			Name:      "host",
			Sensitive: true,
		}, {
			Name:      "path",
			Sensitive: true,
			Default:   "/",
		}, {
			Name:    "snapshot",
			Default: "latest",
			Examples: []fs.OptionExample{{
				Value: "latest",
			}, {
				Value: "pin",
			}, {
				Value: "kd23e26ad7ae4434e1f9eebbd39603a28",
			}},
			Sensitive: true,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	URL      string `config:"url"`
	User     string `config:"user"`
	Host     string `config:"host"`
	Path     string `config:"path"`
	Snapshot string `config:"snapshot"`
}

// Fs represents a remote seafile
type Fs struct {
	name     string
	root     string
	opt      Options
	features *fs.Features
	srv      *rest.Client
	pacer    *fs.Pacer
	initOnce sync.Once
	rootId   string

	rootEntries *fs.DirEntries
}

// NewFs creates a new Fs object from the name and root. It connects to
// the host specified in the config file.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	root = cleanPath(root)
	f := &Fs{
		name:     name,
		root:     root,
		opt:      *opt,
		features: &fs.Features{},
		srv:      rest.NewClient(fshttp.NewClient(ctx)).SetRoot(strings.TrimRight(opt.URL, "/")),
		pacer:    fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(10*time.Millisecond), pacer.MaxSleep(3200*time.Millisecond), pacer.DecayConstant(2))),
	}
	if root != "" {
		obj, err := f.newObject(ctx, root)
		if err != nil {
			return nil, err
		}
		if _, ok := obj.(*Object); ok {
			dir, _ := path.Split(root)
			if dir == "." || dir == "/" {
				dir = ""
			}
			f.root = dir
			return f, fs.ErrorIsFile
		}
	}
	return f, nil
}

func (f *Fs) getRootId(ctx context.Context) (string, error) {
	f.initOnce.Do(func() {
		result := SnapshotResponse{}
		var resp *http.Response
		var err error
		err = f.pacer.Call(func() (bool, error) {
			resp, err = f.srv.CallJSON(ctx, &rest.Opts{
				Method: "GET",
				Path:   "/api/v1/snapshots",
				Parameters: url.Values{
					"userName": []string{f.opt.User},
					"host":     []string{f.opt.Host},
					"path":     []string{f.opt.Path},
				},
			}, nil, &result)
			return f.shouldRetry(ctx, resp, err)
		})
		if err != nil {
			return
		}
		for i := len(result.Snapshots) - 1; i >= 0; i-- {
			snapshot := result.Snapshots[i]
			if f.opt.Snapshot == snapshot.RootID {
				f.rootId = snapshot.RootID
				break
			}
			if !slices.Contains(snapshot.Retention, "incomplete") {
				if (f.opt.Snapshot == "pin" && len(snapshot.Pins) > 0) ||
					(f.opt.Snapshot == "" || f.opt.Snapshot == "latest") {
					f.rootId = snapshot.RootID
					break
				}
			}
		}
		if f.rootId == "" {
			fs.Errorf(nil, "kopia snapshot: %s not found", f.opt.Snapshot)
			go func() {
				time.Sleep(3 * time.Second)
				f.initOnce = sync.Once{}
			}()
			return
		}
		fs.Infof(nil, "kopia load snapshot: %s", f.rootId)
	})
	if f.rootId == "" {
		return "", fmt.Errorf("%s not found", f.String())
	}
	return f.rootId, nil
}

func (f *Fs) shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	return fserrors.ShouldRetry(err) || resp == nil || resp.StatusCode >= 500, err
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("kopia %s[%s@%s:%s/%s]", f.name, f.opt.User, f.opt.Host, f.opt.Path, f.root)
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return time.Nanosecond
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

func cleanPath(p string) string {
	if p != "" {
		p = strings.Trim(path.Clean(p), "/")
	}
	return p
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return fs.ErrorDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	return f.list(ctx, path.Join(f.root, dir))
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	obj, err := f.newObject(ctx, path.Join(f.root, remote))
	if err != nil {
		return nil, err
	}
	if _, ok := obj.(*Directory); ok {
		return nil, fs.ErrorIsDir
	}
	return obj.(fs.Object), nil
}

func (f *Fs) listObject(ctx context.Context, remote string, objId string) (dirEntries fs.DirEntries, err error) {
	result := FileResponse{}
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &rest.Opts{
			Method: "GET",
			Path:   fmt.Sprintf("/api/v1/objects/%s", objId),
		}, nil, &result)
		return f.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}
	if resp == nil || !strings.Contains(resp.Header.Get("Content-Type"), "json") {
		return nil, fs.ErrorIsFile
	}
	for _, item := range result.Entries {
		var entry fs.DirEntry
		if item.Type == "d" {
			entry = &Directory{
				ObjectInfo: ObjectInfo{
					fs:      f,
					id:      item.Obj,
					name:    item.Name,
					remote:  path.Join(remote, item.Name),
					modTime: item.MTime,
					size:    item.Summary.Size,
				},
				entries: nil,
			}
		} else {
			entry = &Object{
				ObjectInfo: ObjectInfo{
					fs:      f,
					id:      item.Obj,
					name:    item.Name,
					remote:  path.Join(remote, item.Name),
					modTime: item.MTime,
					size:    item.Size,
				},
			}
		}
		dirEntries = append(dirEntries, entry)
	}
	return dirEntries, nil
}

func (f *Fs) list(ctx context.Context, remote string) (fs.DirEntries, error) {
	remote = cleanPath(remote)
	var dirEntries fs.DirEntries
	if remote == "" {
		if f.rootEntries != nil {
			dirEntries = *f.rootEntries
		} else {
			rootId, err := f.getRootId(ctx)
			if err != nil {
				return nil, err
			}
			dirEntries, err = f.listObject(ctx, remote, rootId)
			if err != nil {
				return nil, err
			}
			f.rootEntries = &dirEntries
		}
		return dirEntries, nil
	} else {
		obj, err := f.newObject(ctx, remote)
		if err != nil {
			if errors.Is(err, fs.ErrorObjectNotFound) {
				return nil, fs.ErrorDirNotFound
			}
			return nil, err
		}
		dirObj, ok := obj.(*Directory)
		if !ok {
			return nil, fs.ErrorIsFile
		}
		if dirObj.entries == nil {
			dirEntries, err = f.listObject(ctx, remote, dirObj.id)
			if err != nil {
				return nil, err
			}
			dirObj.entries = &dirEntries
		}
		return *dirObj.entries, nil
	}
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObject(ctx context.Context, remote string) (obj DirEntry, err error) {
	remote = cleanPath(remote)
	var dirEntries fs.DirEntries
	dir, file := path.Split(remote)
	dirEntries, err = f.list(ctx, dir)
	if err != nil {
		return nil, err
	}
	if file == "" {
		return nil, fs.ErrorIsDir
	}
	for _, item := range dirEntries {
		if item.(DirEntry).Name() == file {
			return item.(DirEntry), nil
		}
	}
	return nil, fs.ErrorObjectNotFound
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, fs.ErrorPermissionDenied
}

// Mkdir makes the directory or library
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return fs.ErrorPermissionDenied
}

// Rmdir removes the directory or library if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return fs.ErrorPermissionDenied
}
