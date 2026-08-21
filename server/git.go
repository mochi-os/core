// Mochi server: Git repository operations
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dsnet/compress/bzip2"
	"github.com/gin-gonic/gin"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/sideband"
	"github.com/go-git/go-git/v5/plumbing/revlist"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

var api_git = sls.FromStringDict(sl.String("mochi.git"), sl.StringDict{
	"init":     sl.NewBuiltin("mochi.git.init", api_git_init),
	"delete":   sl.NewBuiltin("mochi.git.delete", api_git_delete),
	"path":     sl.NewBuiltin("mochi.git.path", api_git_path),
	"size":     sl.NewBuiltin("mochi.git.size", api_git_size),
	"refs":     sl.NewBuiltin("mochi.git.refs", api_git_refs),
	"branches": sl.NewBuiltin("mochi.git.branches", api_git_branches),
	"tags":     sl.NewBuiltin("mochi.git.tags", api_git_tags),
	"tree":     sl.NewBuiltin("mochi.git.tree", api_git_tree),
	"archive":  sl.NewBuiltin("mochi.git.archive", api_git_archive),
	"branch": sls.FromStringDict(sl.String("mochi.git.branch"), sl.StringDict{
		"create": sl.NewBuiltin("mochi.git.branch.create", api_git_branch_create),
		"delete": sl.NewBuiltin("mochi.git.branch.delete", api_git_branch_delete),
		"default": sls.FromStringDict(sl.String("mochi.git.branch.default"), sl.StringDict{
			"get": sl.NewBuiltin("mochi.git.branch.default.get", api_git_branch_default_get),
			"set": sl.NewBuiltin("mochi.git.branch.default.set", api_git_branch_default_set),
		}),
	}),
	"commit": sls.FromStringDict(sl.String("mochi.git.commit"), sl.StringDict{
		"list":    sl.NewBuiltin("mochi.git.commit.list", api_git_commit_list),
		"get":     sl.NewBuiltin("mochi.git.commit.get", api_git_commit_get),
		"log":     sl.NewBuiltin("mochi.git.commit.log", api_git_commit_log),
		"between": sl.NewBuiltin("mochi.git.commit.between", api_git_commit_between),
	}),
	"blob": sls.FromStringDict(sl.String("mochi.git.blob"), sl.StringDict{
		"content": sl.NewBuiltin("mochi.git.blob.content", api_git_blob_content),
		"get":     sl.NewBuiltin("mochi.git.blob.get", api_git_blob_get),
	}),
	"diff": &git_diff_module{},
	"merge": sls.FromStringDict(sl.String("mochi.git.merge"), sl.StringDict{
		"base":    sl.NewBuiltin("mochi.git.merge.base", api_git_merge_base),
		"check":   sl.NewBuiltin("mochi.git.merge.check", api_git_merge_check),
		"perform": sl.NewBuiltin("mochi.git.merge.perform", api_git_merge_perform),
	}),
})

// git_loader implements server.Loader to load repository storage from filesystem paths.
// budget, when positive, is the number of decoded object bytes a push may add
// (see git_storage).
type git_loader struct {
	budget int64
}

// Load loads a storer.Storer for the given endpoint path
func (l *git_loader) Load(ep *transport.Endpoint) (storer.Storer, error) {
	fs := osfs.New(ep.Path)
	if _, err := fs.Stat("config"); err != nil {
		return nil, transport.ErrRepositoryNotFound
	}
	// Wrap in git_storage to hide PackfileWriter interface. Without this,
	// packfile.UpdateObjectStorage takes a raw-copy path that can't resolve
	// thin pack deltas (base objects not included in the pack). The wrapper
	// forces the parser path which looks up base objects from the storer.
	return &git_storage{
		Storer:    filesystem.NewStorage(fs, cache.NewObjectLRUDefault()),
		remaining: l.budget,
		metered:   l.budget > 0,
	}, nil
}

// git_storage wraps storer.Storer to hide PackfileWriter, so the packfile
// parser resolves thin-pack deltas from the object store and hands every
// decoded object to SetEncodedObject - the one place that sees what a pack
// expands to. The meter counts decoded bytes against a quota measured on disk,
// so it refuses early.
type git_storage struct {
	storer.Storer
	remaining int64 // decoded bytes still allowed, when metered
	metered   bool
}

func (s *git_storage) SetEncodedObject(obj plumbing.EncodedObject) (plumbing.Hash, error) {
	if s.metered {
		s.remaining -= obj.Size()
		if s.remaining < 0 {
			return plumbing.ZeroHash, fmt.Errorf("push exceeds the storage available to this account")
		}
	}
	return s.Storer.SetEncodedObject(obj)
}

// git_transport is the go-git server transport for handling git protocol.
// Unmetered: it serves fetches and ref advertisements, which store nothing.
// Pushes build their own transport carrying the owner's remaining storage.
var git_transport = server.NewServer(&git_loader{})

// git_diff_module is a callable module that also has a .stats method
type git_diff_module struct{}

func (m *git_diff_module) String() string        { return "mochi.git.diff" }
func (m *git_diff_module) Type() string          { return "module" }
func (m *git_diff_module) Freeze()               {}
func (m *git_diff_module) Truth() sl.Bool        { return sl.True }
func (m *git_diff_module) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: module") }
func (m *git_diff_module) Name() string          { return "mochi.git.diff" }
func (m *git_diff_module) AttrNames() []string   { return []string{"stats"} }

func (m *git_diff_module) Attr(name string) (sl.Value, error) {
	if name == "stats" {
		return sl.NewBuiltin("mochi.git.diff.stats", api_git_diff_stats), nil
	}
	return nil, nil
}

func (m *git_diff_module) CallInternal(thread *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return api_git_diff(thread, nil, args, kwargs)
}

// git_repo_path returns a repository's directory. It uses the calling app's id,
// so it lines up with every other path-composing API: published apps land at
// users/<uid>/<app-entity-id>/<repo-entity>/, dev apps at
// users/<uid>/<app-name>/<repo-entity>/.
func git_repo_path(owner *User, app *App, entity string) string {
	return fmt.Sprintf("%s/users/%s/%s/%s", data_dir, owner.UID, app.id, entity)
}

// Open a repository
func git_open(owner *User, app *App, entity string) (*git.Repository, error) {
	path := git_repo_path(owner, app, entity)
	return git.PlainOpen(path)
}

// git_can_write reports whether the thread's authenticated identity holds
// repository/<entity> write in owner's repositories ACL - the same grant a git
// push requires - and fails closed. A P2P event runs as the entity owner, so a
// caller authorizing a remote initiator must check its verified `from` itself.
func git_can_write(t *sl.Thread, owner *User, app *App, entity string) bool {
	if owner == nil || app == nil || entity == "" {
		return false
	}
	user := principal_caller(t)
	if user == nil {
		return false
	}
	// Prefer the acting identity (set in action / service / remote contexts);
	// fall back to the account's person entity, matching the git push
	// (receive-pack) path. Fail closed if neither yields an identity.
	identity_id := ""
	if user.Identity != nil {
		identity_id = user.Identity.ID
	} else if ident := user.identity(); ident != nil {
		identity_id = ident.ID
	}
	if identity_id == "" {
		return false
	}
	app_db := db_app_system(owner, app)
	if app_db == nil {
		return false
	}
	return app_db.access_check(owner, identity_id, user.Role, "repository/"+entity, "write")
}

// git_can_read reports whether the thread's identity - or anyone, for a public
// "*" read grant - holds repository/<entity> read. Unlike git_can_write it does
// not fail closed on a missing identity, so anonymous read of public
// repositories survives. It cannot see that core runs a `public: true` action
// as the entity OWNER and a service call as the CALLING user; refusing those is
// the app's job.
func git_can_read(t *sl.Thread, owner *User, app *App, entity string) bool {
	if owner == nil || app == nil || entity == "" {
		return false
	}
	identity_id := ""
	role := ""
	if user := principal_caller(t); user != nil {
		if user.Identity != nil {
			identity_id = user.Identity.ID
		} else if ident := user.identity(); ident != nil {
			identity_id = ident.ID
		}
		role = user.Role
	}
	app_db := db_app_system(owner, app)
	if app_db == nil {
		return false
	}
	return app_db.access_check(owner, identity_id, role, "repository/"+entity, "read")
}

// git_init creates a bare repository with no commits and no refs - only HEAD
// pointing symbolically at refs/heads/main, so the first pushed branch lands as
// main. No placeholder commit: one makes every first push a divergent-history
// rejection. The display layer treats a repo with no refs as empty.
func git_init(owner *User, app *App, entity string) error {
	path := git_repo_path(owner, app, entity)

	// Create parent directory if needed
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	repo, err := git.PlainInit(path, true) // true = bare repository
	if err != nil {
		return err
	}

	// Set HEAD to point to main
	head := plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))
	return repo.Storer.SetReference(head)
}

// git_placeholder_sweep restores repositories created before git_init stopped
// manufacturing a placeholder commit, so a first push is not refused as
// unrelated history. Runs once at startup. The glob spans every app directory
// because app.id is the app's entity id on a published install, not the app's
// name.
func git_placeholder_sweep() {
	const empty_tree = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
	candidates, err := filepath.Glob(filepath.Join(data_dir, "users", "*", "*", "*"))
	if err != nil {
		return
	}
	swept := 0
	for _, path := range candidates {
		if !file_exists(filepath.Join(path, "HEAD")) {
			continue
		}
		repo, err := git.PlainOpen(path)
		if err != nil {
			continue
		}
		refs, err := repo.References()
		if err != nil {
			continue
		}
		var only *plumbing.Reference
		count := 0
		refs.ForEach(func(r *plumbing.Reference) error {
			if r.Type() == plumbing.HashReference {
				count++
				only = r
			}
			return nil
		})
		if count != 1 || only == nil || only.Name() != plumbing.NewBranchReferenceName("main") {
			continue
		}
		commit, err := repo.CommitObject(only.Hash())
		if err != nil {
			continue
		}
		if commit.NumParents() != 0 || commit.TreeHash.String() != empty_tree ||
			strings.TrimSpace(commit.Message) != "Initial commit" ||
			commit.Author.Name != "Mochi" || commit.Author.Email != "mochi@localhost" {
			continue
		}
		if err := repo.Storer.RemoveReference(only.Name()); err != nil {
			warn("git: unable to remove placeholder ref in %q: %v", path, err)
			continue
		}
		swept++
	}
	if swept > 0 {
		info("git: returned %d repository/repositories holding only the old placeholder commit to empty", swept)
	}
}

// Delete a repository
func git_delete(owner *User, app *App, entity string) error {
	path := git_repo_path(owner, app, entity)
	return os.RemoveAll(path)
}

// Get repository size in bytes
func git_size(owner *User, app *App, entity string) (int64, error) {
	path := git_repo_path(owner, app, entity)
	var size int64

	err := filepath.Walk(path, func(_ string, information os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !information.IsDir() {
			size += information.Size()
		}
		return nil
	})

	return size, err
}

// git_branch_reference builds refs/heads/<name> and validates it before any
// storer sees it. plumbing concatenates without checking and the filesystem
// storer joins the result onto the repository directory, so a name containing
// ".." resolves onto the repository's own config, HEAD or packed-refs.
func git_branch_reference(name string) (plumbing.ReferenceName, error) {
	reference := plumbing.NewBranchReferenceName(name)
	if err := reference.Validate(); err != nil {
		return "", fmt.Errorf("invalid branch name %q: %v", name, err)
	}
	return reference, nil
}

// git_tag_reference is the tag-namespace counterpart of git_branch_reference.
func git_tag_reference(name string) (plumbing.ReferenceName, error) {
	reference := plumbing.NewTagReferenceName(name)
	if err := reference.Validate(); err != nil {
		return "", fmt.Errorf("invalid tag name %q: %v", name, err)
	}
	return reference, nil
}

// git_update_branch points branch at hash, but only while the branch still
// holds expected. A merge does tree work after resolving its target, so an
// unconditional SetReference would silently discard a push that landed
// meanwhile; CheckAndSetReference returns storage.ErrReferenceHasChanged
// instead.
func git_update_branch(repo *git.Repository, branch string, expected plumbing.Hash, hash plumbing.Hash) error {
	name, err := git_branch_reference(branch)
	if err != nil {
		return err
	}
	err = repo.Storer.CheckAndSetReference(
		plumbing.NewHashReference(name, hash),
		plumbing.NewHashReference(name, expected),
	)
	if errors.Is(err, storage.ErrReferenceHasChanged) {
		return fmt.Errorf("branch %q changed while the merge was in progress - retry against its new tip", branch)
	}
	return err
}

// Resolve a reference string to a commit hash
func git_resolve_ref(repo *git.Repository, ref string) (*plumbing.Hash, error) {
	if ref == "" || ref == "HEAD" {
		head, err := repo.Head()
		if err == nil {
			hash := head.Hash()
			return &hash, nil
		}
		// HEAD might point to a non-existent branch (e.g., master when main was pushed)
		// Try common default branch names
		for _, branch := range []string{"main", "master"} {
			branch_ref, err := repo.Reference(plumbing.NewBranchReferenceName(branch), true)
			if err == nil {
				hash := branch_ref.Hash()
				return &hash, nil
			}
		}
		return nil, err
	}

	// Try as a branch. An invalid name is skipped rather than fatal: the ref
	// may still be a raw SHA, handled below.
	if name, err := git_branch_reference(ref); err == nil {
		if branch_ref, err := repo.Reference(name, true); err == nil {
			hash := branch_ref.Hash()
			return &hash, nil
		}
	}

	// Try as a tag, likewise skipped when the name is not a valid ref.
	if name, err := git_tag_reference(ref); err == nil {
		if tag_ref, err := repo.Reference(name, true); err == nil {
			// For annotated tags, dereference to get the commit hash
			tag_obj, err := repo.TagObject(tag_ref.Hash())
			if err == nil {
				// Annotated tag - get the commit it points to
				commit, err := tag_obj.Commit()
				if err == nil {
					hash := commit.Hash
					return &hash, nil
				}
			}
			// Lightweight tag or failed to dereference - use tag hash directly
			hash := tag_ref.Hash()
			return &hash, nil
		}
	}

	// Try as a commit hash
	if len(ref) >= 4 {
		hash := plumbing.NewHash(ref)
		if !hash.IsZero() {
			return &hash, nil
		}
	}

	return nil, fmt.Errorf("cannot resolve ref %q", ref)
}

// mochi.git.init(entity) -> bool: Initialize a bare git repository
//
// Deliberately ungated: the entity's access grants are written after this runs,
// so a git_can_write here would consult an ACL that does not exist yet.
// Repositories are created under the calling app's own directory.
func api_git_init(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	err := git_init(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to initialize repository: %v", err)
	}

	return sl.True, nil
}

// mochi.git.delete(entity) -> bool: Delete a git repository
func api_git_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_write(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository write required to delete a repository")
	}

	err := git_delete(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to delete repository: %v", err)
	}

	return sl.True, nil
}

// mochi.git.path(entity) -> string: Get the filesystem path to a repository
func api_git_path(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to read a repository path")
	}

	return sl.String(git_repo_path(owner, app, entity)), nil
}

// mochi.git.size(entity) -> int: Get repository size in bytes
func api_git_size(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to read the repository size")
	}

	size, err := git_size(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to get size: %v", err)
	}

	return sl.MakeInt64(size), nil
}

// mochi.git.refs(entity) -> list: List all refs (branches and tags)
func api_git_refs(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to list refs")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	var refs []map[string]any
	iter, err := repo.References()
	if err != nil {
		return sl_error(fn, "failed to list refs: %v", err)
	}

	err = iter.ForEach(func(ref *plumbing.Reference) error {
		name := ref.Name().String()
		reference_type := "unknown"
		short_name := name

		if ref.Name().IsBranch() {
			reference_type = "branch"
			short_name = ref.Name().Short()
		} else if ref.Name().IsTag() {
			reference_type = "tag"
			short_name = ref.Name().Short()
		} else if ref.Name().IsRemote() {
			reference_type = "remote"
			short_name = ref.Name().Short()
		}

		refs = append(refs, map[string]any{
			"name": short_name,
			"full": name,
			"type": reference_type,
			"sha":  ref.Hash().String(),
		})
		return nil
	})

	if err != nil {
		return sl_error(fn, "failed to iterate refs: %v", err)
	}

	return sl_encode(refs), nil
}

// mochi.git.branches(entity) -> list: List branches
func api_git_branches(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to list branches")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	var branches []map[string]any
	iter, err := repo.Branches()
	if err != nil {
		return sl_error(fn, "failed to list branches: %v", err)
	}

	err = iter.ForEach(func(ref *plumbing.Reference) error {
		branches = append(branches, map[string]any{
			"name": ref.Name().Short(),
			"sha":  ref.Hash().String(),
		})
		return nil
	})

	if err != nil {
		return sl_error(fn, "failed to iterate branches: %v", err)
	}

	return sl_encode(branches), nil
}

// mochi.git.tags(entity) -> list: List tags
func api_git_tags(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to list tags")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	var tags []map[string]any
	iter, err := repo.Tags()
	if err != nil {
		return sl_error(fn, "failed to list tags: %v", err)
	}

	err = iter.ForEach(func(ref *plumbing.Reference) error {
		tag := map[string]any{
			"name": ref.Name().Short(),
			"sha":  ref.Hash().String(),
		}

		// Try to get annotated tag info
		tag_obj, err := repo.TagObject(ref.Hash())
		if err == nil {
			tag["message"] = strings.TrimSpace(tag_obj.Message)
			tag["tagger"] = tag_obj.Tagger.Name
			tag["date"] = tag_obj.Tagger.When.Unix()
		}

		tags = append(tags, tag)
		return nil
	})

	if err != nil {
		return sl_error(fn, "failed to iterate tags: %v", err)
	}

	return sl_encode(tags), nil
}

// mochi.git.branch.create(entity, name, ref) -> bool: Create a new branch
func api_git_branch_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity: string>, <name: string>, <ref: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	name, ok := sl.AsString(args[1])
	if !ok || name == "" {
		return sl_error(fn, "invalid branch name")
	}

	ref, ok := sl.AsString(args[2])
	if !ok {
		return sl_error(fn, "invalid ref")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_write(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository write required to create a branch")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	hash, err := git_resolve_ref(repo, ref)
	if err != nil {
		return sl_error(fn, "failed to resolve ref: %v", err)
	}

	branch_ref, err := git_branch_reference(name)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	new_reference := plumbing.NewHashReference(branch_ref, *hash)
	err = repo.Storer.SetReference(new_reference)
	if err != nil {
		return sl_error(fn, "failed to create branch: %v", err)
	}

	return sl.True, nil
}

// mochi.git.branch.delete(entity, name) -> bool: Delete a branch
func api_git_branch_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <entity: string>, <name: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	name, ok := sl.AsString(args[1])
	if !ok || name == "" {
		return sl_error(fn, "invalid branch name")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_write(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository write required to delete a branch")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	branch_ref, err := git_branch_reference(name)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	err = repo.Storer.RemoveReference(branch_ref)
	if err != nil {
		return sl_error(fn, "failed to delete branch: %v", err)
	}

	return sl.True, nil
}

// mochi.git.branch.default.get(entity) -> string: Get default branch name
func api_git_branch_default_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to read the default branch")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	head, err := repo.Head()
	if err != nil {
		// Empty repo, return "main" as default
		return sl.String("main"), nil
	}

	return sl.String(head.Name().Short()), nil
}

// mochi.git.branch.default.set(entity, name) -> bool: Set default branch
func api_git_branch_default_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <entity: string>, <name: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	name, ok := sl.AsString(args[1])
	if !ok || name == "" {
		return sl_error(fn, "invalid branch name")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_write(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository write required to set the default branch")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	// Verify the branch exists
	branch_name, err := git_branch_reference(name)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	branch_ref, err := repo.Reference(branch_name, true)
	if err != nil {
		return sl_error(fn, "branch %q does not exist", name)
	}

	// Set HEAD to point to the branch
	head_reference := plumbing.NewSymbolicReference(plumbing.HEAD, branch_ref.Name())
	err = repo.Storer.SetReference(head_reference)
	if err != nil {
		return sl_error(fn, "failed to set default branch: %v", err)
	}

	return sl.True, nil
}

// mochi.git.commit.list(entity, ref, limit, offset) -> list: List commits
func api_git_commit_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 4 {
		return sl_error(fn, "syntax: <entity: string>, [ref: string], [limit: int], [offset: int]")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	ref := "HEAD"
	if len(args) > 1 && args[1] != sl.None {
		ref, _ = sl.AsString(args[1])
	}

	limit := 50
	if len(args) > 2 && args[2] != sl.None {
		limit, _ = sl.AsInt32(args[2])
	}

	offset := 0
	if len(args) > 3 && args[3] != sl.None {
		offset, _ = sl.AsInt32(args[3])
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to list commits")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	hash, err := git_resolve_ref(repo, ref)
	if err != nil {
		return sl.None, nil // ref not found
	}

	iter, err := repo.Log(&git.LogOptions{From: *hash})
	if err != nil {
		return sl.None, nil // log not found
	}

	var commits []map[string]any
	count := 0
	err = iter.ForEach(func(c *object.Commit) error {
		if count < offset {
			count++
			return nil
		}
		if len(commits) >= limit {
			return io.EOF
		}

		var parents []string
		for _, p := range c.ParentHashes {
			parents = append(parents, p.String())
		}

		commits = append(commits, map[string]any{
			"sha":     c.Hash.String(),
			"message": strings.TrimSpace(c.Message),
			"author":  c.Author.Name,
			"email":   c.Author.Email,
			"date":    c.Author.When.Unix(),
			"parents": parents,
		})
		count++
		return nil
	})

	if err != nil && err != io.EOF {
		return sl_error(fn, "failed to iterate commits: %v", err)
	}

	return sl_encode(commits), nil
}

// mochi.git.commit.get(entity, sha) -> dict: Get a single commit
func api_git_commit_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <entity: string>, <sha: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	sha, ok := sl.AsString(args[1])
	if !ok || sha == "" {
		return sl_error(fn, "invalid sha")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to read a commit")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	hash := plumbing.NewHash(sha)
	commit, err := repo.CommitObject(hash)
	if err != nil {
		return sl.None, nil
	}

	var parents []string
	for _, p := range commit.ParentHashes {
		parents = append(parents, p.String())
	}

	return sl_encode(map[string]any{
		"sha":       commit.Hash.String(),
		"message":   strings.TrimSpace(commit.Message),
		"author":    commit.Author.Name,
		"email":     commit.Author.Email,
		"date":      commit.Author.When.Unix(),
		"committer": commit.Committer.Name,
		"parents":   parents,
	}), nil
}

// mochi.git.commit.log(entity, ref, path, limit) -> list: Commits affecting a path
func api_git_commit_log(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 || len(args) > 4 {
		return sl_error(fn, "syntax: <entity: string>, <ref: string>, <path: string>, [limit: int]")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	ref, ok := sl.AsString(args[1])
	if !ok {
		ref = "HEAD"
	}

	path, ok := sl.AsString(args[2])
	if !ok {
		return sl_error(fn, "invalid path")
	}

	limit := 50
	if len(args) > 3 && args[3] != sl.None {
		limit, _ = sl.AsInt32(args[3])
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to read the commit log")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	hash, err := git_resolve_ref(repo, ref)
	if err != nil {
		return sl.None, nil // ref not found
	}

	iter, err := repo.Log(&git.LogOptions{
		From: *hash,
		PathFilter: func(p string) bool {
			return strings.HasPrefix(p, path) || p == path
		},
	})
	if err != nil {
		return sl.None, nil // log not found
	}

	var commits []map[string]any
	err = iter.ForEach(func(c *object.Commit) error {
		if len(commits) >= limit {
			return io.EOF
		}
		commits = append(commits, map[string]any{
			"sha":     c.Hash.String(),
			"message": strings.TrimSpace(c.Message),
			"author":  c.Author.Name,
			"date":    c.Author.When.Unix(),
		})
		return nil
	})

	if err != nil && err != io.EOF {
		return sl_error(fn, "failed to iterate commits: %v", err)
	}

	return sl_encode(commits), nil
}

// mochi.git.commit.between(entity, base, head) -> list: Commits between refs
func api_git_commit_between(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity: string>, <base: string>, <head: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	base, ok := sl.AsString(args[1])
	if !ok || base == "" {
		return sl_error(fn, "invalid base")
	}

	head, ok := sl.AsString(args[2])
	if !ok || head == "" {
		return sl_error(fn, "invalid head")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to list commits")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	base_hash, err := git_resolve_ref(repo, base)
	if err != nil {
		return sl.None, nil // base ref not found
	}

	head_hash, err := git_resolve_ref(repo, head)
	if err != nil {
		return sl.None, nil // head ref not found
	}

	// Get commits reachable from head
	head_commit, err := repo.CommitObject(*head_hash)
	if err != nil {
		return sl.None, nil // head commit not found
	}

	base_commit, err := repo.CommitObject(*base_hash)
	if err != nil {
		return sl.None, nil // base commit not found
	}

	// Find commits in head not in base
	base_ancestors := make(map[plumbing.Hash]bool)
	base_iter := object.NewCommitIterCTime(base_commit, nil, nil)
	base_iter.ForEach(func(c *object.Commit) error {
		base_ancestors[c.Hash] = true
		return nil
	})

	var commits []map[string]any
	head_iter := object.NewCommitIterCTime(head_commit, nil, nil)
	head_iter.ForEach(func(c *object.Commit) error {
		if !base_ancestors[c.Hash] {
			commits = append(commits, map[string]any{
				"sha":     c.Hash.String(),
				"message": strings.TrimSpace(c.Message),
				"author":  c.Author.Name,
				"date":    c.Author.When.Unix(),
			})
		}
		return nil
	})

	// Reverse to get chronological order
	for i, j := 0, len(commits)-1; i < j; i, j = i+1, j-1 {
		commits[i], commits[j] = commits[j], commits[i]
	}

	return sl_encode(commits), nil
}

// mochi.git.tree(entity, ref, path) -> list: List directory contents
func api_git_tree(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 3 {
		return sl_error(fn, "syntax: <entity: string>, [ref: string], [path: string]")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	ref := "HEAD"
	if len(args) > 1 && args[1] != sl.None {
		ref, _ = sl.AsString(args[1])
	}

	path := ""
	if len(args) > 2 && args[2] != sl.None {
		path, _ = sl.AsString(args[2])
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to browse the tree")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	hash, err := git_resolve_ref(repo, ref)
	if err != nil {
		return sl.None, nil // ref not found
	}

	commit, err := repo.CommitObject(*hash)
	if err != nil {
		return sl.None, nil // commit not found
	}

	tree, err := commit.Tree()
	if err != nil {
		return sl.None, nil // tree not found
	}

	// Navigate to path if specified
	if path != "" {
		tree, err = tree.Tree(path)
		if err != nil {
			return sl.None, nil // path not found
		}
	}

	var entries []map[string]any
	for _, entry := range tree.Entries {
		entry_type := "file"
		if entry.Mode == filemode.Dir {
			entry_type = "dir"
		} else if entry.Mode == filemode.Submodule {
			entry_type = "submodule"
		} else if entry.Mode == filemode.Symlink {
			entry_type = "symlink"
		}

		e := map[string]any{
			"name": entry.Name,
			"type": entry_type,
			"sha":  entry.Hash.String(),
			"mode": fmt.Sprintf("%o", entry.Mode),
		}

		// Get size for files
		if entry_type == "file" {
			blob, err := repo.BlobObject(entry.Hash)
			if err == nil {
				e["size"] = blob.Size
			}
		}

		entries = append(entries, e)
	}

	// Deliberately unsorted: the consumer sorts user-facing strings (the web
	// re-sorts with naturalCompare). Entries arrive in git's own tree order.

	return sl_encode(entries), nil
}

// mochi.git.blob.content(entity, ref, path) -> string: Get file contents
func api_git_blob_content(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity: string>, <ref: string>, <path: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	ref, ok := sl.AsString(args[1])
	if !ok {
		ref = "HEAD"
	}

	path, ok := sl.AsString(args[2])
	if !ok || path == "" {
		return sl_error(fn, "invalid path")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to read file content")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	hash, err := git_resolve_ref(repo, ref)
	if err != nil {
		return sl.None, nil // ref not found
	}

	commit, err := repo.CommitObject(*hash)
	if err != nil {
		return sl.None, nil // commit not found
	}

	file, err := commit.File(path)
	if err != nil {
		return sl.None, nil // file not found
	}

	content, err := file.Contents()
	if err != nil {
		return sl_error(fn, "failed to read file: %v", err)
	}

	return sl.String(content), nil
}

// mochi.git.blob.get(entity, ref, path) -> dict: Get file metadata
func api_git_blob_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity: string>, <ref: string>, <path: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	ref, ok := sl.AsString(args[1])
	if !ok {
		ref = "HEAD"
	}

	path, ok := sl.AsString(args[2])
	if !ok || path == "" {
		return sl_error(fn, "invalid path")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to read a file")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	hash, err := git_resolve_ref(repo, ref)
	if err != nil {
		return sl.None, nil // ref not found
	}

	commit, err := repo.CommitObject(*hash)
	if err != nil {
		return sl.None, nil // commit not found
	}

	file, err := commit.File(path)
	if err != nil {
		return sl.None, nil // file not found
	}

	// Check if binary by looking for null bytes in first 8KB
	reader, err := file.Reader()
	if err != nil {
		return sl_error(fn, "failed to read file: %v", err)
	}
	defer reader.Close()

	buf := make([]byte, 8192)
	n, _ := reader.Read(buf)
	binary := false
	for i := 0; i < n; i++ {
		if buf[i] == 0 {
			binary = true
			break
		}
	}

	return sl_encode(map[string]any{
		"sha":    file.Hash.String(),
		"size":   file.Size,
		"binary": binary,
	}), nil
}

// mochi.git.diff(entity, base, head) -> string: Get unified diff
func api_git_diff(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity: string>, <base: string>, <head: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	base, ok := sl.AsString(args[1])
	if !ok || base == "" {
		return sl_error(fn, "invalid base")
	}

	head, ok := sl.AsString(args[2])
	if !ok || head == "" {
		return sl_error(fn, "invalid head")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to diff")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	base_hash, err := git_resolve_ref(repo, base)
	if err != nil {
		return sl_error(fn, "failed to resolve base: %v", err)
	}

	head_hash, err := git_resolve_ref(repo, head)
	if err != nil {
		return sl_error(fn, "failed to resolve head: %v", err)
	}

	base_commit, err := repo.CommitObject(*base_hash)
	if err != nil {
		return sl_error(fn, "failed to get base commit: %v", err)
	}

	head_commit, err := repo.CommitObject(*head_hash)
	if err != nil {
		return sl_error(fn, "failed to get head commit: %v", err)
	}

	base_tree, err := base_commit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get base tree: %v", err)
	}

	head_tree, err := head_commit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get head tree: %v", err)
	}

	changes, err := base_tree.Diff(head_tree)
	if err != nil {
		return sl_error(fn, "failed to compute diff: %v", err)
	}

	patch, err := changes.Patch()
	if err != nil {
		return sl_error(fn, "failed to generate patch: %v", err)
	}

	return sl.String(patch.String()), nil
}

// mochi.git.diff.stats(entity, base, head) -> dict: Get diff statistics
func api_git_diff_stats(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity: string>, <base: string>, <head: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	base, ok := sl.AsString(args[1])
	if !ok || base == "" {
		return sl_error(fn, "invalid base")
	}

	head, ok := sl.AsString(args[2])
	if !ok || head == "" {
		return sl_error(fn, "invalid head")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to diff")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	base_hash, err := git_resolve_ref(repo, base)
	if err != nil {
		return sl_error(fn, "failed to resolve base: %v", err)
	}

	head_hash, err := git_resolve_ref(repo, head)
	if err != nil {
		return sl_error(fn, "failed to resolve head: %v", err)
	}

	base_commit, err := repo.CommitObject(*base_hash)
	if err != nil {
		return sl_error(fn, "failed to get base commit: %v", err)
	}

	head_commit, err := repo.CommitObject(*head_hash)
	if err != nil {
		return sl_error(fn, "failed to get head commit: %v", err)
	}

	base_tree, err := base_commit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get base tree: %v", err)
	}

	head_tree, err := head_commit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get head tree: %v", err)
	}

	changes, err := base_tree.Diff(head_tree)
	if err != nil {
		return sl_error(fn, "failed to compute diff: %v", err)
	}

	patch, err := changes.Patch()
	if err != nil {
		return sl_error(fn, "failed to generate patch: %v", err)
	}

	stats := patch.Stats()
	var files []map[string]any
	additions := 0
	deletions := 0

	for _, stat := range stats {
		files = append(files, map[string]any{
			"name":      stat.Name,
			"additions": stat.Addition,
			"deletions": stat.Deletion,
		})
		additions += stat.Addition
		deletions += stat.Deletion
	}

	return sl_encode(map[string]any{
		"files":     files,
		"additions": additions,
		"deletions": deletions,
	}), nil
}

// mochi.git.merge.base(entity, ref1, ref2) -> string: Find common ancestor
func api_git_merge_base(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity: string>, <ref1: string>, <ref2: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	ref1, ok := sl.AsString(args[1])
	if !ok || ref1 == "" {
		return sl_error(fn, "invalid ref1")
	}

	ref2, ok := sl.AsString(args[2])
	if !ok || ref2 == "" {
		return sl_error(fn, "invalid ref2")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to find the merge base")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	hash1, err := git_resolve_ref(repo, ref1)
	if err != nil {
		return sl_error(fn, "failed to resolve ref1: %v", err)
	}

	hash2, err := git_resolve_ref(repo, ref2)
	if err != nil {
		return sl_error(fn, "failed to resolve ref2: %v", err)
	}

	commit1, err := repo.CommitObject(*hash1)
	if err != nil {
		return sl_error(fn, "failed to get commit1: %v", err)
	}

	commit2, err := repo.CommitObject(*hash2)
	if err != nil {
		return sl_error(fn, "failed to get commit2: %v", err)
	}

	bases, err := commit1.MergeBase(commit2)
	if err != nil {
		return sl_error(fn, "failed to find merge base: %v", err)
	}

	if len(bases) == 0 {
		return sl.None, nil
	}

	return sl.String(bases[0].Hash.String()), nil
}

// mochi.git.merge.check(entity, source, target) -> dict: Check if merge is possible
func api_git_merge_check(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity: string>, <source: string>, <target: string>")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	source, ok := sl.AsString(args[1])
	if !ok || source == "" {
		return sl_error(fn, "invalid source")
	}

	target, ok := sl.AsString(args[2])
	if !ok || target == "" {
		return sl_error(fn, "invalid target")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to check merge")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	source_hash, err := git_resolve_ref(repo, source)
	if err != nil {
		return sl_error(fn, "failed to resolve source: %v", err)
	}

	target_hash, err := git_resolve_ref(repo, target)
	if err != nil {
		return sl_error(fn, "failed to resolve target: %v", err)
	}

	source_commit, err := repo.CommitObject(*source_hash)
	if err != nil {
		return sl_error(fn, "failed to get source commit: %v", err)
	}

	target_commit, err := repo.CommitObject(*target_hash)
	if err != nil {
		return sl_error(fn, "failed to get target commit: %v", err)
	}

	// Find merge base
	bases, err := source_commit.MergeBase(target_commit)
	if err != nil || len(bases) == 0 {
		return sl_encode(map[string]any{
			"can_merge": false,
			"conflicts": []string{},
			"error":     "no common ancestor",
		}), nil
	}

	// Count ahead/behind
	ahead := 0
	behind := 0
	base_hash := bases[0].Hash
	source_iter := object.NewCommitIterCTime(source_commit, nil, nil)
	source_iter.ForEach(func(c *object.Commit) error {
		if c.Hash == base_hash {
			return io.EOF
		}
		ahead++
		return nil
	})
	target_iter := object.NewCommitIterCTime(target_commit, nil, nil)
	target_iter.ForEach(func(c *object.Commit) error {
		if c.Hash == base_hash {
			return io.EOF
		}
		behind++
		return nil
	})

	// Check for conflicts by comparing trees
	source_tree, err := source_commit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get source tree: %v", err)
	}

	target_tree, err := target_commit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get target tree: %v", err)
	}

	base_tree, err := bases[0].Tree()
	if err != nil {
		return sl_error(fn, "failed to get base tree: %v", err)
	}

	// Get changes from base to source and base to target
	source_changes, _ := base_tree.Diff(source_tree)
	target_changes, _ := base_tree.Diff(target_tree)

	// Check for overlapping changes (potential conflicts)
	source_files := make(map[string]bool)
	for _, change := range source_changes {
		from, to, _ := change.Files()
		if from != nil {
			source_files[from.Name] = true
		}
		if to != nil {
			source_files[to.Name] = true
		}
	}

	var conflicts []string
	for _, change := range target_changes {
		from, to, _ := change.Files()
		var name string
		if from != nil {
			name = from.Name
		} else if to != nil {
			name = to.Name
		}
		if name != "" && source_files[name] {
			conflicts = append(conflicts, name)
		}
	}

	return sl_encode(map[string]any{
		"can_merge": len(conflicts) == 0,
		"conflicts": conflicts,
		"base":      bases[0].Hash.String(),
		"ahead":     ahead,
		"behind":    behind,
	}), nil
}

// mochi.git.merge.perform(entity, source, target, message, author_name, author_email) -> dict: Perform a merge
func api_git_merge_perform(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 6 || len(args) > 7 {
		return sl_error(fn, "syntax: <entity: string>, <source: string>, <target: string>, <message: string>, <author_name: string>, <author_email: string>, [method: string]")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	source, ok := sl.AsString(args[1])
	if !ok || source == "" {
		return sl_error(fn, "invalid source")
	}

	target, ok := sl.AsString(args[2])
	if !ok || target == "" {
		return sl_error(fn, "invalid target")
	}

	message, ok := sl.AsString(args[3])
	if !ok || message == "" {
		message = "Merge branch"
	}

	author_name, ok := sl.AsString(args[4])
	if !ok || author_name == "" {
		author_name = "Mochi"
	}

	author_email, _ := sl.AsString(args[5])

	method := "merge"
	if len(args) == 7 {
		m, ok := sl.AsString(args[6])
		if ok && m != "" {
			method = m
		}
	}
	if method != "merge" && method != "squash" && method != "rebase" {
		return sl_error(fn, "invalid method: must be 'merge', 'squash', or 'rebase'")
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_write(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository write required to merge")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	source_hash, err := git_resolve_ref(repo, source)
	if err != nil {
		return sl_error(fn, "failed to resolve source: %v", err)
	}

	target_hash, err := git_resolve_ref(repo, target)
	if err != nil {
		return sl_error(fn, "failed to resolve target: %v", err)
	}

	source_commit, err := repo.CommitObject(*source_hash)
	if err != nil {
		return sl_error(fn, "failed to get source commit: %v", err)
	}

	target_commit, err := repo.CommitObject(*target_hash)
	if err != nil {
		return sl_error(fn, "failed to get target commit: %v", err)
	}

	// Find merge base
	bases, err := source_commit.MergeBase(target_commit)
	if err != nil || len(bases) == 0 {
		return sl_error(fn, "no common ancestor between source and target")
	}
	base_commit := bases[0]

	// Fast-forward: if target is the merge base, just update the ref (all methods)
	if base_commit.Hash == *target_hash {
		if method == "squash" {
			// Squash: create a single commit with source tree on top of target
			return git_merge_squash(repo, source_commit, target_hash, target, message, author_name, author_email)
		}
		// Merge and rebase: fast-forward
		if err := git_update_branch(repo, target, *target_hash, *source_hash); err != nil {
			return sl_error(fn, "failed to fast-forward: %v", err)
		}
		return sl_encode(map[string]any{
			"success":      true,
			"commit":       source_hash.String(),
			"fast_forward": true,
		}), nil
	}

	// Already up to date: source is an ancestor of target
	if base_commit.Hash == *source_hash {
		return sl_encode(map[string]any{
			"success":      true,
			"commit":       target_hash.String(),
			"fast_forward": false,
			"up_to_date":   true,
		}), nil
	}

	// Three-way merge required
	base_tree, err := base_commit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get base tree: %v", err)
	}

	source_tree, err := source_commit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get source tree: %v", err)
	}

	target_tree, err := target_commit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get target tree: %v", err)
	}

	// Compute diffs from base to each branch
	source_changes, err := base_tree.Diff(source_tree)
	if err != nil {
		return sl_error(fn, "failed to diff base to source: %v", err)
	}

	target_changes, err := base_tree.Diff(target_tree)
	if err != nil {
		return sl_error(fn, "failed to diff base to target: %v", err)
	}

	// Build map of source-side changes
	type file_change struct {
		action string // "add", "modify", "delete"
		hash   plumbing.Hash
		mode   filemode.FileMode
	}
	source_file_changes := make(map[string]*file_change)
	for _, change := range source_changes {
		from, to, _ := change.Files()
		if from != nil && to == nil {
			source_file_changes[from.Name] = &file_change{action: "delete"}
		} else if from == nil && to != nil {
			source_file_changes[to.Name] = &file_change{action: "add", hash: to.Hash, mode: to.Mode}
		} else if from != nil && to != nil {
			if from.Name != to.Name {
				source_file_changes[from.Name] = &file_change{action: "delete"}
				source_file_changes[to.Name] = &file_change{action: "add", hash: to.Hash, mode: to.Mode}
			} else {
				source_file_changes[to.Name] = &file_change{action: "modify", hash: to.Hash, mode: to.Mode}
			}
		}
	}

	// Build map of target-side changes for conflict detection
	target_changed_files := make(map[string]bool)
	for _, change := range target_changes {
		from, to, _ := change.Files()
		if from != nil {
			target_changed_files[from.Name] = true
		}
		if to != nil {
			target_changed_files[to.Name] = true
		}
	}

	// Check for conflicts (same file changed on both sides)
	var conflicts []string
	for name := range source_file_changes {
		if target_changed_files[name] {
			conflicts = append(conflicts, name)
		}
	}
	if len(conflicts) > 0 {
		sort.Strings(conflicts)
		return sl_encode(map[string]any{
			"success":   false,
			"conflicts": conflicts,
		}), nil
	}

	// Build merged tree: start from target tree, apply source changes
	merged_entries := make(map[string]object.TreeEntry)
	git_tree_flatten(target_tree, "", merged_entries)

	// Apply source changes
	for name, change := range source_file_changes {
		switch change.action {
		case "delete":
			delete(merged_entries, name)
		case "add", "modify":
			merged_entries[name] = object.TreeEntry{
				Name: name,
				Mode: change.mode,
				Hash: change.hash,
			}
		}
	}

	// Build the merged tree object hierarchy and store it
	merged_tree_hash, err := git_build_tree(repo, merged_entries)
	if err != nil {
		return sl_error(fn, "failed to build merged tree: %v", err)
	}

	now := time.Now()
	author := object.Signature{
		Name:  author_name,
		Email: author_email,
		When:  now,
	}

	switch method {
	case "squash":
		// Create a single commit with merged tree, only target as parent
		squash_commit := &object.Commit{
			Author:       author,
			Committer:    author,
			Message:      message,
			TreeHash:     merged_tree_hash,
			ParentHashes: []plumbing.Hash{*target_hash},
		}
		obj := repo.Storer.NewEncodedObject()
		obj.SetType(plumbing.CommitObject)
		err = squash_commit.Encode(obj)
		if err != nil {
			return sl_error(fn, "failed to encode squash commit: %v", err)
		}
		commit_hash, err := repo.Storer.SetEncodedObject(obj)
		if err != nil {
			return sl_error(fn, "failed to store squash commit: %v", err)
		}
		if err := git_update_branch(repo, target, *target_hash, commit_hash); err != nil {
			return sl_error(fn, "failed to update target branch: %v", err)
		}
		return sl_encode(map[string]any{
			"success":      true,
			"commit":       commit_hash.String(),
			"fast_forward": false,
		}), nil

	case "rebase":
		// Replay source commits from merge base to HEAD on top of target
		return git_merge_rebase(repo, source_commit, &base_commit.Hash, target_hash, target, author_name, author_email)

	default:
		// Standard merge commit with two parents
		merge_commit := &object.Commit{
			Author:       author,
			Committer:    author,
			Message:      message,
			TreeHash:     merged_tree_hash,
			ParentHashes: []plumbing.Hash{*target_hash, *source_hash},
		}
		obj := repo.Storer.NewEncodedObject()
		obj.SetType(plumbing.CommitObject)
		err = merge_commit.Encode(obj)
		if err != nil {
			return sl_error(fn, "failed to encode merge commit: %v", err)
		}
		commit_hash, err := repo.Storer.SetEncodedObject(obj)
		if err != nil {
			return sl_error(fn, "failed to store merge commit: %v", err)
		}
		if err := git_update_branch(repo, target, *target_hash, commit_hash); err != nil {
			return sl_error(fn, "failed to update target branch: %v", err)
		}
		return sl_encode(map[string]any{
			"success":      true,
			"commit":       commit_hash.String(),
			"fast_forward": false,
		}), nil
	}
}

// git_merge_squash creates a single squash commit with source tree on top of target (for fast-forward case)
func git_merge_squash(repo *git.Repository, source_commit *object.Commit, target_hash *plumbing.Hash, target, message, author_name, author_email string) (sl.Value, error) {
	now := time.Now()
	author := object.Signature{Name: author_name, Email: author_email, When: now}
	squash := &object.Commit{
		Author:       author,
		Committer:    author,
		Message:      message,
		TreeHash:     source_commit.TreeHash,
		ParentHashes: []plumbing.Hash{*target_hash},
	}
	obj := repo.Storer.NewEncodedObject()
	obj.SetType(plumbing.CommitObject)
	if err := squash.Encode(obj); err != nil {
		return sl_error(nil, "failed to encode squash commit: %v", err)
	}
	commit_hash, err := repo.Storer.SetEncodedObject(obj)
	if err != nil {
		return sl_error(nil, "failed to store squash commit: %v", err)
	}
	if err := git_update_branch(repo, target, *target_hash, commit_hash); err != nil {
		return sl_error(nil, "failed to update target branch: %v", err)
	}
	return sl_encode(map[string]any{
		"success":      true,
		"commit":       commit_hash.String(),
		"fast_forward": false,
	}), nil
}

// git_merge_rebase replays source commits from merge base to HEAD on top of target
func git_merge_rebase(repo *git.Repository, source_commit *object.Commit, base_hash, target_hash *plumbing.Hash, target, author_name, author_email string) (sl.Value, error) {
	// Collect commits from source back to merge base
	var commits []*object.Commit
	current := source_commit
	for current.Hash != *base_hash {
		commits = append(commits, current)
		if len(current.ParentHashes) == 0 {
			break
		}
		parent, err := repo.CommitObject(current.ParentHashes[0])
		if err != nil {
			return sl_error(nil, "failed to walk commit history: %v", err)
		}
		current = parent
	}

	// Reverse to replay in chronological order
	for i, j := 0, len(commits)-1; i < j; i, j = i+1, j-1 {
		commits[i], commits[j] = commits[j], commits[i]
	}

	// Replay each commit on top of target
	current_parent := *target_hash
	now := time.Now()
	for _, c := range commits {
		// Get this commit's tree and its parent's tree
		commit_tree, err := c.Tree()
		if err != nil {
			return sl_error(nil, "failed to get commit tree: %v", err)
		}

		var parent_tree *object.Tree
		if len(c.ParentHashes) > 0 {
			parent_commit, err := repo.CommitObject(c.ParentHashes[0])
			if err != nil {
				return sl_error(nil, "failed to get parent commit: %v", err)
			}
			parent_tree, err = parent_commit.Tree()
			if err != nil {
				return sl_error(nil, "failed to get parent tree: %v", err)
			}
		}

		// Get the current base tree we're replaying onto
		base_commit, err := repo.CommitObject(current_parent)
		if err != nil {
			return sl_error(nil, "failed to get base commit for replay: %v", err)
		}
		base_tree, err := base_commit.Tree()
		if err != nil {
			return sl_error(nil, "failed to get base tree for replay: %v", err)
		}

		// Diff the commit against its parent to get its changes
		var changes object.Changes
		if parent_tree != nil {
			changes, err = parent_tree.Diff(commit_tree)
		} else {
			changes, err = (&object.Tree{}).Diff(commit_tree)
		}
		if err != nil {
			return sl_error(nil, "failed to diff commit: %v", err)
		}

		// Apply changes to base tree
		entries := make(map[string]object.TreeEntry)
		git_tree_flatten(base_tree, "", entries)

		for _, change := range changes {
			from, to, _ := change.Files()
			if from != nil && to == nil {
				delete(entries, from.Name)
			} else if from == nil && to != nil {
				entries[to.Name] = object.TreeEntry{Name: to.Name, Mode: to.Mode, Hash: to.Hash}
			} else if from != nil && to != nil {
				if from.Name != to.Name {
					delete(entries, from.Name)
				}
				entries[to.Name] = object.TreeEntry{Name: to.Name, Mode: to.Mode, Hash: to.Hash}
			}
		}

		new_tree_hash, err := git_build_tree(repo, entries)
		if err != nil {
			return sl_error(nil, "failed to build rebased tree: %v", err)
		}

		// Create new commit preserving original author and message
		committer := object.Signature{Name: author_name, Email: author_email, When: now}
		rebased := &object.Commit{
			Author:       c.Author,
			Committer:    committer,
			Message:      c.Message,
			TreeHash:     new_tree_hash,
			ParentHashes: []plumbing.Hash{current_parent},
		}
		obj := repo.Storer.NewEncodedObject()
		obj.SetType(plumbing.CommitObject)
		if err := rebased.Encode(obj); err != nil {
			return sl_error(nil, "failed to encode rebased commit: %v", err)
		}
		hash, err := repo.Storer.SetEncodedObject(obj)
		if err != nil {
			return sl_error(nil, "failed to store rebased commit: %v", err)
		}
		current_parent = hash
	}

	// Update target branch ref
	if err := git_update_branch(repo, target, *target_hash, current_parent); err != nil {
		return sl_error(nil, "failed to update target branch: %v", err)
	}

	return sl_encode(map[string]any{
		"success":      true,
		"commit":       current_parent.String(),
		"fast_forward": false,
	}), nil
}

// git_tree_flatten collects all entries from a tree into a flat map keyed by path
func git_tree_flatten(tree *object.Tree, prefix string, entries map[string]object.TreeEntry) {
	for _, entry := range tree.Entries {
		path := entry.Name
		if prefix != "" {
			path = prefix + "/" + entry.Name
		}
		if entry.Mode == filemode.Dir {
			// Recurse into subtree
			subtree, err := tree.Tree(entry.Name)
			if err == nil {
				git_tree_flatten(subtree, path, entries)
			}
		} else {
			entries[path] = object.TreeEntry{
				Name: path,
				Mode: entry.Mode,
				Hash: entry.Hash,
			}
		}
	}
}

// git_dir_node represents a directory in a tree being built
type git_dir_node struct {
	entries  []object.TreeEntry
	children map[string]*git_dir_node
}

// git_build_tree builds a tree object hierarchy from a flat map of path→entry and stores it in the repo
func git_build_tree(repo *git.Repository, entries map[string]object.TreeEntry) (plumbing.Hash, error) {
	root := &git_dir_node{children: make(map[string]*git_dir_node)}

	// Sort paths for deterministic output
	paths := make([]string, 0, len(entries))
	for path := range entries {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	for _, path := range paths {
		entry := entries[path]
		parts := strings.Split(path, "/")
		node := root
		for i := 0; i < len(parts)-1; i++ {
			child, exists := node.children[parts[i]]
			if !exists {
				child = &git_dir_node{children: make(map[string]*git_dir_node)}
				node.children[parts[i]] = child
			}
			node = child
		}
		node.entries = append(node.entries, object.TreeEntry{
			Name: parts[len(parts)-1],
			Mode: entry.Mode,
			Hash: entry.Hash,
		})
	}

	// Recursively build tree objects from leaves up
	return git_store_tree(repo, root)
}

// git_store_tree recursively stores tree objects and returns the hash
func git_store_tree(repo *git.Repository, node *git_dir_node) (plumbing.Hash, error) {
	var all_entries []object.TreeEntry

	// Process child directories first
	child_names := make([]string, 0, len(node.children))
	for name := range node.children {
		child_names = append(child_names, name)
	}
	sort.Strings(child_names)

	for _, name := range child_names {
		child := node.children[name]
		child_hash, err := git_store_tree(repo, child)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		all_entries = append(all_entries, object.TreeEntry{
			Name: name,
			Mode: filemode.Dir,
			Hash: child_hash,
		})
	}

	// Add file entries
	all_entries = append(all_entries, node.entries...)

	// Sort entries (git requires sorted tree entries)
	sort.Slice(all_entries, func(i, j int) bool {
		// Git sorts directories with trailing slash
		ni := all_entries[i].Name
		nj := all_entries[j].Name
		if all_entries[i].Mode == filemode.Dir {
			ni += "/"
		}
		if all_entries[j].Mode == filemode.Dir {
			nj += "/"
		}
		return ni < nj
	})

	tree := &object.Tree{Entries: all_entries}
	obj := repo.Storer.NewEncodedObject()
	obj.SetType(plumbing.TreeObject)
	err := tree.Encode(obj)
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to encode tree: %v", err)
	}
	hash, err := repo.Storer.SetEncodedObject(obj)
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to store tree: %v", err)
	}
	return hash, nil
}

// mochi.git.archive(entity, ref, format, [prefix], [stream]) -> int: Stream a tree archive to the action's HTTP response (default) or to a Stream
func api_git_archive(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 || len(args) > 5 {
		return sl_error(fn, "syntax: <entity: string>, <ref: string>, <format: string>, [prefix: string], [stream: Stream]")
	}

	entity, ok := sl.AsString(args[0])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	ref, ok := sl.AsString(args[1])
	if !ok || ref == "" {
		return sl_error(fn, "invalid ref")
	}

	format, ok := sl.AsString(args[2])
	if !ok || (format != "zip" && format != "tar.gz" && format != "tar.bz2") {
		return sl_error(fn, "format must be 'zip', 'tar.gz', or 'tar.bz2'")
	}

	prefix := ""
	var stream *Stream
	for i := 3; i < len(args); i++ {
		if args[i] == sl.None {
			continue
		}
		if s, ok := args[i].(*Stream); ok {
			stream = s
		} else if str, ok := sl.AsString(args[i]); ok {
			prefix = str
		}
	}
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	owner := principal_owner(t)
	app := principal_app(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	if app == nil {
		return sl_error(fn, "no app")
	}

	if !git_can_read(t, owner, app, entity) {
		return sl_error(fn, "permission denied: repository read required to download an archive")
	}

	repo, err := git_open(owner, app, entity)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	hash, err := git_resolve_ref(repo, ref)
	if err != nil {
		return sl_error(fn, "ref not found: %v", err)
	}

	commit, err := repo.CommitObject(*hash)
	if err != nil {
		return sl_error(fn, "commit not found: %v", err)
	}

	tree, err := commit.Tree()
	if err != nil {
		return sl_error(fn, "tree not found: %v", err)
	}

	var dest io.Writer
	if stream != nil {
		defer stream.close_write()
		dest = stream.writer
	} else {
		action, ok := t.Local("action").(*Action)
		if !ok || action == nil {
			return sl_error(fn, "called from non-action and no stream provided")
		}
		starlark_serving_set(t, action.web.Writer)
		if !action.web.Writer.Written() {
			action.web.Status(http.StatusOK)
		}
		dest = action.web.Writer
	}

	mtime := commit.Author.When
	w := &git_archive_counter{w: dest}

	switch format {
	case "zip":
		err = git_archive_write_zip(w, tree, prefix, mtime)
	case "tar.gz":
		err = git_archive_write_targz(w, tree, prefix, mtime)
	case "tar.bz2":
		err = git_archive_write_tarbz2(w, tree, prefix, mtime)
	}
	if err != nil && !is_client_disconnect(err) {
		return sl_error(fn, "failed to write archive: %v", err)
	}

	return sl.MakeInt64(w.n), nil
}

// git_archive_counter wraps an io.Writer and counts bytes written
type git_archive_counter struct {
	w io.Writer
	n int64
}

func (c *git_archive_counter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// git_archive_write_zip walks the tree and writes a zip archive
func git_archive_write_zip(w io.Writer, tree *object.Tree, prefix string, mtime time.Time) error {
	zw := zip.NewWriter(w)
	defer zw.Close()

	// Always include the prefix directory entry — extracting an empty repo's
	// archive should still yield <prefix>/ rather than a pile of nothing.
	if prefix != "" {
		dir := &zip.FileHeader{
			Name:     prefix,
			Method:   zip.Store,
			Modified: mtime,
		}
		dir.SetMode(os.ModeDir | 0755)
		if _, err := zw.CreateHeader(dir); err != nil {
			return err
		}
	}

	files := tree.Files()
	defer files.Close()

	return files.ForEach(func(f *object.File) error {
		if f.Mode == filemode.Submodule {
			return nil
		}

		hdr := &zip.FileHeader{
			Name:     prefix + f.Name,
			Method:   zip.Deflate,
			Modified: mtime,
		}
		if f.Mode == filemode.Executable {
			hdr.SetMode(0755)
		} else if f.Mode == filemode.Symlink {
			hdr.SetMode(os.ModeSymlink | 0777)
		} else {
			hdr.SetMode(0644)
		}

		entry, err := zw.CreateHeader(hdr)
		if err != nil {
			return err
		}

		reader, err := f.Reader()
		if err != nil {
			return err
		}
		defer reader.Close()

		_, err = io.Copy(entry, reader)
		return err
	})
}

// git_archive_write_targz walks the tree and writes a gzipped tar archive
func git_archive_write_targz(w io.Writer, tree *object.Tree, prefix string, mtime time.Time) error {
	gz := gzip.NewWriter(w)
	defer gz.Close()
	return git_archive_write_tar(gz, tree, prefix, mtime)
}

// git_archive_write_tarbz2 walks the tree and writes a bzip2-compressed tar archive
func git_archive_write_tarbz2(w io.Writer, tree *object.Tree, prefix string, mtime time.Time) error {
	bz, err := bzip2.NewWriter(w, &bzip2.WriterConfig{Level: bzip2.DefaultCompression})
	if err != nil {
		return err
	}
	defer bz.Close()
	return git_archive_write_tar(bz, tree, prefix, mtime)
}

// git_archive_write_tar walks the tree and writes a tar archive to w
func git_archive_write_tar(w io.Writer, tree *object.Tree, prefix string, mtime time.Time) error {
	tw := tar.NewWriter(w)
	defer tw.Close()

	// Always include the prefix directory entry — extracting an empty repo's
	// archive should still yield <prefix>/ rather than a pile of nothing.
	if prefix != "" {
		if err := tw.WriteHeader(&tar.Header{
			Name:     prefix,
			Typeflag: tar.TypeDir,
			Mode:     0755,
			ModTime:  mtime,
		}); err != nil {
			return err
		}
	}

	files := tree.Files()
	defer files.Close()

	return files.ForEach(func(f *object.File) error {
		if f.Mode == filemode.Submodule {
			return nil
		}

		hdr := &tar.Header{
			Name:    prefix + f.Name,
			Size:    f.Size,
			ModTime: mtime,
		}
		switch f.Mode {
		case filemode.Executable:
			hdr.Typeflag = tar.TypeReg
			hdr.Mode = 0755
		case filemode.Symlink:
			hdr.Typeflag = tar.TypeSymlink
			hdr.Mode = 0777
			target, err := f.Contents()
			if err != nil {
				return err
			}
			hdr.Linkname = target
			hdr.Size = 0
		default:
			hdr.Typeflag = tar.TypeReg
			hdr.Mode = 0644
		}

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		if hdr.Typeflag == tar.TypeSymlink {
			return nil
		}

		reader, err := f.Reader()
		if err != nil {
			return err
		}
		defer reader.Close()

		_, err = io.Copy(tw, reader)
		return err
	})
}

// git_http_handler handles the Smart HTTP protocol for git clone/push/fetch
// Path format: /info/refs, /git-upload-pack, /git-receive-pack

func git_http_handler(c *gin.Context, a *App, owner *User, user *User, repo string, path string) bool {
	if owner == nil {
		c.String(http.StatusNotFound, "Repository not found") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	// Find repository entity by fingerprint for this owner
	// The repo parameter is the entity fingerprint extracted from the URL
	db := db_open("db/users.db")
	row, err := db.row("select id from entities where user = ? and fingerprint = ?", owner.UID, repo)
	if err != nil || row == nil {
		c.String(http.StatusNotFound, "Repository not found") // i18n-ok: git protocol, read by the client not a person
		return true
	}
	id, ok := row["id"].(string)
	if !ok || id == "" {
		c.String(http.StatusNotFound, "Repository not found") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	// Build repository path
	repo_path := git_repo_path(owner, a, id)
	if _, err := os.Stat(repo_path); os.IsNotExist(err) {
		c.String(http.StatusNotFound, "Repository not found") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	// The operation comes from the path, never from the caller-controlled
	// ?service= query: honouring that let a receive-pack POST be authorised as a
	// read and dispatched as a push. Only info/refs may read the query, and only
	// through git_service_name.
	service := ""
	if strings.HasSuffix(path, "git-upload-pack") {
		service = "git-upload-pack"
	} else if strings.HasSuffix(path, "git-receive-pack") {
		service = "git-receive-pack"
	} else if strings.HasSuffix(path, "info/refs") {
		service = git_service_name(c.Query("service"))
	}

	// Determine if this is a read or write operation
	is_write := service == "git-receive-pack"

	// Try to authenticate if credentials are provided
	if user == nil {
		user = git_authenticate(c, a)
	}

	// A missing app-system database is refused, not skipped: db_app_system returns
	// nil when the handle cannot be created at all, and treating that as "no rules
	// to apply" would hand anonymous callers clone and push.
	app_db := db_app_system(owner, a)
	if app_db == nil {
		info("git_http_handler: no app-system database for user %q app %q; refusing", owner.UID, a.id)
		c.String(http.StatusInternalServerError, "Repository access unavailable") // i18n-ok: git protocol, read by the client not a person
		return true
	}
	identity_id := ""
	role := ""
	if user != nil {
		if ident := user.identity(); ident != nil {
			identity_id = ident.ID
		}
		role = user.Role
	}
	op := "read"
	if is_write {
		op = "write"
	}
	if !app_db.access_check(owner, identity_id, role, "repository/"+id, op) {
		if user == nil {
			c.Header("WWW-Authenticate", `Basic realm="Mochi Git"`)
			c.String(http.StatusUnauthorized, "Authentication required") // i18n-ok: git protocol, read by the client not a person
		} else {
			c.String(http.StatusNotFound, "Repository not found") // i18n-ok: git protocol, read by the client not a person
		}
		return true
	}

	// Route to appropriate handler
	if strings.HasSuffix(path, "info/refs") {
		return git_info_refs(c, repo_path, service)
	} else if strings.HasSuffix(path, "git-upload-pack") {
		return git_service_rpc(c, repo_path, "git-upload-pack", owner)
	} else if strings.HasSuffix(path, "git-receive-pack") {
		return git_service_rpc(c, repo_path, "git-receive-pack", owner)
	}

	c.String(http.StatusNotFound, "Not found") // i18n-ok: git protocol, read by the client not a person
	return true
}

// git_http_handler_entity handles git Smart HTTP for domain-routed entities.
// The entity is already resolved, so no fingerprint lookup is needed.
func git_http_handler_entity(c *gin.Context, a *App, owner *User, user *User, e *Entity, path string) bool {
	if owner == nil {
		c.String(http.StatusNotFound, "Repository not found") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	// Build repository path from the pre-resolved entity
	repo_path := git_repo_path(owner, a, e.ID)
	if _, err := os.Stat(repo_path); os.IsNotExist(err) {
		c.String(http.StatusNotFound, "Repository not found") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	// Determine operation from the path, never the caller-controlled query.
	// See git_http_handler above for what honouring the query allowed.
	service := ""
	if path == "git-upload-pack" {
		service = "git-upload-pack"
	} else if path == "git-receive-pack" {
		service = "git-receive-pack"
	} else if path == "info/refs" {
		service = git_service_name(c.Query("service"))
	}

	// Determine if this is a read or write operation
	is_write := service == "git-receive-pack"

	// Try to authenticate if credentials are provided
	if user == nil {
		user = git_authenticate(c, a)
	}

	// Check access control, failing closed on a missing app-system database.
	// See git_http_handler above for why nil is refused rather than skipped.
	app_db := db_app_system(owner, a)
	if app_db == nil {
		info("git_http_handler_entity: no app-system database for user %q app %q; refusing", owner.UID, a.id)
		c.String(http.StatusInternalServerError, "Repository access unavailable") // i18n-ok: git protocol, read by the client not a person
		return true
	}
	identity_id := ""
	role := ""
	if user != nil {
		if id := user.identity(); id != nil {
			identity_id = id.ID
		}
		role = user.Role
	}
	op := "read"
	if is_write {
		op = "write"
	}
	if !app_db.access_check(owner, identity_id, role, "repository/"+e.ID, op) {
		if user == nil {
			c.Header("WWW-Authenticate", `Basic realm="Mochi Git"`)
			c.String(http.StatusUnauthorized, "Authentication required") // i18n-ok: git protocol, read by the client not a person
		} else {
			c.String(http.StatusNotFound, "Repository not found") // i18n-ok: git protocol, read by the client not a person
		}
		return true
	}

	// Route to appropriate handler
	if path == "info/refs" {
		return git_info_refs(c, repo_path, service)
	} else if path == "git-upload-pack" {
		return git_service_rpc(c, repo_path, "git-upload-pack", owner)
	} else if path == "git-receive-pack" {
		return git_service_rpc(c, repo_path, "git-receive-pack", owner)
	}

	c.String(http.StatusNotFound, "Not found") // i18n-ok: git protocol, read by the client not a person
	return true
}

// git_authenticate extracts and validates Basic Auth credentials from the request
func git_authenticate(c *gin.Context, a *App) *User {
	_, password, ok := c.Request.BasicAuth()
	if !ok {
		return nil
	}

	// Validate token (checks expiration, updates used timestamp)
	token := token_validate(password)
	if token == nil {
		return nil
	}

	// Token must be for the repositories app
	if token.App != a.id {
		return nil
	}

	// ...and minted for git. Basic auth parses no api_token, so web_action's
	// action binding is never consulted here; this scope is the matching gate,
	// stopping a token minted for something else from cloning or pushing. Tokens
	// carrying no scopes still mean "all".
	if !token_has_scope(token, "git") {
		return nil
	}

	return user_by_uid(token.User)
}

// git_service_name returns the requested service only when it names one of the
// two git services, and "" otherwise. info/refs is the one endpoint whose
// service legitimately comes from the query string, so the value is whitelisted
// first.
func git_service_name(requested string) string {
	if requested == "git-upload-pack" || requested == "git-receive-pack" {
		return requested
	}
	return ""
}

// git_upload_pack_advertise adds the fetch capabilities this server implements
// to an upload-pack advertisement. None come from go-git, whose session rejects
// anything beyond agent and ofs-delta - which is why git_upload_pack builds the
// packfile itself. Every entry here must have a matching implementation below.
func git_upload_pack_advertise(capabilities *capability.List) {
	// multi_ack_detailed tells the client during negotiation which commits we
	// already share. Without it there is only NAK, so a client on the stateless
	// HTTP transport keeps offering haves until it runs out and the request that
	// finally asks for the pack carries its oldest haves, excluding almost
	// nothing.
	capabilities.Add(capability.MultiACKDetailed)
	capabilities.Add(capability.MultiACK)

	// side-band-64k multiplexes the packfile with progress and error messages;
	// without it a clone shows no "remote:" lines at all and a failure after the
	// pack starts has no channel to explain itself. side-band is the 1000-byte
	// legacy form; no-progress asks for the band without the commentary.
	capabilities.Add(capability.Sideband64k)
	capabilities.Add(capability.Sideband)
	capabilities.Add(capability.NoProgress)

	// shallow lets a client bound how much history it takes. Without it `git clone
	// --depth 1` fails outright rather than degrading to a full clone.
	// deepen-since and deepen-not name a boundary; deepen-relative measures it
	// from where the client's history currently stops.
	capabilities.Add(capability.Shallow)
	capabilities.Add(capability.DeepenSince)
	capabilities.Add(capability.DeepenNot)
	capabilities.Add(capability.DeepenRelative)

	// thin-pack lets the pack refer to objects the client already holds and
	// send only the difference against them. Without it every changed file
	// travels in full, however small the edit, because the only bases a pack
	// may use are the ones it also carries.
	capabilities.Add(capability.ThinPack)

	// filter is partial clone: unadvertised, `git clone --filter=blob:none`
	// silently cloned everything. include-tag saves a round trip for the tags on a
	// branch being cloned.
	capabilities.Add(capability.Filter)
	capabilities.Add(capability.IncludeTag)

	// A partial clone comes back for skipped objects by name rather than by ref.
	// These say it may; this server already serves any object it holds in a
	// repository the caller may read, so nothing new is granted.
	capabilities.Add(capability.AllowTipSHA1InWant)
	capabilities.Add(capability.AllowReachableSHA1InWant)
}

// git_advertise_peeled fills in what an annotated tag ultimately points at, so
// the advertisement carries the "<commit> <ref>^{}" line beside the tag object.
func git_advertise_peeled(storage storer.Storer, refs *packp.AdvRefs) {
	for name, hash := range refs.References {
		if !strings.HasPrefix(name, "refs/tags/") {
			continue
		}
		if target, tags, err := git_peel(storage, hash); err == nil && len(tags) > 0 {
			refs.Peeled[name] = target
		}
	}
}

// git_filter_rule reports whether one object survives one filter. depth is the
// object's distance from the root tree of a commit carrying it, or -1 when it
// sits in no tree.
type git_filter_rule func(kind plumbing.ObjectType, size int64, depth int) bool

// git_filter_apply drops the objects a partial-clone filter excludes. An
// unimplemented specification is refused rather than ignored, since the client
// has been told we honour it. A filter never removes an object named as a want,
// only ones reached from them - otherwise a lazy fetch could never retrieve its
// blob.
func git_filter_apply(storage storer.Storer, objects []plumbing.Hash, specification string, wants []plumbing.Hash) ([]plumbing.Hash, error) {
	rules, measured, err := git_filter_parse(specification)
	if err != nil {
		return nil, err
	}
	requested := git_hash_set(wants)

	depths := map[plumbing.Hash]int{}
	if measured {
		if depths, err = git_object_depths(storage, objects); err != nil {
			return nil, err
		}
	}

	kept := make([]plumbing.Hash, 0, len(objects))
	for _, hash := range objects {
		if requested[hash] {
			kept = append(kept, hash)
			continue
		}
		object, err := storage.EncodedObject(plumbing.AnyObject, hash)
		if err != nil {
			return nil, err
		}
		depth, known := depths[hash]
		if !known {
			depth = -1
		}
		keep := true
		for _, rule := range rules {
			if !rule(object.Type(), object.Size(), depth) {
				keep = false
				break
			}
		}
		if keep {
			kept = append(kept, hash)
		}
	}
	return kept, nil
}

// git_filter_parse turns a filter specification into the rules an object has to
// satisfy, and reports whether any of them needs to know how deep an object
// sits - which costs a tree walk, so it is only paid for when asked.
func git_filter_parse(specification string) ([]git_filter_rule, bool, error) {
	// A combined filter omits an object that ANY of its parts omits, so the
	// parts simply concatenate.
	if rest, ok := strings.CutPrefix(specification, "combine:"); ok {
		var rules []git_filter_rule
		measured := false
		for _, part := range strings.Split(rest, "+") {
			decoded, err := url.QueryUnescape(part)
			if err != nil {
				return nil, false, fmt.Errorf("malformed combined filter %q", specification)
			}
			inner, needed, err := git_filter_parse(decoded)
			if err != nil {
				return nil, false, err
			}
			rules = append(rules, inner...)
			measured = measured || needed
		}
		return rules, measured, nil
	}

	switch {
	case specification == "blob:none":
		return []git_filter_rule{func(kind plumbing.ObjectType, _ int64, _ int) bool {
			return kind != plumbing.BlobObject
		}}, false, nil

	case strings.HasPrefix(specification, "blob:limit="):
		limit, err := git_filter_size(strings.TrimPrefix(specification, "blob:limit="))
		if err != nil {
			return nil, false, err
		}
		return []git_filter_rule{func(kind plumbing.ObjectType, size int64, _ int) bool {
			return kind != plumbing.BlobObject || size < limit
		}}, false, nil

	case strings.HasPrefix(specification, "tree:"):
		limit, err := strconv.Atoi(strings.TrimPrefix(specification, "tree:"))
		if err != nil || limit < 0 {
			return nil, false, fmt.Errorf("malformed filter %q", specification)
		}
		return []git_filter_rule{func(kind plumbing.ObjectType, _ int64, depth int) bool {
			if kind != plumbing.TreeObject && kind != plumbing.BlobObject {
				return true
			}
			return depth >= 0 && depth < limit
		}}, true, nil

	case strings.HasPrefix(specification, "object:type="):
		wanted, err := git_filter_kind(strings.TrimPrefix(specification, "object:type="))
		if err != nil {
			return nil, false, err
		}
		return []git_filter_rule{func(kind plumbing.ObjectType, _ int64, _ int) bool {
			return kind == wanted
		}}, false, nil
	}

	return nil, false, fmt.Errorf("unsupported filter %q", specification)
}

// git_filter_size reads a byte count, which a client may scale with k, m or g.
func git_filter_size(text string) (int64, error) {
	multiplier := int64(1)
	switch strings.ToLower(text[max(len(text)-1, 0):]) {
	case "k":
		multiplier = 1 << 10
	case "m":
		multiplier = 1 << 20
	case "g":
		multiplier = 1 << 30
	}
	if multiplier > 1 {
		text = text[:len(text)-1]
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("malformed size %q", text)
	}
	return value * multiplier, nil
}

// git_filter_kind reads an object type name.
func git_filter_kind(name string) (plumbing.ObjectType, error) {
	switch name {
	case "blob":
		return plumbing.BlobObject, nil
	case "tree":
		return plumbing.TreeObject, nil
	case "commit":
		return plumbing.CommitObject, nil
	case "tag":
		return plumbing.TagObject, nil
	}
	return plumbing.InvalidObject, fmt.Errorf("unknown object type %q", name)
}

// git_object_depths measures how far each tree and blob sits from the root tree
// of a commit carrying it: the root tree is 0, its own entries 1, and so on.
// An object reached by several paths takes the shallowest.
func git_object_depths(storage storer.Storer, objects []plumbing.Hash) (map[plumbing.Hash]int, error) {
	depths := map[plumbing.Hash]int{}
	err := git_walk_paths(storage, git_commit_roots(storage, objects), func(path string, hash plumbing.Hash) {
		depth := 0
		if path != "" {
			depth = strings.Count(path, "/") + 1
		}
		if current, seen := depths[hash]; !seen || depth < current {
			depths[hash] = depth
		}
	})
	return depths, err
}

// git_commit_roots picks the commits out of an object list. They are the roots
// of every tree the list carries.
func git_commit_roots(storage storer.Storer, objects []plumbing.Hash) []plumbing.Hash {
	var commits []plumbing.Hash
	for _, hash := range objects {
		if _, err := storage.EncodedObject(plumbing.CommitObject, hash); err == nil {
			commits = append(commits, hash)
		}
	}
	return commits
}

// git_include_tags adds any annotated tag whose target is already in the pack.
// That is what include-tag asks for: a clone taking a branch gets the tags on
// it without a second round trip.
func git_include_tags(storage storer.Storer, objects []plumbing.Hash) ([]plumbing.Hash, error) {
	iterator, err := storage.IterReferences()
	if err != nil {
		return nil, err
	}
	defer iterator.Close()

	sending := git_hash_set(objects)
	err = iterator.ForEach(func(reference *plumbing.Reference) error {
		if reference.Type() != plumbing.HashReference || !strings.HasPrefix(reference.Name().String(), "refs/tags/") {
			return nil
		}
		target, tags, err := git_peel(storage, reference.Hash())
		if err != nil || !sending[target] {
			return nil
		}
		for _, tag := range tags {
			if !sending[tag] {
				sending[tag] = true
				objects = append(objects, tag)
			}
		}
		return nil
	})
	return objects, err
}

// git_request is an upload-pack request: what the client wants, what it has,
// and how much history it asks for. Parsed here rather than by
// packp.UploadRequest.Decode, which holds a single Depth (so it cannot combine
// deepen-not with deepen-since) and rejects a "filter" line outright.
type git_request struct {
	capabilities *capability.List
	wants        []plumbing.Hash
	haves        []plumbing.Hash
	shallows     []plumbing.Hash // the client's own boundary: it holds these but nothing behind them
	depth        int             // deepen <n>: commits along any path, counting the tip as 1; 0 when absent
	relative     bool            // deepen-relative: depth counts from the client's boundary, not from the tips
	since        time.Time       // deepen-since
	exclude      []string        // deepen-not, one per line
	filter       string
	tags         bool // include-tag: send annotated tags whose target is in the pack
	done         bool
}

// deepening reports whether the client asked for a different amount of history
// than it currently holds. Only a deepening request gets a shallow-info section
// in the response - a client that merely declares its existing boundary is not
// expecting one and fails on the lines it did not ask for.
func (r *git_request) deepening() bool {
	return r.depth > 0 || !r.since.IsZero() || len(r.exclude) > 0
}

// shallow reports whether shallow history is involved at all, in either
// direction.
func (r *git_request) shallow() bool {
	return len(r.shallows) > 0 || r.deepening()
}

// git_request_decode reads a whole upload-pack request body in one pass: every
// line in the v0 grammar is tagged by its keyword, so the sections need not be
// tracked, and flush packets carry nothing and are skipped.
func git_request_decode(reader io.Reader) (*git_request, error) {
	request := &git_request{capabilities: capability.NewList()}
	scanner := pktline.NewScanner(reader)
	first := true

	for scanner.Scan() {
		line := strings.TrimSuffix(string(scanner.Bytes()), "\n")
		if line == "" {
			continue
		}
		keyword, argument, _ := strings.Cut(line, " ")

		switch keyword {
		case "want":
			// Capabilities ride on the first want line, after the hash.
			name, capabilities, _ := strings.Cut(argument, " ")
			hash, err := git_request_hash(name)
			if err != nil {
				return nil, err
			}
			request.wants = append(request.wants, hash)
			if first && capabilities != "" {
				if err := request.capabilities.Decode([]byte(capabilities)); err != nil {
					return nil, fmt.Errorf("invalid capabilities %q: %w", capabilities, err)
				}
				request.relative = request.capabilities.Supports(capability.DeepenRelative)
				request.tags = request.capabilities.Supports(capability.IncludeTag)
			}
			first = false

		case "have":
			hash, err := git_request_hash(argument)
			if err != nil {
				return nil, err
			}
			request.haves = append(request.haves, hash)

		case "shallow":
			hash, err := git_request_hash(argument)
			if err != nil {
				return nil, err
			}
			request.shallows = append(request.shallows, hash)

		case "deepen":
			depth, err := strconv.Atoi(argument)
			if err != nil || depth < 0 {
				return nil, fmt.Errorf("malformed depth %q", argument)
			}
			request.depth = depth

		case "deepen-since":
			seconds, err := strconv.ParseInt(argument, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("malformed deepen-since %q", argument)
			}
			request.since = time.Unix(seconds, 0).UTC()

		case "deepen-not":
			request.exclude = append(request.exclude, argument)

		case "filter":
			request.filter = argument

		case "done":
			request.done = true

		default:
			return nil, fmt.Errorf("unexpected line %q", line)
		}
	}

	return request, scanner.Err()
}

// git_request_hash parses an object name. plumbing.NewHash reports nothing for
// malformed input - it returns the zero hash, which is indistinguishable from a
// client genuinely naming all zeroes - so the text is checked here.
func git_request_hash(text string) (plumbing.Hash, error) {
	hash := plumbing.NewHash(text)
	if len(text) != 40 || hash.IsZero() {
		return plumbing.ZeroHash, fmt.Errorf("malformed object name %q", text)
	}
	return hash, nil
}

func git_info_refs(c *gin.Context, repo_path string, service string) bool {
	if service != "git-upload-pack" && service != "git-receive-pack" {
		c.String(http.StatusForbidden, "Service not enabled") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	// A v2 advertisement carries no references at all, so it is answered
	// before any of the work below. Push stays on v0: its session is go-git's,
	// which has no v2 at all, and a client offered v0 simply uses it.
	if service == "git-upload-pack" && git_protocol_version(c) == 2 {
		return git_v2_advertise(c, service)
	}

	// Create endpoint for the repository path
	ep := &transport.Endpoint{Path: repo_path}
	ctx := context.Background()

	// Create appropriate session based on service and get advertised refs
	var refs *packp.AdvRefs
	if service == "git-upload-pack" {
		session, err := git_transport.NewUploadPackSession(ep, nil)
		if err != nil {
			info("git_info_refs: upload-pack session failed for %s: %v", repo_path, err)
			c.String(http.StatusInternalServerError, "Failed to create session") // i18n-ok: git protocol, read by the client not a person
			return true
		}
		defer session.Close()
		refs, err = session.AdvertisedReferencesContext(ctx)
		if err != nil {
			info("git_info_refs: upload-pack advertise refs failed for %s: %v", repo_path, err)
			c.String(http.StatusInternalServerError, "Failed to get refs") // i18n-ok: git protocol, read by the client not a person
			return true
		}
		git_upload_pack_advertise(refs.Capabilities)

		// go-git's advertisement carries no peeled refs, so an annotated tag gives
		// the client no way to see the commit behind it and `git ls-remote` shows no
		// "^{}" lines.
		if storage, err := (&git_loader{}).Load(ep); err == nil {
			git_advertise_peeled(storage, refs)
		}
	} else {
		session, err := git_transport.NewReceivePackSession(ep, nil)
		if err != nil {
			info("git_info_refs: receive-pack session failed for %s: %v", repo_path, err)
			c.String(http.StatusInternalServerError, "Failed to create session") // i18n-ok: git protocol, read by the client not a person
			return true
		}
		defer session.Close()
		refs, err = session.AdvertisedReferencesContext(ctx)
		if err != nil {
			info("git_info_refs: receive-pack advertise refs failed for %s: %v", repo_path, err)
			c.String(http.StatusInternalServerError, "Failed to get refs") // i18n-ok: git protocol, read by the client not a person
			return true
		}
	}

	c.Status(http.StatusOK)
	c.Header("Content-Type", fmt.Sprintf("application/x-%s-advertisement", service))
	c.Header("Cache-Control", "no-cache")

	// Git protocol: first send a packet-line with the service name
	git_service := fmt.Sprintf("# service=%s\n", service)
	pkt_line := fmt.Sprintf("%04x%s0000", len(git_service)+4, git_service)
	c.Writer.WriteString(pkt_line)

	// Encode advertised refs
	if err := refs.Encode(c.Writer); err != nil {
		info("git_info_refs: failed to encode refs: %v", err)
	}

	return true
}

// Ceiling on a git-upload-pack request body. It carries want/have negotiation
// lines only — never repository content — so it stays small however large the
// repository is: at roughly 50 bytes a line this allows hundreds of thousands
// of them.
const git_negotiation_maximum = 16 << 20 // 16MB

// git_limited_reader fails once more than remaining bytes have been read,
// rather than truncating the way io.LimitReader does. Truncation would surface
// as a corrupt-object error from the pack decoder, hiding a size refusal behind
// a confusing failure.
type git_limited_reader struct {
	reader    io.Reader
	remaining int64
}

func (r *git_limited_reader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, fmt.Errorf("git request body exceeds the maximum accepted size")
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	return n, err
}

// git_storage_budget returns the storage the owner may still consume, clamped
// to a finite value (administrators are quota-exempt and report MaxInt64, but
// unbounded is the thing being bounded here). It returns 0 when there is no
// owner or the figure cannot be established, which callers read as "no push".
func git_storage_budget(owner *User) int64 {
	// user_storage_remaining dereferences the user to locate its storage
	// directory, so a missing owner must not reach it.
	if owner == nil {
		return 0
	}
	remaining, err := user_storage_remaining(owner)
	if err != nil || remaining <= 0 {
		return 0
	}
	if remaining > file_maximum_storage {
		remaining = file_maximum_storage
	}
	return remaining
}

// git_request_maximum returns the largest request body this service may send. A
// receive-pack body becomes repository content, so it is bounded by the owner's
// remaining storage; upload-pack negotiation is never stored and gets the fixed
// ceiling above. This bounds the pack as sent - git_storage meters what it
// decodes to.
func git_request_maximum(service string, budget int64) int64 {
	if service != "git-receive-pack" {
		return git_negotiation_maximum
	}
	// No owner, or no room left, still has to allow the small bodies: the
	// flush-packet authentication probe and a delete-only push carry no pack.
	// The decode meter is what refuses content in that state.
	if budget <= 0 {
		return git_negotiation_maximum
	}
	return budget
}

// git_service_rpc handles POST /git-upload-pack and /git-receive-pack
func git_service_rpc(c *gin.Context, repo_path string, service string, owner *User) bool {
	// Bound the request body. git pack bodies are exempt from web_body_limit, so
	// without this both the compressed body and its gzip expansion are unbounded
	// and any client allowed to fetch can send a decompression bomb. Measured
	// once: the budget walk is a storage-directory traversal.
	budget := git_storage_budget(owner)
	maximum := git_request_maximum(service, budget)
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maximum)

	// Handle gzip compressed request body
	var reader io.ReadCloser = c.Request.Body
	if c.GetHeader("Content-Encoding") == "gzip" {
		gz_reader, err := gzip.NewReader(c.Request.Body)
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid gzip data") // i18n-ok: git protocol, read by the client not a person
			return true
		}
		defer gz_reader.Close()
		// Bound the decompressed stream too: the cap above limits what the
		// client sends, not what it expands to.
		reader = io.NopCloser(&git_limited_reader{reader: gz_reader, remaining: maximum})
	}

	// A request whose body exceeds the client's http.postBuffer cannot be
	// replayed on an authentication challenge, so git first sends a probe
	// request containing a single flush packet to settle authentication.
	// git-http-backend answers it with an empty success; go-git's decoders
	// reject the bare flush, so answer it here before dispatching.
	buffered := bufio.NewReader(reader)
	head, _ := buffered.Peek(4)
	if string(head) == "0000" {
		c.Status(http.StatusOK)
		c.Header("Content-Type", fmt.Sprintf("application/x-%s-result", service))
		c.Header("Cache-Control", "no-cache")
		return true
	}
	reader = io.NopCloser(buffered)

	if service == "git-upload-pack" {
		if git_protocol_version(c) == 2 {
			return git_v2_serve(c, repo_path, reader)
		}
		return git_upload_pack(c, repo_path, reader)
	}
	return git_receive_pack(c, repo_path, reader, budget)
}

// Protocol version 2 ---------------------------------------------------------

// git_protocol_version reads the version a client asked for from the
// Git-Protocol header's colon-separated parameters. An unknown version is
// answered in v0, the documented fallback.
func git_protocol_version(c *gin.Context) int {
	for _, parameter := range strings.Split(c.GetHeader("Git-Protocol"), ":") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(parameter), "version="); ok {
			if version, err := strconv.Atoi(value); err == nil {
				return version
			}
		}
	}
	return 0
}

// git_v2_advertise answers the opening info/refs request in protocol v2: a
// version line and the commands this server implements, and notably NOT the
// references. That omission is the point - v0 sends every ref on every
// connection, tens of kilobytes before an object moves; in v2 the client asks.
func git_v2_advertise(c *gin.Context, service string) bool {
	c.Status(http.StatusOK)
	c.Header("Content-Type", fmt.Sprintf("application/x-%s-advertisement", service))
	c.Header("Cache-Control", "no-cache")

	encoder := pktline.NewEncoder(c.Writer)
	// The service line is v0's framing, but a v2 client steps over it when it
	// is there and reads the version line behind it, so one shape serves both
	// and any smart-HTTP intermediary still sees what it expects.
	encoder.Encodef("# service=%s\n", service)
	encoder.Flush()

	encoder.Encodef("version 2\n")
	// The agent value may not be empty, and build_version is stamped in by the
	// linker - it is blank in any build that did not go through the Makefile.
	version := build_version
	if version == "" {
		version = "unknown"
	}
	encoder.Encodef("agent=mochi/%s\n", version)
	encoder.Encodef("ls-refs\n")
	// Only features beyond the base fetch arguments are named here: shallow
	// covers the whole deepen family, filter is partial clone. thin-pack,
	// no-progress, ofs-delta and include-tag are base arguments in v2 and are
	// not advertised.
	encoder.Encodef("fetch=shallow filter\n")
	encoder.Encodef("object-format=sha1\n")
	encoder.Flush()
	return true
}

// Packet kinds git_pktline distinguishes. v2 gives 0001 and 0002 meanings that
// v0 never had.
const (
	git_packet_data = iota
	git_packet_flush
	git_packet_delimiter
	git_packet_end
)

// git_pktline reads packet lines, telling the special packets apart. go-git's
// scanner was written for v0, where a length of 1 or 2 cannot occur, and
// rejects a delimiter packet as an invalid length.
type git_pktline struct {
	reader *bufio.Reader
}

// next returns the next packet's payload with its trailing newline removed, or
// an empty payload and the kind of special packet that was found.
func (p *git_pktline) next() (string, int, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(p.reader, header); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return "", git_packet_end, nil
		}
		return "", git_packet_data, err
	}

	length, err := strconv.ParseInt(string(header), 16, 32)
	if err != nil {
		return "", git_packet_data, fmt.Errorf("malformed packet length %q", header)
	}
	switch length {
	case 0:
		return "", git_packet_flush, nil
	case 1:
		return "", git_packet_delimiter, nil
	case 2:
		return "", git_packet_end, nil
	}
	if length < 4 || length > 65524 {
		return "", git_packet_data, fmt.Errorf("packet length %d out of range", length)
	}

	payload := make([]byte, length-4)
	if _, err := io.ReadFull(p.reader, payload); err != nil {
		return "", git_packet_data, err
	}
	return strings.TrimSuffix(string(payload), "\n"), git_packet_data, nil
}

// git_v2_delimiter writes a delimiter packet, which separates the sections of a
// v2 response. go-git's encoder has no notion of one.
func git_v2_delimiter(writer io.Writer) error {
	_, err := writer.Write([]byte("0001"))
	return err
}

// git_v2_command is a v2 request: one command, and the arguments that follow
// the delimiter packet.
type git_v2_command struct {
	name      string
	arguments []string
}

// git_v2_decode reads a v2 command request. Before the delimiter the client
// names the command and the capabilities it is using; after it come the
// command's own arguments.
func git_v2_decode(reader io.Reader) (*git_v2_command, error) {
	lines := &git_pktline{reader: bufio.NewReader(reader)}
	command := &git_v2_command{}
	arguments := false

	for {
		payload, kind, err := lines.next()
		if err != nil {
			return nil, err
		}
		switch kind {
		case git_packet_data:
			if arguments {
				command.arguments = append(command.arguments, payload)
				continue
			}
			if name, ok := strings.CutPrefix(payload, "command="); ok {
				command.name = name
			}
			// Anything else before the delimiter is a capability the client
			// is using - agent, object-format - and none of them change what
			// this server does.
		case git_packet_delimiter:
			arguments = true
		case git_packet_flush, git_packet_end:
			if command.name == "" {
				return nil, fmt.Errorf("request names no command")
			}
			return command, nil
		}
	}
}

// git_v2_serve dispatches one v2 command.
func git_v2_serve(c *gin.Context, repo_path string, reader io.ReadCloser) bool {
	storage, err := (&git_loader{}).Load(&transport.Endpoint{Path: repo_path})
	if err != nil {
		info("git_v2_serve: cannot load %s: %v", repo_path, err)
		c.String(http.StatusInternalServerError, "Failed to open repository") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	command, err := git_v2_decode(reader)
	if err != nil {
		c.String(http.StatusBadRequest, "Failed to decode request: %v", err) // i18n-ok: git protocol, read by the client not a person
		return true
	}

	switch command.name {
	case "ls-refs":
		return git_v2_ls_refs(c, storage, command.arguments)
	case "fetch":
		return git_v2_fetch(c, storage, repo_path, command.arguments)
	}
	c.String(http.StatusBadRequest, "Unknown command %q", command.name) // i18n-ok: git protocol, read by the client not a person
	return true
}

// git_v2_ls_refs answers the v2 reference advertisement, which unlike v0's is
// asked for explicitly and can be narrowed to the refs the client cares about.
func git_v2_ls_refs(c *gin.Context, storage storer.Storer, arguments []string) bool {
	var prefixes []string
	peel, symrefs := false, false
	for _, argument := range arguments {
		switch {
		case argument == "peel":
			peel = true
		case argument == "symrefs":
			symrefs = true
		case strings.HasPrefix(argument, "ref-prefix "):
			prefixes = append(prefixes, strings.TrimPrefix(argument, "ref-prefix "))
		}
	}

	type advertised struct {
		name   string
		hash   plumbing.Hash
		target string
		peeled plumbing.Hash
	}
	var refs []advertised

	wanted := func(name string) bool {
		if len(prefixes) == 0 {
			return true
		}
		for _, prefix := range prefixes {
			if strings.HasPrefix(name, prefix) {
				return true
			}
		}
		return false
	}

	// HEAD comes first and is the one ref that is normally symbolic, so the
	// client learns which branch a fresh clone should check out.
	if head, err := storage.Reference(plumbing.HEAD); err == nil && wanted("HEAD") {
		entry := advertised{name: "HEAD"}
		if head.Type() == plumbing.SymbolicReference {
			if symrefs {
				entry.target = head.Target().String()
			}
			resolved, err := storer.ResolveReference(storage, head.Target())
			if err == nil && resolved != nil {
				entry.hash = resolved.Hash()
			}
		} else {
			entry.hash = head.Hash()
		}
		// A HEAD pointing at a branch with no commits yet has nothing to
		// advertise, and this server does not offer the unborn feature.
		if !entry.hash.IsZero() {
			refs = append(refs, entry)
		}
	}

	iterator, err := storage.IterReferences()
	if err != nil {
		info("git_v2_ls_refs: cannot iterate references: %v", err)
		c.String(http.StatusInternalServerError, "Failed to get refs") // i18n-ok: git protocol, read by the client not a person
		return true
	}
	err = iterator.ForEach(func(reference *plumbing.Reference) error {
		name := reference.Name().String()
		if reference.Type() != plumbing.HashReference || name == "HEAD" || !wanted(name) {
			return nil
		}
		entry := advertised{name: name, hash: reference.Hash()}
		if peel {
			// A peeled line names what an annotated tag ultimately points at,
			// so the client can tell a tag object from the commit it tags
			// without fetching it first.
			if target, tags, err := git_peel(storage, reference.Hash()); err == nil && len(tags) > 0 {
				entry.peeled = target
			}
		}
		refs = append(refs, entry)
		return nil
	})
	iterator.Close()
	if err != nil {
		info("git_v2_ls_refs: cannot read references: %v", err)
		c.String(http.StatusInternalServerError, "Failed to get refs") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	// HEAD stays at the front; the rest go in name order so the same
	// repository advertises the same way twice.
	sort.SliceStable(refs, func(i, j int) bool {
		if refs[i].name == "HEAD" || refs[j].name == "HEAD" {
			return refs[i].name == "HEAD" && refs[j].name != "HEAD"
		}
		return refs[i].name < refs[j].name
	})

	c.Status(http.StatusOK)
	c.Header("Content-Type", "application/x-git-upload-pack-result")
	c.Header("Cache-Control", "no-cache")

	encoder := pktline.NewEncoder(c.Writer)
	for _, entry := range refs {
		line := entry.hash.String() + " " + entry.name
		if entry.target != "" {
			line += " symref-target:" + entry.target
		}
		if !entry.peeled.IsZero() {
			line += " peeled:" + entry.peeled.String()
		}
		encoder.Encodef("%s\n", line)
	}
	encoder.Flush()
	return true
}

// git_v2_fetch answers the v2 fetch command.
func git_v2_fetch(c *gin.Context, storage storer.Storer, repo_path string, arguments []string) bool {
	request, err := git_v2_fetch_request(arguments)
	if err != nil {
		c.String(http.StatusBadRequest, "Failed to decode request: %v", err) // i18n-ok: git protocol, read by the client not a person
		return true
	}
	if len(request.wants) == 0 {
		c.String(http.StatusBadRequest, "Request asks for nothing") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	var known []plumbing.Hash
	for _, have := range request.haves {
		if storage.HasEncodedObject(have) != nil {
			continue
		}
		commit, _, err := git_peel(storage, have)
		if err != nil {
			continue
		}
		known = append(known, commit)
	}

	// "ready" says a pack can be built from what has been found in common, and
	// commits this response to carrying it. Without it the client keeps offering
	// haves until it runs out.
	ready := !request.done && len(known) > 0

	selection, err := git_upload_pack_select(storage, request, known, request.done || ready)
	if err != nil {
		info("git_v2_fetch: selecting objects for %s failed: %v", repo_path, err)
		c.String(http.StatusInternalServerError, "Upload pack failed") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	c.Status(http.StatusOK)
	c.Header("Content-Type", "application/x-git-upload-pack-result")
	c.Header("Cache-Control", "no-cache")
	encoder := pktline.NewEncoder(c.Writer)

	// The acknowledgements section is omitted entirely once the client has
	// said "done" - the specification requires that - and is the whole
	// response while negotiation has not converged.
	if !request.done {
		encoder.Encodef("acknowledgments\n")
		if !ready {
			encoder.Encodef("NAK\n")
			encoder.Flush()
			return true
		}
		for _, hash := range known {
			encoder.Encodef("ACK %s\n", hash.String())
		}
		encoder.Encodef("ready\n")
		git_v2_delimiter(c.Writer)
	}

	if selection.update != nil {
		encoder.Encodef("shallow-info\n")
		for _, hash := range selection.update.Shallows {
			encoder.Encodef("shallow %s\n", hash.String())
		}
		for _, hash := range selection.update.Unshallows {
			encoder.Encodef("unshallow %s\n", hash.String())
		}
		git_v2_delimiter(c.Writer)
	}

	encoder.Encodef("packfile\n")

	// v2 always multiplexes the packfile section, so the band is not something
	// the client opts into here; git_v2_fetch_request sets the capability.
	band := git_band_open(c.Writer, request.capabilities)
	band.message("Enumerating objects: %d, done.\n", len(selection.objects))
	if err := git_upload_pack_send(band, storage, selection); err != nil {
		info("git_v2_fetch: encoding the packfile for %s failed: %v", repo_path, err)
		band.fail("packfile generation failed")
	}
	band.close()
	return true
}

// git_v2_fetch_request turns v2 argument lines into the same request the v0
// decoder produces, so the history walk, the shallow boundary, the filter and
// the pack are one implementation serving both protocols.
func git_v2_fetch_request(arguments []string) (*git_request, error) {
	request := &git_request{capabilities: capability.NewList()}
	// The packfile section of a v2 response is always multiplexed.
	request.capabilities.Add(capability.Sideband64k)

	for _, argument := range arguments {
		keyword, value, _ := strings.Cut(argument, " ")
		switch keyword {
		case "want":
			hash, err := git_request_hash(value)
			if err != nil {
				return nil, err
			}
			request.wants = append(request.wants, hash)

		case "have":
			hash, err := git_request_hash(value)
			if err != nil {
				return nil, err
			}
			request.haves = append(request.haves, hash)

		case "shallow":
			hash, err := git_request_hash(value)
			if err != nil {
				return nil, err
			}
			request.shallows = append(request.shallows, hash)

		case "deepen":
			depth, err := strconv.Atoi(value)
			if err != nil || depth < 0 {
				return nil, fmt.Errorf("malformed depth %q", value)
			}
			request.depth = depth

		case "deepen-since":
			seconds, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("malformed deepen-since %q", value)
			}
			request.since = time.Unix(seconds, 0).UTC()

		case "deepen-not":
			request.exclude = append(request.exclude, value)

		case "deepen-relative":
			request.relative = true

		case "filter":
			request.filter = value

		case "done":
			request.done = true

		case "thin-pack":
			request.capabilities.Add(capability.ThinPack)

		case "no-progress":
			request.capabilities.Add(capability.NoProgress)

		case "include-tag":
			request.tags = true

		case "ofs-delta":
			// The pack encoder uses offset deltas within a pack already, and
			// falls back to reference deltas only for a thin pack's outside
			// bases, where an offset cannot reach.

		default:
			return nil, fmt.Errorf("unexpected argument %q", argument)
		}
	}
	return request, nil
}

// git_selection is what a fetch decided to do: the objects to send, and - when
// the client asked about shallow history - where its history now stops.
type git_selection struct {
	objects []plumbing.Hash
	update  *packp.ShallowUpdate
	known   []plumbing.Hash // the commits the client offered that this repository holds
	thin    bool            // the client will resolve deltas against bases the pack does not carry
}

// git_upload_pack_select decides what a fetch sends. pack is false for an
// exploratory negotiation round, which still needs the shallow boundary
// answered but must not pay for the object walk that would build a packfile.
func git_upload_pack_select(storage storer.Storer, request *git_request, known []plumbing.Hash, pack bool) (*git_selection, error) {
	selection := &git_selection{
		known: known,
		thin:  request.capabilities.Supports(capability.ThinPack),
	}

	if !request.shallow() {
		if !pack {
			return selection, nil
		}
		// The ordinary case, and the fast one: everything reachable from the
		// wants, less everything reachable from the haves, with revlist
		// pruning the walk as it goes.
		shared, err := revlist.Objects(storage, known, nil)
		if err != nil {
			return nil, err
		}
		if selection.objects, err = revlist.Objects(storage, request.wants, shared); err != nil {
			return nil, err
		}
		return selection, git_upload_pack_trim(storage, request, selection)
	}

	// Sort the wants. The walk below is over commits, so a want naming an
	// annotated tag is peeled and the tag object travels alongside. A want that is
	// a blob or tree is content to include, not a tip to walk from - that is how a
	// partial clone comes back for an object, and peeling one fails.
	var tips, tags, named []plumbing.Hash
	for _, want := range request.wants {
		object, err := storage.EncodedObject(plumbing.AnyObject, want)
		if err != nil {
			return nil, err
		}
		if object.Type() == plumbing.BlobObject || object.Type() == plumbing.TreeObject {
			named = append(named, want)
			continue
		}
		commit, walked, err := git_peel(storage, want)
		if err != nil {
			return nil, err
		}
		tips = append(tips, commit)
		tags = append(tags, walked...)
	}

	included, boundary, err := git_upload_pack_history(storage, request, tips)
	if err != nil {
		return nil, err
	}

	// What the client already holds, its own boundary respected. A walk that
	// ran past that boundary would conclude it has objects it does not, and
	// the pack would arrive missing them.
	held, _, err := git_history(storage, known, git_hash_set(request.shallows), nil)
	if err != nil {
		return nil, err
	}

	// Only a deepening request gets a shallow list. A client that merely
	// declared its existing boundary is not expecting one and treats the lines
	// it did not ask for as a protocol error.
	if request.deepening() {
		edge := git_hash_set(boundary)
		reached := git_hash_set(included)
		selection.update = &packp.ShallowUpdate{Shallows: boundary}
		for _, hash := range request.shallows {
			if reached[hash] && !edge[hash] {
				selection.update.Unshallows = append(selection.update.Unshallows, hash)
			}
		}
	}

	if !pack {
		return selection, nil
	}
	if selection.objects, err = git_objects(storage, included, held, tags, named); err != nil {
		return nil, err
	}
	return selection, git_upload_pack_trim(storage, request, selection)
}

// git_upload_pack_trim adds the tags include-tag asked to travel with the pack,
// drops whatever a partial-clone filter excludes, and settles the order. Tags
// first, since a filter is entitled to remove one again.
func git_upload_pack_trim(storage storer.Storer, request *git_request, selection *git_selection) error {
	var err error
	if request.tags {
		if selection.objects, err = git_include_tags(storage, selection.objects); err != nil {
			return err
		}
	}
	if request.filter != "" {
		if selection.objects, err = git_filter_apply(storage, selection.objects, request.filter, request.wants); err != nil {
			return err
		}
	}
	git_sort_hashes(selection.objects)
	return nil
}

// git_sort_hashes puts an object list in a stable order. The pack is written
// from it in order, and revlist returns its result by iterating a Go map, so
// without this two identical fetches produce different bytes.
func git_sort_hashes(hashes []plumbing.Hash) {
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
}

// git_upload_pack_history works out which commits a shallow fetch covers, and
// which of them end up on the boundary.
func git_upload_pack_history(storage storer.Storer, request *git_request, tips []plumbing.Hash) (included, boundary []plumbing.Hash, err error) {
	// deepen-relative measures depth from where the client's history stops, so the
	// new boundary comes from its shallow commits first. Counting a shallow commit
	// itself as 1, depth+1 adds exactly the generations asked for.
	if request.relative && request.depth > 0 && len(request.shallows) > 0 {
		_, edge, err := git_history(storage, request.shallows, nil, git_depth_limit(request.depth+1))
		if err != nil {
			return nil, nil, err
		}
		return git_history(storage, tips, git_hash_set(edge), nil)
	}

	follow, err := git_upload_pack_limit(storage, request)
	if err != nil {
		return nil, nil, err
	}
	return git_history(storage, tips, nil, follow)
}

// git_upload_pack_limit builds the rule deciding how far back a deepening
// request reaches. It returns nil when the client named no limit at all, which
// means walk the whole history - `git fetch --unshallow` arrives as a depth of
// 2147483647 and amounts to the same thing.
func git_upload_pack_limit(storage storer.Storer, request *git_request) (func(*object.Commit, int) bool, error) {
	var excluded map[plumbing.Hash]bool
	if len(request.exclude) > 0 {
		var err error
		if excluded, err = git_excluded(storage, request.exclude); err != nil {
			return nil, err
		}
	}
	depth, since := request.depth, request.since
	if depth == 0 && since.IsZero() && excluded == nil {
		return nil, nil
	}
	return func(parent *object.Commit, reached int) bool {
		if depth > 0 && reached > depth {
			return false
		}
		if !since.IsZero() && parent.Committer.When.Before(since) {
			return false
		}
		return !excluded[parent.Hash]
	}, nil
}

// git_depth_limit stops a walk a fixed number of commits along any path.
func git_depth_limit(depth int) func(*object.Commit, int) bool {
	return func(_ *object.Commit, reached int) bool { return reached <= depth }
}

// git_excluded resolves the refs a deepen-not names and returns every commit
// reachable from them: --shallow-exclude means "stop where this history begins".
func git_excluded(storage storer.Storer, refs []string) (map[plumbing.Hash]bool, error) {
	excluded := map[plumbing.Hash]bool{}
	for _, name := range refs {
		hash, err := git_reference_hash(storage, name)
		if err != nil {
			return nil, err
		}
		commit, _, err := git_peel(storage, hash)
		if err != nil {
			return nil, err
		}
		reached, _, err := git_history(storage, []plumbing.Hash{commit}, nil, nil)
		if err != nil {
			return nil, err
		}
		for _, hash := range reached {
			excluded[hash] = true
		}
	}
	return excluded, nil
}

// git_reference_hash resolves a name the client gave, trying the abbreviations
// git itself accepts before falling back to reading it as an object name.
func git_reference_hash(storage storer.ReferenceStorer, name string) (plumbing.Hash, error) {
	for _, candidate := range []string{name, "refs/" + name, "refs/tags/" + name, "refs/heads/" + name} {
		reference, err := storer.ResolveReference(storage, plumbing.ReferenceName(candidate))
		if err == nil && reference != nil {
			return reference.Hash(), nil
		}
	}
	if hash, err := git_request_hash(name); err == nil {
		return hash, nil
	}
	return plumbing.ZeroHash, fmt.Errorf("cannot resolve %q", name)
}

// git_history walks back from roots and returns the commits reached, plus the
// boundary: those with a parent that is not itself reached. A boundary commit
// is what a "shallow" line names - the client is told its history stops there.
//
// follow decides each parent edge, given the parent and its distance from a
// root, counting a root as 1; nil follows every edge. Commits in cut are
// grafted roots: they are reached, but nothing behind them is.
func git_history(
	storage storer.EncodedObjectStorer,
	roots []plumbing.Hash,
	cut map[plumbing.Hash]bool,
	follow func(parent *object.Commit, depth int) bool,
) (reached []plumbing.Hash, boundary []plumbing.Hash, err error) {
	type step struct {
		hash  plumbing.Hash
		depth int
	}

	parents := map[plumbing.Hash][]plumbing.Hash{}
	included := map[plumbing.Hash]bool{}
	queue := make([]step, 0, len(roots))
	for _, root := range roots {
		queue = append(queue, step{hash: root, depth: 1})
	}

	// Breadth first, so the first time a commit is taken off the queue is by
	// its shortest path from a root. A depth limit that saw a longer path
	// first would cut away history the client asked for.
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if included[current.hash] {
			continue
		}
		commit, err := object.GetCommit(storage, current.hash)
		if err != nil {
			return nil, nil, err
		}
		included[current.hash] = true
		reached = append(reached, current.hash)
		parents[current.hash] = commit.ParentHashes

		if cut[current.hash] {
			continue
		}
		for _, parent := range commit.ParentHashes {
			if included[parent] {
				continue
			}
			ancestor, err := object.GetCommit(storage, parent)
			if err != nil {
				// A parent this repository does not hold ends the history
				// here as surely as a depth limit does.
				continue
			}
			if follow != nil && !follow(ancestor, current.depth+1) {
				continue
			}
			queue = append(queue, step{hash: parent, depth: current.depth + 1})
		}
	}

	// The boundary can only be settled once the whole reachable set is known:
	// a parent refused along one path may well arrive by a shorter one, and a
	// commit is only shallow if NO path reached its parent.
	for _, hash := range reached {
		for _, parent := range parents[hash] {
			if !included[parent] {
				boundary = append(boundary, hash)
				break
			}
		}
	}
	return reached, boundary, nil
}

// git_objects turns two commit sets into the objects to send: what the wanted
// commits reach and the held ones do not, plus the commits and tag objects.
// Only trees are walked, never parents - the caller has already decided which
// commits are in play.
func git_objects(storage storer.Storer, wanted, held, tags, named []plumbing.Hash) ([]plumbing.Hash, error) {
	held_trees, err := git_trees(storage, held)
	if err != nil {
		return nil, err
	}
	// Only trees go in, so revlist's expansion of the ignore set walks trees
	// and blobs and cannot reach a parent commit the client does not hold.
	held_objects, err := revlist.Objects(storage, held_trees, nil)
	if err != nil {
		return nil, err
	}

	wanted_trees, err := git_trees(storage, wanted)
	if err != nil {
		return nil, err
	}
	// Objects named directly by the client are expanded the same way, so a
	// tree asked for by name brings what it contains.
	objects, err := revlist.Objects(storage, append(wanted_trees, named...), held_objects)
	if err != nil {
		return nil, err
	}

	// revlist saw only trees, so the commits and tags are not in the list yet.
	seen := git_hash_set(held)
	for _, hash := range append(append([]plumbing.Hash{}, wanted...), tags...) {
		if !seen[hash] {
			seen[hash] = true
			objects = append(objects, hash)
		}
	}
	return objects, nil
}

// git_trees returns the tree each commit points at.
func git_trees(storage storer.EncodedObjectStorer, commits []plumbing.Hash) ([]plumbing.Hash, error) {
	trees := make([]plumbing.Hash, 0, len(commits))
	for _, hash := range commits {
		commit, err := object.GetCommit(storage, hash)
		if err != nil {
			return nil, err
		}
		trees = append(trees, commit.TreeHash)
	}
	return trees, nil
}

// git_peel resolves an object name to the commit it names, returning any tag
// objects walked through so they can travel in the pack alongside it.
func git_peel(storage storer.EncodedObjectStorer, hash plumbing.Hash) (plumbing.Hash, []plumbing.Hash, error) {
	var tags []plumbing.Hash
	current := hash
	// A tag pointing at a tag is legal; a chain long enough to matter is not.
	for depth := 0; depth < 16; depth++ {
		encoded, err := storage.EncodedObject(plumbing.AnyObject, current)
		if err != nil {
			return plumbing.ZeroHash, nil, err
		}
		switch encoded.Type() {
		case plumbing.CommitObject:
			return current, tags, nil
		case plumbing.TagObject:
			decoded, err := object.DecodeObject(storage, encoded)
			if err != nil {
				return plumbing.ZeroHash, nil, err
			}
			tag, ok := decoded.(*object.Tag)
			if !ok {
				return plumbing.ZeroHash, nil, fmt.Errorf("object %s does not decode as a tag", current)
			}
			tags = append(tags, current)
			current = tag.Target
		default:
			return plumbing.ZeroHash, nil, fmt.Errorf("object %s is a %s, not a commit", current, encoded.Type())
		}
	}
	return plumbing.ZeroHash, nil, fmt.Errorf("tag chain from %s is too long to follow", hash)
}

// git_hash_set indexes hashes for membership tests.
func git_hash_set(hashes []plumbing.Hash) map[plumbing.Hash]bool {
	set := make(map[plumbing.Hash]bool, len(hashes))
	for _, hash := range hashes {
		set[hash] = true
	}
	return set
}

// git_upload_pack handles the git-upload-pack service (fetch/clone). The
// packfile is built here rather than by go-git's upSession.UploadPack, which
// refuses any capability beyond agent and ofs-delta - so every capability
// advertised would have to be stripped from the request and would still be
// unimplemented.
func git_upload_pack(c *gin.Context, repo_path string, reader io.ReadCloser) bool {
	ep := &transport.Endpoint{Path: repo_path}

	storage, err := (&git_loader{}).Load(ep)
	if err != nil {
		info("git_upload_pack: cannot load %s: %v", repo_path, err)
		c.String(http.StatusInternalServerError, "Failed to open repository") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	// The whole body in one pass - wants and capabilities, shallow and deepen
	// lines, haves, and the final "done". The negotiation section is not optional:
	// without haves every fetch ships the full repository, and a round without
	// "done" must not answer with a packfile ("bad line length character: PACK").
	request, err := git_request_decode(reader)
	if err != nil {
		c.String(http.StatusBadRequest, "Failed to decode request: %v", err) // i18n-ok: git protocol, read by the client not a person
		return true
	}
	if len(request.wants) == 0 {
		c.String(http.StatusBadRequest, "Request asks for nothing") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	// Keep only the haves this repository holds, peeled to the commits they
	// name: the client legitimately offers commits from its other branches,
	// and the history walk fails outright on an object it cannot read.
	var known []plumbing.Hash
	for _, have := range request.haves {
		if storage.HasEncodedObject(have) != nil {
			continue
		}
		commit, _, err := git_peel(storage, have)
		if err != nil {
			continue
		}
		known = append(known, commit)
	}

	c.Header("Content-Type", "application/x-git-upload-pack-result")
	c.Header("Cache-Control", "no-cache")

	// The shallow boundary has to be settled even for an exploratory round: a
	// deepening client reads the shallow list and its flush before it will look at
	// an acknowledgement. The packfile is another matter, so no object walk here.
	selection, err := git_upload_pack_select(storage, request, known, request.done)
	if err != nil {
		info("git_upload_pack: selecting objects for %s failed: %v", repo_path, err)
		c.String(http.StatusInternalServerError, "Upload pack failed") // i18n-ok: git protocol, read by the client not a person
		return true
	}

	if !request.done {
		// An exploratory round: the client expects acknowledgement lines, never a
		// packfile. Each have we hold is acknowledged "common" and the last "ready",
		// which ends negotiation while the client's haves are still recent. A bare
		// "ACK <sha>" must NOT be used - in single-ack that means "pack next", and
		// the client would expect one in this response.
		c.Status(http.StatusOK)
		if selection.update != nil {
			if err := selection.update.Encode(c.Writer); err != nil {
				info("git_upload_pack: failed to encode the shallow list: %v", err)
				return true
			}

			// A deepening client's first request asks only where its history will stop:
			// it reads the shallow list and its flush, then stops reading. Anything
			// written after that surfaces at the head of the NEXT response ("fatal: git
			// fetch-pack: expected shallow list").
			if len(request.haves) == 0 {
				return true
			}
		}
		e := pktline.NewEncoder(c.Writer)
		if len(known) == 0 {
			e.Encodef("NAK\n")
			return true
		}
		for _, have := range known {
			e.Encodef("ACK %s common\n", have.String())
		}
		// NAK terminates the round - not "nothing in common", but "every have here
		// has been answered". Omitting it blocks fetch-pack on a socket read, and a
		// flush packet in its place is refused outright.
		e.Encodef("NAK\n")
		return true
	}

	c.Status(http.StatusOK)

	// The shallow list comes before everything else, and carries its own flush
	// packet. A deepening client reads it - and requires that flush - before it
	// will read an acknowledgement.
	if selection.update != nil {
		if err := selection.update.Encode(c.Writer); err != nil {
			info("git_upload_pack: failed to encode the shallow list: %v", err)
			return true
		}
	}

	// The final round: a single ACK naming the commit the pack was built against,
	// or NAK when nothing is in common. Exactly one - more would be the "common"
	// lines of the exploratory rounds. Always plain pkt-line; the side band, if
	// any, begins at the packfile.
	acknowledgement := packp.ServerResponse{}
	if len(known) > 0 {
		acknowledgement.ACKs = []plumbing.Hash{known[len(known)-1]}
	}
	if err := acknowledgement.Encode(c.Writer, true); err != nil {
		info("git_upload_pack: failed to encode acknowledgements: %v", err)
		return true
	}

	band := git_band_open(c.Writer, request.capabilities)
	band.message("Enumerating objects: %d, done.\n", len(selection.objects))

	if err := git_upload_pack_send(band, storage, selection); err != nil {
		// Past this point the status and the acknowledgements have gone out,
		// so there is no HTTP error left to send. With a side band the client
		// gets the reason; without one it sees a truncated pack and says so
		// itself.
		info("git_upload_pack: encoding the packfile for %s failed: %v", repo_path, err)
		band.fail("packfile generation failed")
	}
	band.close()

	return true
}

// Sliding window the packfile encoder uses when looking for delta bases. 10 is
// go-git's own choice for a server-side pack.
const git_pack_window = 10

// How much packfile data goes out between progress messages.
const git_progress_interval = 1 << 20

// Largest fetch a thin pack is attempted for. Beyond this the fetch is
// effectively a clone, where the saving is proportionally small and the cost of
// holding two candidate packs in memory to compare them is not.
const git_thin_maximum = 50000

// Largest object a delta is computed for. Both sides are held in memory to
// diff them, so a pathological blob would otherwise decide how much memory a
// fetch takes.
const git_thin_object_maximum = 64 << 20

// Most a candidate pack may occupy in memory. Comparing thin against whole
// holds both at once, and the object ceiling bounds no bytes at all - one large
// blob is one object. A pack over this is streamed by the ordinary encoder
// instead. A variable rather than a constant so a test can lower it.
var git_pack_memory_maximum = 64 << 20

// git_pack_oversize reports a candidate pack that outgrew the memory allowed
// for comparing it. It is not a failure - the streamed encoder still has a
// correct answer - so callers fall back rather than fail the fetch.
var git_pack_oversize = errors.New("pack exceeds the memory allowed for comparing candidates")

// git_capped collects a pack in memory but refuses to grow past a limit, so a
// pack built only to be measured cannot decide how much memory a fetch takes.
type git_capped struct {
	buffer bytes.Buffer
	limit  int
}

func (c *git_capped) Write(p []byte) (int, error) {
	if c.buffer.Len()+len(p) > c.limit {
		return 0, git_pack_oversize
	}
	return c.buffer.Write(p)
}

// git_upload_pack_send encodes objects as a packfile and writes it to the band.
// For a thin pack two candidates are built and the smaller sent: thin deltifies
// against what the client already holds, the ordinary encoder deltifies within
// the pack, and neither wins every time. Oversize fetches skip the comparison.
func git_upload_pack_send(band *git_band, storage storer.Storer, selection *git_selection) error {
	if chosen := git_upload_pack_candidate(storage, selection); chosen != nil {
		written, err := band.send(bytes.NewReader(chosen))
		if err != nil {
			return err
		}
		band.message("Sending objects: %s, done.\n", git_bytes(written))
		return nil
	}

	reader, writer := io.Pipe()
	go func() {
		encoder := packfile.NewEncoder(writer, storage, false)
		_, err := encoder.Encode(selection.objects, git_pack_window)
		writer.CloseWithError(err)
	}()
	defer reader.Close()

	written, err := band.send(reader)
	if err != nil {
		return err
	}
	band.message("Sending objects: %s, done.\n", git_bytes(written))
	return nil
}

// git_upload_pack_candidate returns the pack to send when a thin one was worth
// building and both candidates fitted in memory to be compared, and nil when
// the streamed encoder is the answer.
func git_upload_pack_candidate(storage storer.Storer, selection *git_selection) []byte {
	thin := git_thin_candidate(storage, selection)
	if thin == nil {
		return nil
	}

	plain, err := git_pack_plain(storage, selection.objects)
	if err != nil {
		// An ordinary pack that outgrew the comparison limit is by definition
		// larger than the thin one, which did not - so the choice is already
		// made, and made in thin's favour. That is exactly the case thin-pack
		// exists for: a small edit to a large file.
		if errors.Is(err, git_pack_oversize) {
			debug("git_upload_pack_candidate: thin pack chosen, %d bytes against a whole pack over %d",
				len(thin), git_pack_memory_maximum)
			return thin
		}
		debug("git_upload_pack_candidate: building the whole pack failed: %v", err)
		return nil
	}

	if len(plain) <= len(thin) {
		debug("git_upload_pack_candidate: whole pack chosen, %d bytes against %d", len(plain), len(thin))
		return plain
	}
	debug("git_upload_pack_candidate: thin pack chosen, %d bytes against %d", len(thin), len(plain))
	return thin
}

// git_thin_candidate builds the thin pack for a selection, or returns nil when
// one is not worth having: the client did not ask, the fetch is too large, no
// object had a base on the client's side, or the build did not fit or failed.
// None of those is fatal - the ordinary encoder still has a correct answer.
func git_thin_candidate(storage storer.Storer, selection *git_selection) []byte {
	if !selection.thin || len(selection.objects) > git_thin_maximum || len(selection.known) == 0 {
		return nil
	}
	bases, err := git_thin_bases(storage, selection.objects, selection.known)
	if err != nil || len(bases) == 0 {
		if err != nil {
			debug("git_thin_candidate: choosing delta bases failed: %v", err)
		}
		return nil
	}
	pack, deltas, err := git_pack_thin(storage, selection.objects, bases)
	if err != nil {
		debug("git_thin_candidate: building the thin pack failed: %v", err)
		return nil
	}
	if deltas == 0 {
		return nil
	}
	return pack
}

// git_pack_plain encodes objects with go-git's encoder, which deltifies them
// against each other within a sliding window and never against anything the
// pack does not carry.
func git_pack_plain(storage storer.Storer, objects []plumbing.Hash) ([]byte, error) {
	capped := &git_capped{limit: git_pack_memory_maximum}
	encoder := packfile.NewEncoder(capped, storage, false)
	if _, err := encoder.Encode(objects, git_pack_window); err != nil {
		return nil, err
	}
	return capped.buffer.Bytes(), nil
}

// git_pack_thin writes objects as a packfile, sending any object with a chosen
// base as a delta against it. That base is not in the pack - the client
// resolves it from its own store, which is what makes the pack thin - so the
// entry has to be a REF_DELTA naming the base by hash. An OFS_DELTA names its
// base by offset within the pack and cannot reach outside it.
//
// Returns the pack and how many entries ended up deltified.
func git_pack_thin(storage storer.Storer, objects []plumbing.Hash, bases map[plumbing.Hash]plumbing.Hash) ([]byte, int, error) {
	capped := &git_capped{limit: git_pack_memory_maximum}
	// The trailing checksum covers every byte of the pack, so the digest runs
	// alongside the buffer rather than over it afterwards.
	digest := sha1.New()
	writer := io.MultiWriter(capped, digest)

	if _, err := writer.Write([]byte("PACK")); err != nil {
		return nil, 0, err
	}
	if err := binary.Write(writer, binary.BigEndian, uint32(2)); err != nil {
		return nil, 0, err
	}
	if err := binary.Write(writer, binary.BigEndian, uint32(len(objects))); err != nil {
		return nil, 0, err
	}

	deltas := 0
	compressor := zlib.NewWriter(writer)
	for _, hash := range objects {
		object, err := storage.EncodedObject(plumbing.AnyObject, hash)
		if err != nil {
			return nil, 0, err
		}

		payload := object
		kind := object.Type()
		base := plumbing.ZeroHash
		if candidate, ok := bases[hash]; ok {
			if delta, err := git_thin_delta(storage, candidate, object); err == nil && delta != nil {
				payload, kind, base = delta, plumbing.REFDeltaObject, candidate
				deltas++
			}
		}

		if err := git_pack_entry(writer, kind, payload.Size()); err != nil {
			return nil, 0, err
		}
		if !base.IsZero() {
			if _, err := writer.Write(base[:]); err != nil {
				return nil, 0, err
			}
		}

		content, err := payload.Reader()
		if err != nil {
			return nil, 0, err
		}
		compressor.Reset(writer)
		if _, err := io.Copy(compressor, content); err != nil {
			content.Close()
			return nil, 0, err
		}
		content.Close()
		if err := compressor.Close(); err != nil {
			return nil, 0, err
		}
	}

	if _, err := capped.Write(digest.Sum(nil)); err != nil {
		return nil, 0, err
	}
	return capped.buffer.Bytes(), deltas, nil
}

// git_thin_delta produces the delta from base to target, or nil when it is not
// worth sending one. A delta no smaller than the object it replaces is a loss
// twice over: a bigger entry, and work for the client to apply it.
func git_thin_delta(storage storer.Storer, base plumbing.Hash, target plumbing.EncodedObject) (plumbing.EncodedObject, error) {
	if target.Size() > git_thin_object_maximum {
		return nil, nil
	}
	source, err := storage.EncodedObject(plumbing.AnyObject, base)
	if err != nil {
		return nil, err
	}
	if source.Type() != target.Type() || source.Size() > git_thin_object_maximum {
		return nil, nil
	}
	delta, err := packfile.GetDelta(source, target)
	if err != nil {
		return nil, err
	}
	if delta.Size() >= target.Size() {
		return nil, nil
	}
	return delta, nil
}

// git_pack_entry writes an entry header: the object type and the uncompressed
// size of what follows, in git's variable-width encoding. The first byte holds
// the type and the low four bits of the size; each byte after it holds seven
// more size bits, least significant first, with the top bit set while more
// follow.
func git_pack_entry(writer io.Writer, kind plumbing.ObjectType, size int64) error {
	current := byte(kind)<<4 | byte(size&0x0f)
	size >>= 4
	header := []byte{}
	for size > 0 {
		header = append(header, current|0x80)
		current = byte(size & 0x7f)
		size >>= 7
	}
	header = append(header, current)
	_, err := writer.Write(header)
	return err
}

// git_thin_bases pairs each object about to be sent with an object the client
// already holds at the same path. Path is the signal git's own "preferred base"
// selection uses; matching by size or similarity would mean scanning the
// client's whole store per object.
func git_thin_bases(storage storer.Storer, objects []plumbing.Hash, known []plumbing.Hash) (map[plumbing.Hash]plumbing.Hash, error) {
	held := map[string]plumbing.Hash{}
	if err := git_walk_paths(storage, known, func(path string, hash plumbing.Hash) {
		if _, seen := held[path]; !seen {
			held[path] = hash
		}
	}); err != nil {
		return nil, err
	}
	if len(held) == 0 {
		return nil, nil
	}

	// The commits being sent are the roots of the trees being sent, so they
	// are where the paths of everything else in the pack come from.
	var roots []plumbing.Hash
	for _, hash := range objects {
		if object, err := storage.EncodedObject(plumbing.CommitObject, hash); err == nil && object != nil {
			roots = append(roots, hash)
		}
	}

	sending := git_hash_set(objects)
	bases := map[plumbing.Hash]plumbing.Hash{}
	if err := git_walk_paths(storage, roots, func(path string, hash plumbing.Hash) {
		if !sending[hash] {
			return
		}
		if _, chosen := bases[hash]; chosen {
			return
		}
		if base, ok := held[path]; ok && base != hash {
			bases[hash] = base
		}
	}); err != nil {
		return nil, err
	}
	return bases, nil
}

// git_walk_paths visits every path in the given commits' trees, root tree
// included, skipping submodule entries - those name a commit in another
// repository, which this one does not hold.
func git_walk_paths(storage storer.EncodedObjectStorer, commits []plumbing.Hash, visit func(path string, hash plumbing.Hash)) error {
	seen := map[plumbing.Hash]bool{}
	for _, hash := range commits {
		commit, err := object.GetCommit(storage, hash)
		if err != nil {
			continue
		}
		tree, err := commit.Tree()
		if err != nil {
			return err
		}
		visit("", tree.Hash)

		walker := object.NewTreeWalker(tree, true, seen)
		for {
			name, entry, err := walker.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				walker.Close()
				return err
			}
			if entry.Mode == filemode.Submodule {
				continue
			}
			visit(name, entry.Hash)
		}
		walker.Close()
	}
	return nil
}

// git_band writes the packfile section of an upload-pack response. Without a
// side band the packfile is simply the rest of the body; with one it is
// multiplexed into pkt-lines across pack, progress and error channels.
type git_band struct {
	writer   gin.ResponseWriter
	muxer    *sideband.Muxer // nil when the client asked for no side band
	target   io.Writer       // the muxer when there is one, the writer when there is not
	progress bool            // false when the client sent no-progress
}

// git_band_open picks the side band the client asked for, if any. A client only
// asks for what git_upload_pack_advertise offered, so an old client that knows
// only the 1000-byte form still gets one.
func git_band_open(writer gin.ResponseWriter, capabilities *capability.List) *git_band {
	band := &git_band{writer: writer, target: writer}
	switch {
	case capabilities.Supports(capability.Sideband64k):
		band.muxer = sideband.NewMuxer(sideband.Sideband64k, writer)
	case capabilities.Supports(capability.Sideband):
		band.muxer = sideband.NewMuxer(sideband.Sideband, writer)
	default:
		return band
	}
	band.target = band.muxer
	band.progress = !capabilities.Supports(capability.NoProgress)
	return band
}

// send copies the packfile onto the band, reporting how much has gone out as it
// goes. The report has to come from inside the copy loop: on a slow link the
// whole point is the line that arrives while the transfer is still running.
func (b *git_band) send(reader io.Reader) (int64, error) {
	var written, reported int64
	buffer := make([]byte, 32<<10)
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			if _, failure := b.target.Write(buffer[:n]); failure != nil {
				return written, failure
			}
			written += int64(n)
			if written-reported >= git_progress_interval {
				reported = written
				b.message("Sending objects: %s\r", git_bytes(written))
			}
		}
		if err == io.EOF {
			return written, nil
		}
		if err != nil {
			return written, err
		}
	}
}

// message reports progress. It is flushed immediately - a progress line whose
// whole purpose is to arrive before the thing it describes finishes must not
// sit in a buffer waiting for the pack to fill it.
func (b *git_band) message(format string, a ...any) {
	if b.muxer == nil || !b.progress {
		return
	}
	b.muxer.WriteChannel(sideband.ProgressMessage, []byte(fmt.Sprintf(format, a...)))
	b.writer.Flush()
}

// fail sends an error the client can print. It ignores no-progress: that
// suppresses commentary, not the explanation for a pack that stopped early.
// With no side band there is no channel for it and the client sees only a short
// read, which is what happened to every failure before the band existed.
func (b *git_band) fail(format string, a ...any) {
	if b.muxer == nil {
		return
	}
	b.muxer.WriteChannel(sideband.ErrorMessage, []byte(fmt.Sprintf(format, a...)))
	b.writer.Flush()
}

// close terminates a multiplexed response. The side band is a pkt-line stream
// and needs its flush packet to end; a raw packfile ends at the end of the body.
func (b *git_band) close() {
	if b.muxer == nil {
		return
	}
	pktline.NewEncoder(b.writer).Flush()
	b.writer.Flush()
}

// git_bytes renders a byte count for a progress message, in the units git's own
// progress output uses.
func git_bytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d bytes", n)
}

// git_receive_pack handles the git-receive-pack service (push). budget is the
// decoded object bytes the owner may still store; the session gets its own
// transport so that meter is per-push rather than shared.
func git_receive_pack(c *gin.Context, repo_path string, reader io.ReadCloser, budget int64) bool {
	ep := &transport.Endpoint{Path: repo_path}
	ctx := context.Background()

	// A budget of 0 would read as "unmetered", and a push with no room left
	// must store nothing at all: meter it at one byte, which every real object
	// exceeds.
	if budget <= 0 {
		budget = 1
	}
	session, err := server.NewServer(&git_loader{budget: budget}).NewReceivePackSession(ep, nil)
	if err != nil {
		info("git_receive_pack: failed to create session for %s: %v", repo_path, err)
		c.String(http.StatusInternalServerError, "Failed to create session") // i18n-ok: git protocol, read by the client not a person
		return true
	}
	defer session.Close()

	// Decode the reference update request from the client
	req := packp.NewReferenceUpdateRequest()
	if err := req.Decode(reader); err != nil {
		info("git_receive_pack: failed to decode request for %s: %v", repo_path, err)
		c.String(http.StatusBadRequest, "Failed to decode request: %v", err) // i18n-ok: git protocol, read by the client not a person
		return true
	}

	// Process the receive-pack request
	status, err := session.ReceivePack(ctx, req)
	if err != nil {
		info("git_receive_pack: %s: %v", repo_path, err)
	}

	// Always send the report status back to the client if available,
	// even on error — the git protocol requires it
	if status != nil {
		c.Status(http.StatusOK)
		c.Header("Content-Type", "application/x-git-receive-pack-result")
		c.Header("Cache-Control", "no-cache")
		if err := status.Encode(c.Writer); err != nil {
			info("git_receive_pack: failed to encode status: %v", err)
		}
		return true
	}

	// No status report at all — something went very wrong
	if err != nil {
		c.String(http.StatusInternalServerError, "Receive pack failed") // i18n-ok: git protocol, read by the client not a person
	}

	return true
}
