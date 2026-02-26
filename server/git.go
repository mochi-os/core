// Mochi server: Git repository operations
// Copyright Alistair Cunningham 2025

package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
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
	"diff": &gitDiffModule{},
	"merge": sls.FromStringDict(sl.String("mochi.git.merge"), sl.StringDict{
		"base":    sl.NewBuiltin("mochi.git.merge.base", api_git_merge_base),
		"check":   sl.NewBuiltin("mochi.git.merge.check", api_git_merge_check),
		"perform": sl.NewBuiltin("mochi.git.merge.perform", api_git_merge_perform),
	}),
})

// git_loader implements server.Loader to load repository storage from filesystem paths
type git_loader struct{}

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
	return &git_storage{filesystem.NewStorage(fs, cache.NewObjectLRUDefault())}, nil
}

// git_storage wraps storer.Storer to hide PackfileWriter, forcing the packfile
// parser to resolve thin pack delta references from the existing object store
type git_storage struct {
	storer.Storer
}

// git_transport is the go-git server transport for handling git protocol
var git_transport = server.NewServer(&git_loader{})

// gitDiffModule is a callable module that also has a .stats method
type gitDiffModule struct{}

func (m *gitDiffModule) String() string        { return "mochi.git.diff" }
func (m *gitDiffModule) Type() string          { return "module" }
func (m *gitDiffModule) Freeze()               {}
func (m *gitDiffModule) Truth() sl.Bool        { return sl.True }
func (m *gitDiffModule) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: module") }
func (m *gitDiffModule) Name() string          { return "mochi.git.diff" }
func (m *gitDiffModule) AttrNames() []string   { return []string{"stats"} }

func (m *gitDiffModule) Attr(name string) (sl.Value, error) {
	if name == "stats" {
		return sl.NewBuiltin("mochi.git.diff.stats", api_git_diff_stats), nil
	}
	return nil, nil
}

func (m *gitDiffModule) CallInternal(thread *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return api_git_diff(thread, nil, args, kwargs)
}

// Get the path to a repository for a given owner and entity ID
func git_repo_path(owner *User, entity_id string) string {
	return fmt.Sprintf("%s/users/%d/repositories/%s", data_dir, owner.ID, entity_id)
}

// Open a repository
func git_open(owner *User, entity_id string) (*git.Repository, error) {
	path := git_repo_path(owner, entity_id)
	return git.PlainOpen(path)
}

// Initialize a new bare repository
func git_init(owner *User, entity_id string) error {
	path := git_repo_path(owner, entity_id)

	// Create parent directory if needed
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	repo, err := git.PlainInit(path, true) // true = bare repository
	if err != nil {
		return err
	}

	// Create initial empty commit so the "main" branch ref exists
	empty_tree := &object.Tree{Entries: []object.TreeEntry{}}
	tree_obj := repo.Storer.NewEncodedObject()
	tree_obj.SetType(plumbing.TreeObject)
	if err := empty_tree.Encode(tree_obj); err != nil {
		return fmt.Errorf("failed to encode empty tree: %v", err)
	}
	tree_hash, err := repo.Storer.SetEncodedObject(tree_obj)
	if err != nil {
		return fmt.Errorf("failed to store empty tree: %v", err)
	}

	now := time.Now()
	sig := object.Signature{Name: "Mochi", Email: "mochi@localhost", When: now}
	commit := &object.Commit{
		Author:    sig,
		Committer: sig,
		Message:   "Initial commit\n",
		TreeHash:  tree_hash,
	}
	commit_obj := repo.Storer.NewEncodedObject()
	commit_obj.SetType(plumbing.CommitObject)
	if err := commit.Encode(commit_obj); err != nil {
		return fmt.Errorf("failed to encode initial commit: %v", err)
	}
	commit_hash, err := repo.Storer.SetEncodedObject(commit_obj)
	if err != nil {
		return fmt.Errorf("failed to store initial commit: %v", err)
	}

	// Set refs/heads/main to the initial commit
	ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), commit_hash)
	if err := repo.Storer.SetReference(ref); err != nil {
		return fmt.Errorf("failed to set main branch: %v", err)
	}

	// Set HEAD to point to main
	head := plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))
	return repo.Storer.SetReference(head)
}

// Delete a repository
func git_delete(owner *User, entity_id string) error {
	path := git_repo_path(owner, entity_id)
	return os.RemoveAll(path)
}

// Get repository size in bytes
func git_size(owner *User, entity_id string) (int64, error) {
	path := git_repo_path(owner, entity_id)
	var size int64

	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})

	return size, err
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

	// Try as a branch
	branch_ref, err := repo.Reference(plumbing.NewBranchReferenceName(ref), true)
	if err == nil {
		hash := branch_ref.Hash()
		return &hash, nil
	}

	// Try as a tag
	tag_ref, err := repo.Reference(plumbing.NewTagReferenceName(ref), true)
	if err == nil {
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

	// Try as a commit hash
	if len(ref) >= 4 {
		hash := plumbing.NewHash(ref)
		if !hash.IsZero() {
			return &hash, nil
		}
	}

	return nil, fmt.Errorf("cannot resolve ref %q", ref)
}

// mochi.git.init(entity_id) -> bool: Initialize a bare git repository
func api_git_init(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity_id: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	err := git_init(owner, entity_id)
	if err != nil {
		return sl_error(fn, "failed to initialize repository: %v", err)
	}

	return sl.True, nil
}

// mochi.git.delete(entity_id) -> bool: Delete a git repository
func api_git_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity_id: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	err := git_delete(owner, entity_id)
	if err != nil {
		return sl_error(fn, "failed to delete repository: %v", err)
	}

	return sl.True, nil
}

// mochi.git.path(entity_id) -> string: Get the filesystem path to a repository
func api_git_path(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity_id: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	return sl.String(git_repo_path(owner, entity_id)), nil
}

// mochi.git.size(entity_id) -> int: Get repository size in bytes
func api_git_size(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity_id: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	size, err := git_size(owner, entity_id)
	if err != nil {
		return sl_error(fn, "failed to get size: %v", err)
	}

	return sl.MakeInt64(size), nil
}

// mochi.git.refs(entity_id) -> list: List all refs (branches and tags)
func api_git_refs(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity_id: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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
		refType := "unknown"
		shortName := name

		if ref.Name().IsBranch() {
			refType = "branch"
			shortName = ref.Name().Short()
		} else if ref.Name().IsTag() {
			refType = "tag"
			shortName = ref.Name().Short()
		} else if ref.Name().IsRemote() {
			refType = "remote"
			shortName = ref.Name().Short()
		}

		refs = append(refs, map[string]any{
			"name": shortName,
			"full": name,
			"type": refType,
			"sha":  ref.Hash().String(),
		})
		return nil
	})

	if err != nil {
		return sl_error(fn, "failed to iterate refs: %v", err)
	}

	return sl_encode(refs), nil
}

// mochi.git.branches(entity_id) -> list: List branches
func api_git_branches(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity_id: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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

// mochi.git.tags(entity_id) -> list: List tags
func api_git_tags(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity_id: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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

// mochi.git.branch.create(entity_id, name, ref) -> bool: Create a new branch
func api_git_branch_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity_id: string>, <name: string>, <ref: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	name, ok := sl.AsString(args[1])
	if !ok || name == "" {
		return sl_error(fn, "invalid branch name")
	}

	ref, ok := sl.AsString(args[2])
	if !ok {
		return sl_error(fn, "invalid ref")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	hash, err := git_resolve_ref(repo, ref)
	if err != nil {
		return sl_error(fn, "failed to resolve ref: %v", err)
	}

	branch_ref := plumbing.NewBranchReferenceName(name)
	newRef := plumbing.NewHashReference(branch_ref, *hash)
	err = repo.Storer.SetReference(newRef)
	if err != nil {
		return sl_error(fn, "failed to create branch: %v", err)
	}

	return sl.True, nil
}

// mochi.git.branch.delete(entity_id, name) -> bool: Delete a branch
func api_git_branch_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <entity_id: string>, <name: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	name, ok := sl.AsString(args[1])
	if !ok || name == "" {
		return sl_error(fn, "invalid branch name")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	branch_ref := plumbing.NewBranchReferenceName(name)
	err = repo.Storer.RemoveReference(branch_ref)
	if err != nil {
		return sl_error(fn, "failed to delete branch: %v", err)
	}

	return sl.True, nil
}

// mochi.git.branch.default.get(entity_id) -> string: Get default branch name
func api_git_branch_default_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <entity_id: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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

// mochi.git.branch.default.set(entity_id, name) -> bool: Set default branch
func api_git_branch_default_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <entity_id: string>, <name: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	name, ok := sl.AsString(args[1])
	if !ok || name == "" {
		return sl_error(fn, "invalid branch name")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
	if err != nil {
		return sl_error(fn, "failed to open repository: %v", err)
	}

	// Verify the branch exists
	branch_ref, err := repo.Reference(plumbing.NewBranchReferenceName(name), true)
	if err != nil {
		return sl_error(fn, "branch %q does not exist", name)
	}

	// Set HEAD to point to the branch
	headRef := plumbing.NewSymbolicReference(plumbing.HEAD, branch_ref.Name())
	err = repo.Storer.SetReference(headRef)
	if err != nil {
		return sl_error(fn, "failed to set default branch: %v", err)
	}

	return sl.True, nil
}

// mochi.git.commit.list(entity_id, ref, limit, offset) -> list: List commits
func api_git_commit_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 4 {
		return sl_error(fn, "syntax: <entity_id: string>, [ref: string], [limit: int], [offset: int]")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
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

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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

// mochi.git.commit.get(entity_id, sha) -> dict: Get a single commit
func api_git_commit_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <entity_id: string>, <sha: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	sha, ok := sl.AsString(args[1])
	if !ok || sha == "" {
		return sl_error(fn, "invalid sha")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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

// mochi.git.commit.log(entity_id, ref, path, limit) -> list: Commits affecting a path
func api_git_commit_log(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 || len(args) > 4 {
		return sl_error(fn, "syntax: <entity_id: string>, <ref: string>, <path: string>, [limit: int]")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
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

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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

// mochi.git.commit.between(entity_id, base, head) -> list: Commits between refs
func api_git_commit_between(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity_id: string>, <base: string>, <head: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	base, ok := sl.AsString(args[1])
	if !ok || base == "" {
		return sl_error(fn, "invalid base")
	}

	head, ok := sl.AsString(args[2])
	if !ok || head == "" {
		return sl_error(fn, "invalid head")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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
	baseAncestors := make(map[plumbing.Hash]bool)
	baseIter := object.NewCommitIterCTime(base_commit, nil, nil)
	baseIter.ForEach(func(c *object.Commit) error {
		baseAncestors[c.Hash] = true
		return nil
	})

	var commits []map[string]any
	headIter := object.NewCommitIterCTime(head_commit, nil, nil)
	headIter.ForEach(func(c *object.Commit) error {
		if !baseAncestors[c.Hash] {
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

// mochi.git.tree(entity_id, ref, path) -> list: List directory contents
func api_git_tree(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 3 {
		return sl_error(fn, "syntax: <entity_id: string>, [ref: string], [path: string]")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	ref := "HEAD"
	if len(args) > 1 && args[1] != sl.None {
		ref, _ = sl.AsString(args[1])
	}

	path := ""
	if len(args) > 2 && args[2] != sl.None {
		path, _ = sl.AsString(args[2])
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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
		entryType := "file"
		if entry.Mode == filemode.Dir {
			entryType = "dir"
		} else if entry.Mode == filemode.Submodule {
			entryType = "submodule"
		} else if entry.Mode == filemode.Symlink {
			entryType = "symlink"
		}

		e := map[string]any{
			"name": entry.Name,
			"type": entryType,
			"sha":  entry.Hash.String(),
			"mode": fmt.Sprintf("%o", entry.Mode),
		}

		// Get size for files
		if entryType == "file" {
			blob, err := repo.BlobObject(entry.Hash)
			if err == nil {
				e["size"] = blob.Size
			}
		}

		entries = append(entries, e)
	}

	// Sort: directories first, then alphabetically
	sort.Slice(entries, func(i, j int) bool {
		iDir := entries[i]["type"] == "dir"
		jDir := entries[j]["type"] == "dir"
		if iDir != jDir {
			return iDir
		}
		return entries[i]["name"].(string) < entries[j]["name"].(string)
	})

	return sl_encode(entries), nil
}

// mochi.git.blob.content(entity_id, ref, path) -> string: Get file contents
func api_git_blob_content(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity_id: string>, <ref: string>, <path: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	ref, ok := sl.AsString(args[1])
	if !ok {
		ref = "HEAD"
	}

	path, ok := sl.AsString(args[2])
	if !ok || path == "" {
		return sl_error(fn, "invalid path")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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

// mochi.git.blob.get(entity_id, ref, path) -> dict: Get file metadata
func api_git_blob_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity_id: string>, <ref: string>, <path: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	ref, ok := sl.AsString(args[1])
	if !ok {
		ref = "HEAD"
	}

	path, ok := sl.AsString(args[2])
	if !ok || path == "" {
		return sl_error(fn, "invalid path")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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

// mochi.git.diff(entity_id, base, head) -> string: Get unified diff
func api_git_diff(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity_id: string>, <base: string>, <head: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	base, ok := sl.AsString(args[1])
	if !ok || base == "" {
		return sl_error(fn, "invalid base")
	}

	head, ok := sl.AsString(args[2])
	if !ok || head == "" {
		return sl_error(fn, "invalid head")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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

// mochi.git.diff.stats(entity_id, base, head) -> dict: Get diff statistics
func api_git_diff_stats(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity_id: string>, <base: string>, <head: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	base, ok := sl.AsString(args[1])
	if !ok || base == "" {
		return sl_error(fn, "invalid base")
	}

	head, ok := sl.AsString(args[2])
	if !ok || head == "" {
		return sl_error(fn, "invalid head")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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

// mochi.git.merge.base(entity_id, ref1, ref2) -> string: Find common ancestor
func api_git_merge_base(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity_id: string>, <ref1: string>, <ref2: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	ref1, ok := sl.AsString(args[1])
	if !ok || ref1 == "" {
		return sl_error(fn, "invalid ref1")
	}

	ref2, ok := sl.AsString(args[2])
	if !ok || ref2 == "" {
		return sl_error(fn, "invalid ref2")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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

// mochi.git.merge.check(entity_id, source, target) -> dict: Check if merge is possible
func api_git_merge_check(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <entity_id: string>, <source: string>, <target: string>")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
	}

	source, ok := sl.AsString(args[1])
	if !ok || source == "" {
		return sl_error(fn, "invalid source")
	}

	target, ok := sl.AsString(args[2])
	if !ok || target == "" {
		return sl_error(fn, "invalid target")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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

// mochi.git.merge.perform(entity_id, source, target, message, author_name, author_email) -> dict: Perform a merge
func api_git_merge_perform(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 6 || len(args) > 7 {
		return sl_error(fn, "syntax: <entity_id: string>, <source: string>, <target: string>, <message: string>, <author_name: string>, <author_email: string>, [method: string]")
	}

	entity_id, ok := sl.AsString(args[0])
	if !ok || entity_id == "" {
		return sl_error(fn, "invalid entity_id")
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

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	repo, err := git_open(owner, entity_id)
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
		target_ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName(target), *source_hash)
		err = repo.Storer.SetReference(target_ref)
		if err != nil {
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
		target_ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName(target), commit_hash)
		err = repo.Storer.SetReference(target_ref)
		if err != nil {
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
		target_ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName(target), commit_hash)
		err = repo.Storer.SetReference(target_ref)
		if err != nil {
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
	target_ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName(target), commit_hash)
	if err := repo.Storer.SetReference(target_ref); err != nil {
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
	target_ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName(target), current_parent)
	if err := repo.Storer.SetReference(target_ref); err != nil {
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

// git_http_handler handles the Smart HTTP protocol for git clone/push/fetch
// Path format: /info/refs, /git-upload-pack, /git-receive-pack
func git_http_handler(c *gin.Context, a *App, owner *User, user *User, repo string, path string) bool {
	if owner == nil {
		c.String(http.StatusNotFound, "Repository not found")
		return true
	}

	// Find repository entity by fingerprint for this owner
	// The repo parameter is the entity fingerprint extracted from the URL
	db := db_open("db/users.db")
	row, err := db.row("select id from entities where user = ? and fingerprint = ?", owner.ID, repo)
	if err != nil || row == nil {
		c.String(http.StatusNotFound, "Repository not found")
		return true
	}
	id, ok := row["id"].(string)
	if !ok || id == "" {
		c.String(http.StatusNotFound, "Repository not found")
		return true
	}

	// Build repository path
	repo_path := git_repo_path(owner, id)
	if _, err := os.Stat(repo_path); os.IsNotExist(err) {
		c.String(http.StatusNotFound, "Repository not found")
		return true
	}

	// Determine operation
	service := c.Query("service")
	if service == "" {
		if strings.HasSuffix(path, "git-upload-pack") {
			service = "git-upload-pack"
		} else if strings.HasSuffix(path, "git-receive-pack") {
			service = "git-receive-pack"
		}
	}

	// Determine if this is a read or write operation
	is_write := service == "git-receive-pack"

	// For write operations, require authentication
	if is_write {
		if user == nil {
			// Try Basic Auth with token
			user = git_authenticate(c, a)
			if user == nil {
				c.Header("WWW-Authenticate", `Basic realm="Mochi Git"`)
				c.String(http.StatusUnauthorized, "Authentication required")
				return true
			}
		}

		// Check write access using the app's access control system
		identity := user.identity()
		if identity == nil {
			c.String(http.StatusForbidden, "No write access to repository")
			return true
		}
		app_db := db_app_system(owner, a)
		if app_db == nil || !app_db.access_check(owner, identity.ID, user.Role, "repo/"+id, "write") {
			c.String(http.StatusForbidden, "No write access to repository")
			return true
		}
	}

	// Route to appropriate handler
	if strings.HasSuffix(path, "info/refs") {
		return git_info_refs(c, repo_path, service)
	} else if strings.HasSuffix(path, "git-upload-pack") {
		return git_service_rpc(c, repo_path, "git-upload-pack")
	} else if strings.HasSuffix(path, "git-receive-pack") {
		return git_service_rpc(c, repo_path, "git-receive-pack")
	}

	c.String(http.StatusNotFound, "Not found")
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

	return user_by_id(token.User)
}

// git_info_refs handles GET /info/refs?service=git-upload-pack|git-receive-pack
func git_info_refs(c *gin.Context, repo_path string, service string) bool {
	if service != "git-upload-pack" && service != "git-receive-pack" {
		c.String(http.StatusForbidden, "Service not enabled")
		return true
	}

	// Create endpoint for the repository path
	ep := &transport.Endpoint{Path: repo_path}
	ctx := context.Background()

	// Create appropriate session based on service and get advertised refs
	var refs *packp.AdvRefs
	if service == "git-upload-pack" {
		session, err := git_transport.NewUploadPackSession(ep, nil)
		if err != nil {
			c.String(http.StatusInternalServerError, "Failed to create session: %v", err)
			return true
		}
		defer session.Close()
		refs, err = session.AdvertisedReferencesContext(ctx)
		if err != nil {
			c.String(http.StatusInternalServerError, "Failed to get refs: %v", err)
			return true
		}
	} else {
		session, err := git_transport.NewReceivePackSession(ep, nil)
		if err != nil {
			c.String(http.StatusInternalServerError, "Failed to create session: %v", err)
			return true
		}
		defer session.Close()
		refs, err = session.AdvertisedReferencesContext(ctx)
		if err != nil {
			c.String(http.StatusInternalServerError, "Failed to get refs: %v", err)
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

// git_service_rpc handles POST /git-upload-pack and /git-receive-pack
func git_service_rpc(c *gin.Context, repo_path string, service string) bool {
	// Handle gzip compressed request body
	var reader io.ReadCloser = c.Request.Body
	if c.GetHeader("Content-Encoding") == "gzip" {
		gz_reader, err := gzip.NewReader(c.Request.Body)
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid gzip data")
			return true
		}
		defer gz_reader.Close()
		reader = gz_reader
	}

	if service == "git-upload-pack" {
		return git_upload_pack(c, repo_path, reader)
	}
	return git_receive_pack(c, repo_path, reader)
}

// git_upload_pack handles the git-upload-pack service (fetch/clone)
func git_upload_pack(c *gin.Context, repo_path string, reader io.ReadCloser) bool {
	ep := &transport.Endpoint{Path: repo_path}
	ctx := context.Background()

	session, err := git_transport.NewUploadPackSession(ep, nil)
	if err != nil {
		c.String(http.StatusInternalServerError, "Failed to create session: %v", err)
		return true
	}
	defer session.Close()

	// Decode the upload-pack request from the client
	req := packp.NewUploadPackRequest()
	if err := req.Decode(reader); err != nil {
		c.String(http.StatusBadRequest, "Failed to decode request: %v", err)
		return true
	}

	// Process the upload-pack request
	resp, err := session.UploadPack(ctx, req)
	if err != nil {
		c.String(http.StatusInternalServerError, "Upload pack failed: %v", err)
		return true
	}
	defer resp.Close()

	c.Status(http.StatusOK)
	c.Header("Content-Type", "application/x-git-upload-pack-result")
	c.Header("Cache-Control", "no-cache")

	// Encode the response
	if err := resp.Encode(c.Writer); err != nil {
		info("git_upload_pack: failed to encode response: %v", err)
	}

	return true
}

// git_receive_pack handles the git-receive-pack service (push)
func git_receive_pack(c *gin.Context, repo_path string, reader io.ReadCloser) bool {
	ep := &transport.Endpoint{Path: repo_path}
	ctx := context.Background()

	session, err := git_transport.NewReceivePackSession(ep, nil)
	if err != nil {
		info("git_receive_pack: failed to create session for %s: %v", repo_path, err)
		c.String(http.StatusInternalServerError, "Failed to create session: %v", err)
		return true
	}
	defer session.Close()

	// Decode the reference update request from the client
	req := packp.NewReferenceUpdateRequest()
	if err := req.Decode(reader); err != nil {
		info("git_receive_pack: failed to decode request for %s: %v", repo_path, err)
		c.String(http.StatusBadRequest, "Failed to decode request: %v", err)
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
		c.String(http.StatusInternalServerError, "Receive pack failed: %v", err)
	}

	return true
}
