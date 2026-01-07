// Mochi server: Git repository operations
// Copyright Alistair Cunningham 2025

package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
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
		"base":  sl.NewBuiltin("mochi.git.merge.base", api_git_merge_base),
		"check": sl.NewBuiltin("mochi.git.merge.check", api_git_merge_check),
	}),
})

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

	_, err := git.PlainInit(path, true) // true = bare repository
	return err
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
			branchRef, err := repo.Reference(plumbing.NewBranchReferenceName(branch), true)
			if err == nil {
				hash := branchRef.Hash()
				return &hash, nil
			}
		}
		return nil, err
	}

	// Try as a branch
	branchRef, err := repo.Reference(plumbing.NewBranchReferenceName(ref), true)
	if err == nil {
		hash := branchRef.Hash()
		return &hash, nil
	}

	// Try as a tag
	tagRef, err := repo.Reference(plumbing.NewTagReferenceName(ref), true)
	if err == nil {
		// For annotated tags, dereference to get the commit hash
		tagObj, err := repo.TagObject(tagRef.Hash())
		if err == nil {
			// Annotated tag - get the commit it points to
			commit, err := tagObj.Commit()
			if err == nil {
				hash := commit.Hash
				return &hash, nil
			}
		}
		// Lightweight tag or failed to dereference - use tag hash directly
		hash := tagRef.Hash()
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
		tagObj, err := repo.TagObject(ref.Hash())
		if err == nil {
			tag["message"] = strings.TrimSpace(tagObj.Message)
			tag["tagger"] = tagObj.Tagger.Name
			tag["date"] = tagObj.Tagger.When.Unix()
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

	branchRef := plumbing.NewBranchReferenceName(name)
	newRef := plumbing.NewHashReference(branchRef, *hash)
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

	branchRef := plumbing.NewBranchReferenceName(name)
	err = repo.Storer.RemoveReference(branchRef)
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
	branchRef, err := repo.Reference(plumbing.NewBranchReferenceName(name), true)
	if err != nil {
		return sl_error(fn, "branch %q does not exist", name)
	}

	// Set HEAD to point to the branch
	headRef := plumbing.NewSymbolicReference(plumbing.HEAD, branchRef.Name())
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

	baseHash, err := git_resolve_ref(repo, base)
	if err != nil {
		return sl.None, nil // base ref not found
	}

	headHash, err := git_resolve_ref(repo, head)
	if err != nil {
		return sl.None, nil // head ref not found
	}

	// Get commits reachable from head
	headCommit, err := repo.CommitObject(*headHash)
	if err != nil {
		return sl.None, nil // head commit not found
	}

	baseCommit, err := repo.CommitObject(*baseHash)
	if err != nil {
		return sl.None, nil // base commit not found
	}

	// Find commits in head not in base
	baseAncestors := make(map[plumbing.Hash]bool)
	baseIter := object.NewCommitIterCTime(baseCommit, nil, nil)
	baseIter.ForEach(func(c *object.Commit) error {
		baseAncestors[c.Hash] = true
		return nil
	})

	var commits []map[string]any
	headIter := object.NewCommitIterCTime(headCommit, nil, nil)
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

	baseHash, err := git_resolve_ref(repo, base)
	if err != nil {
		return sl_error(fn, "failed to resolve base: %v", err)
	}

	headHash, err := git_resolve_ref(repo, head)
	if err != nil {
		return sl_error(fn, "failed to resolve head: %v", err)
	}

	baseCommit, err := repo.CommitObject(*baseHash)
	if err != nil {
		return sl_error(fn, "failed to get base commit: %v", err)
	}

	headCommit, err := repo.CommitObject(*headHash)
	if err != nil {
		return sl_error(fn, "failed to get head commit: %v", err)
	}

	baseTree, err := baseCommit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get base tree: %v", err)
	}

	headTree, err := headCommit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get head tree: %v", err)
	}

	changes, err := baseTree.Diff(headTree)
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

	baseHash, err := git_resolve_ref(repo, base)
	if err != nil {
		return sl_error(fn, "failed to resolve base: %v", err)
	}

	headHash, err := git_resolve_ref(repo, head)
	if err != nil {
		return sl_error(fn, "failed to resolve head: %v", err)
	}

	baseCommit, err := repo.CommitObject(*baseHash)
	if err != nil {
		return sl_error(fn, "failed to get base commit: %v", err)
	}

	headCommit, err := repo.CommitObject(*headHash)
	if err != nil {
		return sl_error(fn, "failed to get head commit: %v", err)
	}

	baseTree, err := baseCommit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get base tree: %v", err)
	}

	headTree, err := headCommit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get head tree: %v", err)
	}

	changes, err := baseTree.Diff(headTree)
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

	sourceHash, err := git_resolve_ref(repo, source)
	if err != nil {
		return sl_error(fn, "failed to resolve source: %v", err)
	}

	targetHash, err := git_resolve_ref(repo, target)
	if err != nil {
		return sl_error(fn, "failed to resolve target: %v", err)
	}

	sourceCommit, err := repo.CommitObject(*sourceHash)
	if err != nil {
		return sl_error(fn, "failed to get source commit: %v", err)
	}

	targetCommit, err := repo.CommitObject(*targetHash)
	if err != nil {
		return sl_error(fn, "failed to get target commit: %v", err)
	}

	// Find merge base
	bases, err := sourceCommit.MergeBase(targetCommit)
	if err != nil || len(bases) == 0 {
		return sl_encode(map[string]any{
			"mergeable": false,
			"conflicts": []string{},
			"error":     "no common ancestor",
		}), nil
	}

	// Check for conflicts by comparing trees
	sourceTree, err := sourceCommit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get source tree: %v", err)
	}

	targetTree, err := targetCommit.Tree()
	if err != nil {
		return sl_error(fn, "failed to get target tree: %v", err)
	}

	baseTree, err := bases[0].Tree()
	if err != nil {
		return sl_error(fn, "failed to get base tree: %v", err)
	}

	// Get changes from base to source and base to target
	sourceChanges, _ := baseTree.Diff(sourceTree)
	targetChanges, _ := baseTree.Diff(targetTree)

	// Check for overlapping changes (potential conflicts)
	sourceFiles := make(map[string]bool)
	for _, change := range sourceChanges {
		from, to, _ := change.Files()
		if from != nil {
			sourceFiles[from.Name] = true
		}
		if to != nil {
			sourceFiles[to.Name] = true
		}
	}

	var conflicts []string
	for _, change := range targetChanges {
		from, to, _ := change.Files()
		var name string
		if from != nil {
			name = from.Name
		} else if to != nil {
			name = to.Name
		}
		if name != "" && sourceFiles[name] {
			conflicts = append(conflicts, name)
		}
	}

	return sl_encode(map[string]any{
		"mergeable": len(conflicts) == 0,
		"conflicts": conflicts,
		"base":      bases[0].Hash.String(),
	}), nil
}

// Unused but kept for potential future use
var _ = config.Config{}
var _ = time.Now

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
	repoID, ok := row["id"].(string)
	if !ok || repoID == "" {
		c.String(http.StatusNotFound, "Repository not found")
		return true
	}

	// Build repository path
	repoPath := git_repo_path(owner, repoID)
	if _, err := os.Stat(repoPath); os.IsNotExist(err) {
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
	isWrite := service == "git-receive-pack"

	// For write operations, require authentication
	if isWrite {
		if user == nil {
			// Try Basic Auth with token
			user = git_authenticate(c)
			if user == nil {
				c.Header("WWW-Authenticate", `Basic realm="Mochi Git"`)
				c.String(http.StatusUnauthorized, "Authentication required")
				return true
			}
		}

		// Check write access
		// For now, only the owner can push
		if user.ID != owner.ID {
			c.String(http.StatusForbidden, "No write access to repository")
			return true
		}
	}

	// Route to appropriate handler
	if strings.HasSuffix(path, "info/refs") {
		return git_info_refs(c, repoPath, service)
	} else if strings.HasSuffix(path, "git-upload-pack") {
		return git_service_rpc(c, repoPath, "git-upload-pack")
	} else if strings.HasSuffix(path, "git-receive-pack") {
		return git_service_rpc(c, repoPath, "git-receive-pack")
	}

	c.String(http.StatusNotFound, "Not found")
	return true
}

// git_authenticate extracts and validates Basic Auth credentials from the request
func git_authenticate(c *gin.Context) *User {
	username, password, ok := c.Request.BasicAuth()
	if !ok {
		return nil
	}

	// For git authentication, we accept:
	// - username: anything (often the git username or "x-access-token")
	// - password: a valid mochi token

	// Validate token
	db := db_open("db/users.db")
	hash := token_hash(password)

	row, err := db.row("select user from tokens where hash = ?", hash)
	if err != nil || row == nil {
		// Token not found
		_ = username // Silence unused variable warning
		return nil
	}
	userID, ok := row["user"].(int64)
	if !ok {
		return nil
	}

	// Update last_used
	db.exec("update tokens set last_used = ? where hash = ?", time.Now().Format("2006-01-02 15:04:05"), hash)

	return user_by_id(int(userID))
}

// git_info_refs handles GET /info/refs?service=git-upload-pack|git-receive-pack
func git_info_refs(c *gin.Context, repoPath string, service string) bool {
	if service != "git-upload-pack" && service != "git-receive-pack" {
		c.String(http.StatusForbidden, "Service not enabled")
		return true
	}

	c.Status(http.StatusOK)
	c.Header("Content-Type", fmt.Sprintf("application/x-%s-advertisement", service))
	c.Header("Cache-Control", "no-cache")

	// Git protocol: first send a packet-line with the service name
	gitService := fmt.Sprintf("# service=%s\n", service)
	pktLine := fmt.Sprintf("%04x%s0000", len(gitService)+4, gitService)
	c.Writer.WriteString(pktLine)

	// Run the git command to get refs
	cmd := exec.Command(service, "--stateless-rpc", "--advertise-refs", repoPath)
	cmd.Stdout = c.Writer
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		// Already started writing, can't send error status
		info("git_info_refs: %s failed: %v", service, err)
	}

	return true
}

// git_service_rpc handles POST /git-upload-pack and /git-receive-pack
func git_service_rpc(c *gin.Context, repoPath string, service string) bool {
	c.Status(http.StatusOK)
	c.Header("Content-Type", fmt.Sprintf("application/x-%s-result", service))
	c.Header("Cache-Control", "no-cache")

	// Handle gzip compressed request body
	var reader io.Reader = c.Request.Body
	if c.GetHeader("Content-Encoding") == "gzip" {
		gzReader, err := gzip.NewReader(c.Request.Body)
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid gzip data")
			return true
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Run the git command
	cmd := exec.Command(service, "--stateless-rpc", repoPath)
	cmd.Stdin = reader
	cmd.Stdout = c.Writer
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		info("git_service_rpc: %s failed: %v", service, err)
	}

	return true
}
