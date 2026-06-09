// Mochi server: admin snapshot / vacuum / backup handlers.
// Copyright Alistair Cunningham 2026
//
// Snapshot writes a `.backup` sibling next to every live `*.db` in the data
// dir using SQLite's online backup API (sqlite3_backup_init), so page offsets
// stay stable and rsync delta is tight. Static files are not touched — rsync
// transfers them from their live paths directly. The legacy `.snap` suffix
// from before the 2026-05-27 rename is still recognised by the
// reap/restore/tar paths so pre-existing on-disk files keep working.
//
// All of this is transport-agnostic. Only the snapshot lock is
// platform-specific (flock on Unix, LockFileEx on Windows); snapshot_acquire_
// lock / snapshot_release_lock live in the per-OS transport files.

package main

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const snapshot_lock_name = "snapshot.lock"

// snapshot_summary is the JSON returned by POST /_/admin/snapshot.
type snapshot_summary struct {
	Dbs    int      `json:"database_files"`
	Reaped int      `json:"stale_snapshots_reaped"`
	Bytes  int64    `json:"bytes_written"`
	Ms     int64    `json:"duration_ms"`
	Errors []string `json:"errors,omitempty"`
}

// snapshot_walk_dbs returns all live *.db file paths under root, skipping the
// run/ and cache/ top-level directories. Backup siblings (*.db.backup, the
// legacy *.db.snap, and their *.tmp partials) do not end in .db so the
// `.db` suffix match excludes them automatically.
func snapshot_walk_dbs(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p == root {
				return nil
			}
			base := filepath.Base(p)
			if filepath.Dir(p) == root && (base == "run" || base == "cache") {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".db") {
			return nil
		}
		paths = append(paths, p)
		return nil
	})
	return paths, err
}

// snapshot_walk_backups returns all *.db.backup file paths under root, plus
// any legacy *.db.snap files from before the 2026-05-27 suffix rename.
func snapshot_walk_backups(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p == root {
				return nil
			}
			base := filepath.Base(p)
			if filepath.Dir(p) == root && (base == "run" || base == "cache") {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if strings.HasSuffix(name, ".db.backup") || strings.HasSuffix(name, ".db.snap") {
			paths = append(paths, p)
		}
		return nil
	})
	return paths, err
}

// snapshot_trim_backup_suffix strips `.backup` or the legacy `.snap` suffix
// from name and returns the derived live filename plus whether anything was
// trimmed.
func snapshot_trim_backup_suffix(name string) (string, bool) {
	if v, ok := strings.CutSuffix(name, ".backup"); ok {
		return v, true
	}
	if v, ok := strings.CutSuffix(name, ".snap"); ok {
		return v, true
	}
	return name, false
}

// snapshot_copy_db lives in db_snapshot.go (cross-platform); the
// bootstrap protocol calls it on every platform too.

// snapshot_in_place writes a `.backup` sibling next to every live DB in the
// data dir, then reaps any stale `.backup` (or legacy `.snap`) whose live
// `.db` no longer exists. Acquires the snapshot lock for the duration of
// the call.
func snapshot_in_place() snapshot_summary {
	start := time.Now()
	out := snapshot_summary{}

	lock, err := snapshot_acquire_lock()
	if err != nil {
		out.Errors = append(out.Errors, err.Error())
		out.Ms = time.Since(start).Milliseconds()
		return out
	}
	defer snapshot_release_lock(lock)

	out = snapshot_in_place_locked()
	out.Ms = time.Since(start).Milliseconds()
	return out
}

// snapshot_in_place_locked does the actual snapshot work; the caller must
// already hold the snapshot lock. Used by admin_backup so the lock is held
// for both the snapshot phase and the tar streaming.
func snapshot_in_place_locked() snapshot_summary {
	out := snapshot_summary{}

	// Reap stale backups first so we know the dir is tidy before writing new ones.
	backups, err := snapshot_walk_backups(data_dir)
	if err != nil {
		out.Errors = append(out.Errors, fmt.Sprintf("walk backups: %v", err))
	}
	for _, backup := range backups {
		live, _ := snapshot_trim_backup_suffix(backup)
		if _, err := os.Stat(live); os.IsNotExist(err) {
			if err := os.Remove(backup); err == nil {
				out.Reaped++
			}
		}
	}

	dbs, err := snapshot_walk_dbs(data_dir)
	if err != nil {
		out.Errors = append(out.Errors, fmt.Sprintf("walk dbs: %v", err))
		return out
	}

	for _, src := range dbs {
		tmp := src + ".backup.tmp"
		final := src + ".backup"
		bytes, err := snapshot_copy_db(src, tmp)
		if err != nil {
			out.Errors = append(out.Errors, fmt.Sprintf("%s: %v", src, err))
			_ = os.Remove(tmp)
			continue
		}
		if err := os.Rename(tmp, final); err != nil {
			out.Errors = append(out.Errors, fmt.Sprintf("rename %s: %v", tmp, err))
			_ = os.Remove(tmp)
			continue
		}
		// Drop a legacy `.snap` sibling from before the rename so the tar
		// export does not ship two copies of the same DB.
		_ = os.Remove(src + ".snap")
		out.Dbs++
		out.Bytes += bytes
	}

	return out
}

// admin_snapshot is the POST /_/admin/snapshot handler. Runs in-place; static
// files are left in their live locations (rsync transfers them directly).
func admin_snapshot(c *gin.Context) {
	out := snapshot_in_place()
	status := http.StatusOK
	if len(out.Errors) > 0 && out.Dbs == 0 {
		status = http.StatusInternalServerError
	}
	c.JSON(status, out)
}

// admin_vacuum is the POST /_/admin/vacuum handler. Runs the reclaim pass
// over every currently-open database immediately - the same gate the
// periodic db_manager pass uses - instead of waiting for the next tick.
// Host-local: it compacts only this host's files and is not replicated.
func admin_vacuum(c *gin.Context) {
	start := time.Now()
	count, bytes := db_vacuum_all()
	c.JSON(http.StatusOK, gin.H{
		"databases_reclaimed": count,
		"bytes_reclaimed":     bytes,
		"duration_ms":         time.Since(start).Milliseconds(),
	})
}

// admin_backup is the GET /_/admin/backup handler. Refreshes the in-place
// `.backup` siblings, then streams a tar.gz of (snapshot DBs + live static
// files) to the response. Holds the snapshot lock throughout so the tar
// sees a consistent set of files.
func admin_backup(c *gin.Context) {
	lock, err := snapshot_acquire_lock()
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	defer snapshot_release_lock(lock)

	// Refresh .backup files in the data dir. They persist after the call —
	// useful for subsequent rsync workflows.
	summary := snapshot_in_place_locked()
	if summary.Dbs == 0 && len(summary.Errors) > 0 {
		c.JSON(http.StatusInternalServerError, summary)
		return
	}

	c.Header("Content-Type", "application/gzip")
	c.Header("Content-Disposition", `attachment; filename="mochi-backup.tar.gz"`)
	gz := gzip.NewWriter(c.Writer)
	tw := tar.NewWriter(gz)

	// Walk the data dir, including .backup files (rewritten to drop the
	// suffix in the tar) and static files. Legacy .snap siblings from
	// before the rename are handled the same way. Skip live DB sidecars,
	// in-flight temps, and ephemeral state.
	_ = filepath.WalkDir(data_dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if p == data_dir {
				return nil
			}
			base := filepath.Base(p)
			if filepath.Dir(p) == data_dir && (base == "run" || base == "cache") {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		switch {
		case strings.HasSuffix(name, ".backup") || strings.HasSuffix(name, ".snap"):
			// .db.backup (or legacy .db.snap) -> .db inside the archive,
			// so a restore lands the snapshot at the canonical path.
			rel, err := filepath.Rel(data_dir, p)
			if err != nil {
				return nil
			}
			rel, _ = snapshot_trim_backup_suffix(rel)
			if err := backup_tar_file(tw, p, rel); err != nil {
				warn("backup: tar %s: %v", rel, err)
			}
		case strings.HasSuffix(name, ".db"),
			strings.HasSuffix(name, ".db-wal"),
			strings.HasSuffix(name, ".db-shm"),
			strings.HasSuffix(name, ".db-journal"),
			strings.HasSuffix(name, ".backup.tmp"),
			strings.HasSuffix(name, ".snap.tmp"):
			// Live DB files and in-flight temps are excluded; .backup
			// siblings cover the data.
			return nil
		default:
			rel, err := filepath.Rel(data_dir, p)
			if err != nil {
				return nil
			}
			if err := backup_tar_file(tw, p, rel); err != nil {
				warn("backup: tar static %s: %v", rel, err)
			}
		}
		return nil
	})

	_ = tw.Close()
	_ = gz.Close()
}

// backup_tar_file appends one regular file to the tar writer using the
// given archive-relative name.
func backup_tar_file(tw *tar.Writer, src, rel string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	hdr, err := tar.FileInfoHeader(stat, "")
	if err != nil {
		return err
	}
	hdr.Name = rel
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err = io.Copy(tw, f)
	return err
}
