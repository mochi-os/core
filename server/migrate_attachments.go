// Mochi: Attachment migration script
// Migrates attachments from old standalone attachments.db to new per-app _attachments tables
//
// Usage: go run server/*.go -migrate-attachments
//
// This script:
// 1. Reads attachments from the old attachments/attachments.db
// 2. Determines target app from object path (chat/*, forums/*, feeds/*, etc.)
// 3. Inserts records into each app's _attachments table
// 4. Copies files from old location to new location

package main

import (
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"
)

// Old attachment structure
type OldAttachment struct {
	Entity  string `db:"entity"`
	ID      string `db:"id"`
	Object  string `db:"object"`
	Rank    int    `db:"rank"`
	Name    string `db:"name"`
	Path    string `db:"path"`
	Size    int64  `db:"size"`
	Created int64  `db:"created"`
}

// Migrate attachments from old system to new per-app system
func migrate_attachments() {
	info("Starting attachment migration...")

	// Find all user directories
	users_dir := data_dir + "/users"
	entries, err := os.ReadDir(users_dir)
	if err != nil {
		info("Migration: cannot read users directory: %v", err)
		return
	}

	total_migrated := 0
	total_skipped := 0
	total_errors := 0

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		user_id := entry.Name()
		old_db_path := fmt.Sprintf("%s/%s/attachments/attachments.db", users_dir, user_id)

		if !file_exists(old_db_path) {
			continue
		}

		info("Migration: processing user %s", user_id)
		migrated, skipped, errors := migrate_user_attachments(user_id, old_db_path)
		total_migrated += migrated
		total_skipped += skipped
		total_errors += errors
	}

	info("Migration complete: %d migrated, %d skipped, %d errors", total_migrated, total_skipped, total_errors)
}

func migrate_user_attachments(user_id string, old_db_path string) (migrated, skipped, errors int) {
	// Open old database
	old_db := db_open(old_db_path)
	if old_db == nil {
		info("Migration: cannot open old database for user %s", user_id)
		return 0, 0, 1
	}
	defer old_db.close()

	// Get all attachments
	var attachments []OldAttachment
	old_db.scans(&attachments, "select * from attachments order by object, rank")

	if len(attachments) == 0 {
		info("Migration: no attachments for user %s", user_id)
		return 0, 0, 0
	}

	info("Migration: found %d attachments for user %s", len(attachments), user_id)

	// Group by target app
	app_attachments := make(map[string][]OldAttachment)
	for _, att := range attachments {
		app_name := get_app_from_object(att.Object)
		if app_name == "" {
			info("Migration: cannot determine app for object %q, skipping", att.Object)
			skipped++
			continue
		}
		app_attachments[app_name] = append(app_attachments[app_name], att)
	}

	// Process each app
	for app_name, atts := range app_attachments {
		m, s, e := migrate_app_attachments(user_id, app_name, atts)
		migrated += m
		skipped += s
		errors += e
	}

	return migrated, skipped, errors
}

func migrate_app_attachments(user_id string, app_name string, attachments []OldAttachment) (migrated, skipped, errors int) {
	// Determine new database path
	new_db_path := fmt.Sprintf("%s/users/%s/%s.db", data_dir, user_id, app_name)

	if !file_exists(new_db_path) {
		info("Migration: app database %s does not exist for user %s, skipping %d attachments", app_name, user_id, len(attachments))
		return 0, len(attachments), 0
	}

	// Open new database
	new_db := db_open(new_db_path)
	if new_db == nil {
		info("Migration: cannot open new database %s for user %s", app_name, user_id)
		return 0, 0, len(attachments)
	}
	defer new_db.close()

	// Ensure _attachments table exists
	new_db.exec("create table if not exists _attachments ( id text not null primary key, object text not null, entity text not null default '', name text not null, size integer not null, content_type text not null default '', creator text not null default '', caption text not null default '', description text not null default '', rank integer not null default 0, created integer not null )")
	new_db.exec("create index if not exists _attachments_object on _attachments( object )")

	for _, att := range attachments {
		// Determine new object path (strip app prefix if present)
		new_object := normalize_object(app_name, att.Object)

		// Check if already migrated
		if new_db.exists("select 1 from _attachments where id = ?", att.ID) {
			skipped++
			continue
		}

		// Determine content type
		content_type := mime.TypeByExtension(filepath.Ext(att.Name))
		if content_type == "" {
			content_type = "application/octet-stream"
		}

		// Insert into new table
		// In old system, entity was the owner. In new system, entity is empty for local attachments.
		new_db.exec(`insert into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created)
			values (?, ?, '', ?, ?, ?, ?, '', '', ?, ?)`,
			att.ID, new_object, att.Name, att.Size, content_type, att.Entity, att.Rank, att.Created)

		// Copy file to new location
		old_file_path := fmt.Sprintf("%s/users/%s/%s", data_dir, user_id, att.Path)
		new_file_path := fmt.Sprintf("%s/users/%s/files/%s_%s", data_dir, user_id, att.ID, filepath.Base(att.Name))

		if file_exists(old_file_path) {
			// Ensure directory exists
			file_mkdir(filepath.Dir(new_file_path))

			// Copy file
			if err := copy_file(old_file_path, new_file_path); err != nil {
				info("Migration: failed to copy file %s -> %s: %v", old_file_path, new_file_path, err)
				errors++
				continue
			}
		} else {
			info("Migration: old file not found: %s", old_file_path)
		}

		migrated++
	}

	info("Migration: %s/%s: %d migrated, %d skipped, %d errors", user_id, app_name, migrated, skipped, errors)
	return migrated, skipped, errors
}

// Determine which app an object belongs to based on its path
func get_app_from_object(object string) string {
	parts := strings.SplitN(object, "/", 2)
	if len(parts) == 0 {
		return ""
	}

	switch parts[0] {
	case "chat":
		return "chat"
	case "forums":
		return "forums"
	case "feeds":
		return "feeds"
	default:
		// Try to match app name directly
		return parts[0]
	}
}

// Normalize object path for new system
// Old format: "chat/chatid/messageid" or "forums/forumid/postid"
// New format for chat: "chat/chatid/messageid" (unchanged)
// New format for forums: "postid" (just the post ID)
// New format for feeds: "postid" (just the post ID)
func normalize_object(app_name string, object string) string {
	parts := strings.Split(object, "/")

	switch app_name {
	case "forums":
		// Old: forums/forumid/postid -> New: postid
		if len(parts) >= 3 {
			return parts[2]
		}
	case "feeds":
		// Old: feeds/postid or just postid -> New: postid
		if len(parts) >= 2 && parts[0] == "feeds" {
			return parts[1]
		}
	case "chat":
		// Keep as-is: chat/chatid/messageid
		return object
	}

	return object
}

// Copy a file from src to dst
func copy_file(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0644)
}
