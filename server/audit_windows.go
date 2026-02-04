//go:build windows

// Mochi server: Audit logging (Windows implementation using file logging)
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	audit_file   *os.File
	audit_logger *log.Logger
	audit_mutex  sync.Mutex
)

// Initialize audit logging to file
func audit_init() {
	audit_mutex.Lock()
	defer audit_mutex.Unlock()

	// Create audit log in data directory
	log_path := filepath.Join(data_dir, "audit.log")
	file_mkdir(filepath.Dir(log_path))

	var err error
	audit_file, err = os.OpenFile(log_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		warn("Failed to initialize audit log file: %v", err)
		return
	}

	audit_logger = log.New(audit_file, "", 0)
}

// Close audit logging
func audit_close() {
	audit_mutex.Lock()
	defer audit_mutex.Unlock()

	if audit_file != nil {
		audit_file.Close()
		audit_file = nil
	}
}

// audit_write writes a message to the audit log with timestamp and facility
func audit_write(facility string, msg string) {
	audit_mutex.Lock()
	defer audit_mutex.Unlock()

	if audit_logger != nil {
		timestamp := time.Now().Format("2006-01-02 15:04:05")
		audit_logger.Printf("%s [%s] %s", timestamp, facility, msg)
	}
}

// LOG_AUTH: Authentication, authorization, and security events

// audit_login logs a successful login
func audit_login(user string, ip string) {
	audit_write("AUTH", fmt.Sprintf("login user=%s ip=%s", user, ip))
}

// audit_login_failed logs a failed login attempt
func audit_login_failed(user string, ip string, reason string) {
	audit_write("AUTH", fmt.Sprintf("login_failed user=%s ip=%s reason=%s", user, ip, reason))
}

// audit_logout logs a user logout
func audit_logout(user string, ip string) {
	audit_write("AUTH", fmt.Sprintf("logout user=%s ip=%s", user, ip))
}

// audit_session_anomaly logs session anomalies (e.g., token used after revocation)
func audit_session_anomaly(user string, ip string, reason string) {
	audit_write("AUTH", fmt.Sprintf("session_anomaly user=%s ip=%s reason=%s", user, ip, reason))
}

// audit_access_denied logs access denied events
func audit_access_denied(user string, resource string, operation string) {
	audit_write("AUTH", fmt.Sprintf("access_denied user=%s resource=%s operation=%s", user, resource, operation))
}

// audit_permission_changed logs permission changes
func audit_permission_changed(admin string, subject string, resource string, operation string, grant bool) {
	action := "deny"
	if grant {
		action = "allow"
	}
	audit_write("AUTH", fmt.Sprintf("permission_changed admin=%s subject=%s resource=%s operation=%s action=%s", admin, subject, resource, operation, action))
}

// audit_admin_escalation logs admin privilege escalation
func audit_admin_escalation(admin string, target string, action string) {
	audit_write("AUTH", fmt.Sprintf("admin_escalation admin=%s target=%s action=%s", admin, target, action))
}

// audit_signature_failed logs signature verification failures
func audit_signature_failed(from string, reason string) {
	audit_write("AUTH", fmt.Sprintf("signature_failed from=%s reason=%s", from, reason))
}

// audit_message_rejected logs rejected P2P messages
func audit_message_rejected(from string, reason string) {
	audit_write("AUTH", fmt.Sprintf("message_rejected from=%s reason=%s", from, reason))
}

// audit_key_generated logs key generation events
func audit_key_generated(entity string, class string) {
	audit_write("AUTH", fmt.Sprintf("key_generated entity=%s class=%s", entity, class))
}

// audit_user_created logs user creation
func audit_user_created(admin string, user string, role string) {
	audit_write("AUTH", fmt.Sprintf("user_created admin=%s user=%s role=%s", admin, user, role))
}

// audit_user_deleted logs user deletion
func audit_user_deleted(admin string, user string) {
	audit_write("AUTH", fmt.Sprintf("user_deleted admin=%s user=%s", admin, user))
}

// audit_identity_created logs identity creation
func audit_identity_created(user string, entity string, class string) {
	audit_write("AUTH", fmt.Sprintf("identity_created user=%s entity=%s class=%s", user, entity, class))
}

// audit_identity_deleted logs identity deletion
func audit_identity_deleted(user string, entity string) {
	audit_write("AUTH", fmt.Sprintf("identity_deleted user=%s entity=%s", user, entity))
}

// audit_password_changed logs password/credential changes
func audit_password_changed(user string, method string) {
	audit_write("AUTH", fmt.Sprintf("password_changed user=%s method=%s", user, method))
}

// audit_email_changed logs email changes
func audit_email_changed(admin string, user string, old_email string, new_email string) {
	audit_write("AUTH", fmt.Sprintf("email_changed admin=%s user=%s old=%s new=%s", admin, user, old_email, new_email))
}

// audit_rate_limit logs rate limit triggers
func audit_rate_limit(ip string, limiter string) {
	audit_write("AUTH", fmt.Sprintf("rate_limit ip=%s limiter=%s", ip, limiter))
}

// audit_repeated_failures logs repeated failures from same source
func audit_repeated_failures(ip string, count int, action string) {
	audit_write("AUTH", fmt.Sprintf("repeated_failures ip=%s count=%d action=%s", ip, count, action))
}

// LOG_DAEMON: Service lifecycle events

// audit_server_start logs server startup
func audit_server_start(version string) {
	audit_write("DAEMON", fmt.Sprintf("server_start version=%s", version))
}

// audit_server_stop logs server shutdown
func audit_server_stop() {
	audit_write("DAEMON", "server_stop")
}

// audit_schema_migrated logs server schema migrations
func audit_schema_migrated(from_version int, to_version int) {
	audit_write("DAEMON", fmt.Sprintf("schema_migrated from=%d to=%d", from_version, to_version))
}

// LOG_LOCAL0: Application configuration/operations

// audit_app_installed logs app installation
func audit_app_installed(app string, version string) {
	audit_write("OPS", fmt.Sprintf("app_installed app=%s version=%s", app, version))
}

// audit_app_removed logs app removal
func audit_app_removed(app string) {
	audit_write("OPS", fmt.Sprintf("app_removed app=%s", app))
}

// audit_app_upgraded logs app upgrades
func audit_app_upgraded(app string, old_version string, new_version string) {
	audit_write("OPS", fmt.Sprintf("app_upgraded app=%s old_version=%s new_version=%s", app, old_version, new_version))
}

// audit_app_schema_migrated logs app schema migrations
func audit_app_schema_migrated(app string, from_version int, to_version int) {
	audit_write("OPS", fmt.Sprintf("app_schema_migrated app=%s from=%d to=%d", app, from_version, to_version))
}

// audit_default_version_changed logs default app version changes
func audit_default_version_changed(admin string, app string, version string, track string) {
	audit_write("OPS", fmt.Sprintf("default_version_changed admin=%s app=%s version=%s track=%s", admin, app, version, track))
}

// audit_default_track_changed logs default app track changes
func audit_default_track_changed(admin string, app string, track string, version string) {
	audit_write("OPS", fmt.Sprintf("default_track_changed admin=%s app=%s track=%s version=%s", admin, app, track, version))
}

// audit_default_routing_changed logs default routing changes (class, path, service)
func audit_default_routing_changed(admin string, routing_type string, key string, app string) {
	audit_write("OPS", fmt.Sprintf("default_routing_changed admin=%s type=%s key=%s app=%s", admin, routing_type, key, app))
}

// audit_settings_changed logs non-security settings changes
func audit_settings_changed(admin string, setting string, value string) {
	audit_write("OPS", fmt.Sprintf("settings_changed admin=%s setting=%s value=%s", admin, setting, value))
}

// audit_user_version_changed logs user app version changes
func audit_user_version_changed(user string, app string, version string, track string) {
	audit_write("OPS", fmt.Sprintf("user_version_changed user=%s app=%s version=%s track=%s", user, app, version, track))
}

// audit_user_routing_changed logs user routing changes (class, path, service)
func audit_user_routing_changed(user string, routing_type string, key string, app string) {
	audit_write("OPS", fmt.Sprintf("user_routing_changed user=%s type=%s key=%s app=%s", user, routing_type, key, app))
}
