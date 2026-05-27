//go:build !windows

// Mochi server: Audit logging (Unix implementation using syslog)
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"fmt"
	"log/syslog"
	"os"
)

var (
	audit_auth   *syslog.Writer // LOG_AUTH: authentication, authorization, security
	audit_daemon *syslog.Writer // LOG_DAEMON: service lifecycle
	audit_ops    *syslog.Writer // LOG_LOCAL0: application configuration/operations
)

// audit_syslog_available reports whether a syslog daemon is reachable. On
// hosts with no /dev/log socket (distroless containers, future macOS) syslog
// is structurally unavailable, so audit_init shouldn't warn the admin —
// nothing's broken, the audit-to-syslog path is just inert.
func audit_syslog_available() bool {
	_, err := os.Stat("/dev/log")
	return err == nil
}

// Initialize audit logging writers
func audit_init() {
	if !audit_syslog_available() {
		info("audit: /dev/log not present; syslog audit trail disabled")
		return
	}

	var err error

	audit_auth, err = syslog.New(syslog.LOG_AUTH|syslog.LOG_INFO, "mochi")
	if err != nil {
		warn("Failed to initialize auth audit log: %v", err)
	}

	audit_daemon, err = syslog.New(syslog.LOG_DAEMON|syslog.LOG_INFO, "mochi")
	if err != nil {
		warn("Failed to initialize daemon audit log: %v", err)
	}

	audit_ops, err = syslog.New(syslog.LOG_LOCAL0|syslog.LOG_INFO, "mochi")
	if err != nil {
		warn("Failed to initialize ops audit log: %v", err)
	}
}

// Close audit logging writers
func audit_close() {
	if audit_auth != nil {
		audit_auth.Close()
	}
	if audit_daemon != nil {
		audit_daemon.Close()
	}
	if audit_ops != nil {
		audit_ops.Close()
	}
}

// audit_log_auth writes to the auth facility
func audit_log_auth(msg string) {
	if audit_auth != nil {
		audit_auth.Info(msg)
	}
}

// audit_log_daemon writes to the daemon facility
func audit_log_daemon(msg string) {
	if audit_daemon != nil {
		audit_daemon.Info(msg)
	}
}

// audit_log_ops writes to the ops facility
func audit_log_ops(msg string) {
	if audit_ops != nil {
		audit_ops.Info(msg)
	}
}

// LOG_AUTH: Authentication, authorization, and security events

// audit_login logs a successful login
func audit_login(user string, ip string) {
	audit_log_auth(fmt.Sprintf("login user=%s ip=%s", user, ip))
}

// audit_login_failed logs a failed login attempt
func audit_login_failed(user string, ip string, reason string) {
	audit_log_auth(fmt.Sprintf("login_failed user=%s ip=%s reason=%s", user, ip, reason))
}

// audit_logout logs a user logout
func audit_logout(user string, ip string) {
	audit_log_auth(fmt.Sprintf("logout user=%s ip=%s", user, ip))
}

// audit_session_anomaly logs session anomalies (e.g., token used after revocation)
func audit_session_anomaly(user string, ip string, reason string) {
	audit_log_auth(fmt.Sprintf("session_anomaly user=%s ip=%s reason=%s", user, ip, reason))
}

// audit_access_denied logs access denied events
func audit_access_denied(user string, resource string, operation string) {
	audit_log_auth(fmt.Sprintf("access_denied user=%s resource=%s operation=%s", user, resource, operation))
}

// audit_permission_changed logs permission changes
func audit_permission_changed(admin string, subject string, resource string, operation string, grant bool) {
	action := "deny"
	if grant {
		action = "allow"
	}
	audit_log_auth(fmt.Sprintf("permission_changed admin=%s subject=%s resource=%s operation=%s action=%s", admin, subject, resource, operation, action))
}

// audit_admin_escalation logs admin privilege escalation
func audit_admin_escalation(admin string, target string, action string) {
	audit_log_auth(fmt.Sprintf("admin_escalation admin=%s target=%s action=%s", admin, target, action))
}

// audit_signature_failed logs signature verification failures
func audit_signature_failed(from string, reason string) {
	audit_log_auth(fmt.Sprintf("signature_failed from=%s reason=%s", from, reason))
}

// audit_message_rejected logs rejected Net messages
func audit_message_rejected(from string, reason string) {
	audit_log_auth(fmt.Sprintf("message_rejected from=%s reason=%s", from, reason))
}

// audit_replication_pending_purged logs a unfillable-pending row drop
// from the GC. One entry per dropped row so operators investigating
// "what did we lose" can grep the audit trail by (peer, scope, user,
// db, sequence). age is the row's lifetime in pending before drop.
func audit_replication_pending_purged(peer, scope, user, database string, sequence int64, age int64) {
	audit_log_ops(fmt.Sprintf("replication_pending_purged peer=%s scope=%s user=%s db=%s sequence=%d age=%d",
		peer, scope, user, database, sequence, age))
}

// audit_broadcast_pending_purged logs a skipped-gap broadcast GC
// event. One entry per stalled stream that got its gap skipped, with
// the start/end sequences and the gap size so an operator can grep
// for "what did this user lose from which feed".
func audit_broadcast_pending_purged(user, app, peer, key string, from_seq, to_seq, gap_size int64) {
	audit_log_ops(fmt.Sprintf("broadcast_pending_purged user=%s app=%s peer=%s key=%s from_sequence=%d to_sequence=%d gap=%d",
		user, app, peer, key, from_seq, to_seq, gap_size))
}

// audit_key_generated logs key generation events
func audit_key_generated(entity string, class string) {
	audit_log_auth(fmt.Sprintf("key_generated entity=%s class=%s", entity, class))
}

// audit_user_created logs user creation
func audit_user_created(admin string, user string, role string) {
	audit_log_auth(fmt.Sprintf("user_created admin=%s user=%s role=%s", admin, user, role))
}

// audit_user_deleted logs user deletion
func audit_user_deleted(admin string, user string) {
	audit_log_auth(fmt.Sprintf("user_deleted admin=%s user=%s", admin, user))
}

// audit_identity_created logs identity creation
func audit_identity_created(user string, entity string, class string) {
	audit_log_auth(fmt.Sprintf("identity_created user=%s entity=%s class=%s", user, entity, class))
}

// audit_identity_deleted logs identity deletion
func audit_identity_deleted(user string, entity string) {
	audit_log_auth(fmt.Sprintf("identity_deleted user=%s entity=%s", user, entity))
}

// audit_password_changed logs password/credential changes
func audit_password_changed(user string, method string) {
	audit_log_auth(fmt.Sprintf("password_changed user=%s method=%s", user, method))
}

// audit_email_changed logs email changes
func audit_email_changed(admin string, user string, old_email string, new_email string) {
	audit_log_auth(fmt.Sprintf("email_changed admin=%s user=%s old=%s new=%s", admin, user, old_email, new_email))
}

// audit_rate_limit logs rate limit triggers
func audit_rate_limit(ip string, limiter string) {
	audit_log_auth(fmt.Sprintf("rate_limit ip=%s limiter=%s", ip, limiter))
}

// audit_repeated_failures logs repeated failures from same source
func audit_repeated_failures(ip string, count int, action string) {
	audit_log_auth(fmt.Sprintf("repeated_failures ip=%s count=%d action=%s", ip, count, action))
}

// LOG_DAEMON: Service lifecycle events

// audit_server_start logs server startup
func audit_server_start(version string) {
	audit_log_daemon(fmt.Sprintf("server_start version=%s", version))
}

// audit_server_stop logs server shutdown
func audit_server_stop() {
	audit_log_daemon("server_stop")
}

// audit_schema_migrated logs server schema migrations
func audit_schema_migrated(from_version int, to_version int) {
	audit_log_daemon(fmt.Sprintf("schema_migrated from=%d to=%d", from_version, to_version))
}

// LOG_LOCAL0: Application configuration/operations

// audit_app_installed logs app installation
func audit_app_installed(app string, version string) {
	audit_log_ops(fmt.Sprintf("app_installed app=%s version=%s", app, version))
}

// audit_app_removed logs app removal
func audit_app_removed(app string) {
	audit_log_ops(fmt.Sprintf("app_removed app=%s", app))
}

// audit_app_upgraded logs app upgrades
func audit_app_upgraded(app string, old_version string, new_version string) {
	audit_log_ops(fmt.Sprintf("app_upgraded app=%s old_version=%s new_version=%s", app, old_version, new_version))
}

// audit_app_schema_migrated logs app schema migrations
func audit_app_schema_migrated(app string, from_version int, to_version int) {
	audit_log_ops(fmt.Sprintf("app_schema_migrated app=%s from=%d to=%d", app, from_version, to_version))
}

// audit_default_version_changed logs default app version changes
func audit_default_version_changed(admin string, app string, version string, track string) {
	audit_log_ops(fmt.Sprintf("default_version_changed admin=%s app=%s version=%s track=%s", admin, app, version, track))
}

// audit_default_track_changed logs default app track changes
func audit_default_track_changed(admin string, app string, track string, version string) {
	audit_log_ops(fmt.Sprintf("default_track_changed admin=%s app=%s track=%s version=%s", admin, app, track, version))
}

// audit_default_routing_changed logs default routing changes (class, path, service)
func audit_default_routing_changed(admin string, routing_type string, key string, app string) {
	audit_log_ops(fmt.Sprintf("default_routing_changed admin=%s type=%s key=%s app=%s", admin, routing_type, key, app))
}

// audit_settings_changed logs non-security settings changes
func audit_settings_changed(admin string, setting string, value string) {
	audit_log_ops(fmt.Sprintf("settings_changed admin=%s setting=%s value=%s", admin, setting, value))
}

// audit_user_version_changed logs user app version changes
func audit_user_version_changed(user string, app string, version string, track string) {
	audit_log_ops(fmt.Sprintf("user_version_changed user=%s app=%s version=%s track=%s", user, app, version, track))
}

// audit_user_routing_changed logs user routing changes (class, path, service)
func audit_user_routing_changed(user string, routing_type string, key string, app string) {
	audit_log_ops(fmt.Sprintf("user_routing_changed user=%s type=%s key=%s app=%s", user, routing_type, key, app))
}

// audit_replication_pair_join_approved logs that the operator approved
// an incoming whole-server pair join request from `peer`.
func audit_replication_pair_join_approved(peer string) {
	audit_log_ops(fmt.Sprintf("replication_pair_join_approved peer=%s", peer))
}

// audit_replication_pair_join_denied logs that the operator denied an
// incoming whole-server pair join request from `peer`.
func audit_replication_pair_join_denied(peer string) {
	audit_log_ops(fmt.Sprintf("replication_pair_join_denied peer=%s", peer))
}

// audit_replication_pair_removed logs that the operator kicked a peer
// out of the pair set.
func audit_replication_pair_removed(peer string) {
	audit_log_ops(fmt.Sprintf("replication_pair_removed peer=%s", peer))
}

// audit_replication_link_approved logs that a user approved an
// incoming per-user replication link request from another server.
func audit_replication_link_approved(user string, peer string) {
	audit_log_ops(fmt.Sprintf("replication_link_approved user=%s peer=%s", user, peer))
}

// audit_replication_link_denied logs that a user denied an incoming
// per-user replication link request.
func audit_replication_link_denied(user string, peer string) {
	audit_log_ops(fmt.Sprintf("replication_link_denied user=%s peer=%s", user, peer))
}

// audit_replication_host_removed logs that a user removed a host from
// their per-user opt-in replication set.
func audit_replication_host_removed(user string, peer string) {
	audit_log_ops(fmt.Sprintf("replication_host_removed user=%s peer=%s", user, peer))
}

// audit_replication_bootstrap_started logs that a bulk-bootstrap run
// has been initiated against `peer` — either auto via join-approved or
// manually via mochictl replication resync.
func audit_replication_bootstrap_started(peer string) {
	audit_log_daemon(fmt.Sprintf("replication_bootstrap_started peer=%s", peer))
}

// audit_replication_bootstrap_scope_done logs that one scope of the
// bulk bootstrap has reached state='done' for a given peer.
func audit_replication_bootstrap_scope_done(peer string, scope string) {
	audit_log_daemon(fmt.Sprintf("replication_bootstrap_scope_done peer=%s scope=%s", peer, scope))
}
