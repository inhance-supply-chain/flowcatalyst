// Package usecasepgx is the pgx-backed UnitOfWork for FlowCatalyst use
// cases. It wraps a pgxpool.Pool and a usecasepgx.Sink (which decides
// where domain events and audit logs are written — typically
// outboxpgx.Sink for consumer apps, or a platform-specific sink that
// writes to msg_events / iam_audit_logs directly).
//
// Use cases call the generic free functions Commit, CommitDelete,
// CommitAll, and EmitEvent. Those are the only paths to a Success-valued
// usecase.Result outside this SDK.
package usecasepgx

import "github.com/jackc/pgx/v5"

// DbTx is an opaque write handle passed to repository Persist methods.
// Wraps a pgx.Tx so repository code doesn't import pgx directly through
// some unrelated path; a future driver swap touches this file plus the
// commit.go file, nothing else.
//
// Repositories access the underlying pgx.Tx via Inner().
type DbTx struct {
	inner pgx.Tx
}

// Inner exposes the underlying pgx.Tx. Repository methods call this to
// execute SQL.
func (t *DbTx) Inner() pgx.Tx { return t.inner }

// newDbTx is internal to the SDK; only commit.go / run.go construct one.
func newDbTx(tx pgx.Tx) *DbTx { return &DbTx{inner: tx} }
