// Package csqlite is a minimal direct-SQLite binding for fastdb: exactly the
// calls the read layer uses, nothing more.
//
// It is not a database/sql driver. The SQLite symbols come from the
// amalgamation compiled into the binary by github.com/mattn/go-sqlite3, which
// the application links through internal/database; csqlite does not import the
// driver itself. A binary that links fastdb must also link go-sqlite3 (the
// repository entry points do). The vendored sqlite3-binding.h must match the
// module version — see README.md in this directory.
//
// # CGO safety
//
// No C pointer ever refers to Go memory after a call returns:
//
//   - Paths, SQL text and pragma text are copied into C memory for the call
//     and freed when it returns (sqlite3_prepare_v2 copies the SQL).
//   - BindText and BindTextBytes pass Go string/byte data to
//     sqlite3_bind_text with SQLITE_TRANSIENT, which makes SQLite copy the
//     bytes before the call returns; runtime.KeepAlive pins the argument
//     across the call. SQLITE_STATIC is never used.
//   - ColumnBytes returns a view of SQLite-owned memory and is documented as
//     valid only until the next Step/Reset/Finalize. ColumnText and the
//     fastdb ColumnTextInto copy before returning.
//   - ExecTyped fuses the typed binds, step, last-insert-rowid capture and
//     reset of one write statement into a single call. Its text arguments are
//     bound with SQLITE_TRANSIENT (copied during the call), and the reset that
//     follows a successful step reads no row buffer, so no Go pointer and no
//     SQLite buffer outlives the call.
package csqlite

/*
#include <stdlib.h>
#include "sqlite3-binding.h"

// fastdb_bind_text binds with SQLITE_TRANSIENT: SQLite copies the bytes during
// the call, so Go memory is never retained. A NULL pointer with n == 0 binds
// an empty string (sqlite3_bind_text would otherwise bind SQL NULL).
static int fastdb_bind_text(sqlite3_stmt *stmt, int idx, const void *val, int n) {
	if (val == NULL) val = "";
	return sqlite3_bind_text(stmt, idx, (const char *)val, n, SQLITE_TRANSIENT);
}

// fastdb_write_run fuses one write statement's typed binds, step, optional
// last-insert-rowid capture and reset into a single cgo transition. It is the
// write-side counterpart of the read side's fastdb_step_cells.
//
// Kind codes, mirrored by csqlite.ArgKind: 0 skip, 1 integer, 2 text, 3 null.
// Only the field matching the kind is read (ival for integer, p/n for text).
// Text is bound with fastdb_bind_text's SQLITE_TRANSIENT rule, so no Go
// pointer outlives the call.
//
// The error semantics reproduce the unfused bind/step/reset sequence exactly.
// A failing bind or step returns its raw SQLite result code WITHOUT resetting,
// so the caller can map sqlite3_errmsg while it is still the failing call's
// message and then reset; the unfused path read errmsg before its deferred
// reset did the same. On success the statement is reset and the reset result
// is dropped, as the write lane's deferred Reset drops it. A step that yields
// a row returns FASTDB_WRITE_ROW without resetting, and the caller resets.
#define FASTDB_ARG_SKIP 0
#define FASTDB_ARG_INT  1
#define FASTDB_ARG_TEXT 2
#define FASTDB_ARG_NULL 3
#define FASTDB_WRITE_ROW (-2)

static int fastdb_bind_slot(sqlite3_stmt *stmt, int idx, int kind, sqlite3_int64 ival, const void *p, int n) {
	switch (kind) {
	case FASTDB_ARG_INT:  return sqlite3_bind_int64(stmt, idx, ival);
	case FASTDB_ARG_NULL: return sqlite3_bind_null(stmt, idx);
	case FASTDB_ARG_TEXT: return fastdb_bind_text(stmt, idx, p, n);
	default:              return SQLITE_OK;
	}
}

static int fastdb_write_run(sqlite3_stmt *stmt, sqlite3_int64 *last_id, int want_last_id,
		int k0, sqlite3_int64 v0, const void *p0, int n0,
		int k1, sqlite3_int64 v1, const void *p1, int n1,
		int k2, sqlite3_int64 v2, const void *p2, int n2,
		int k3, sqlite3_int64 v3, const void *p3, int n3,
		int k4, sqlite3_int64 v4, const void *p4, int n4) {
	int rc;
	rc = fastdb_bind_slot(stmt, 1, k0, v0, p0, n0); if (rc != SQLITE_OK) return rc;
	rc = fastdb_bind_slot(stmt, 2, k1, v1, p1, n1); if (rc != SQLITE_OK) return rc;
	rc = fastdb_bind_slot(stmt, 3, k2, v2, p2, n2); if (rc != SQLITE_OK) return rc;
	rc = fastdb_bind_slot(stmt, 4, k3, v3, p3, n3); if (rc != SQLITE_OK) return rc;
	rc = fastdb_bind_slot(stmt, 5, k4, v4, p4, n4); if (rc != SQLITE_OK) return rc;
	rc = sqlite3_step(stmt);
	if (rc != SQLITE_ROW && rc != SQLITE_DONE) return rc;
	if (want_last_id) *last_id = sqlite3_last_insert_rowid(sqlite3_db_handle(stmt));
	if (rc == SQLITE_ROW) return FASTDB_WRITE_ROW;
	sqlite3_reset(stmt);
	return SQLITE_OK;
}
*/
import "C"

import (
	"errors"
	"runtime"
	"strconv"
	"strings"
	"unsafe"
)

// Primary SQLite result codes this package maps to error sentinels. The
// constants exist so Unwrap can name them and tests can assert them without
// parsing messages.
const (
	ResultBusy       = int(C.SQLITE_BUSY)
	ResultLocked     = int(C.SQLITE_LOCKED)
	ResultReadOnly   = int(C.SQLITE_READONLY)
	ResultCorrupt    = int(C.SQLITE_CORRUPT)
	ResultCantOpen   = int(C.SQLITE_CANTOPEN)
	ResultNotADB     = int(C.SQLITE_NOTADB)
	ResultConstraint = int(C.SQLITE_CONSTRAINT)
)

// Column value classes, as returned by Stmt.ColumnType.
const (
	TypeInteger = int(C.SQLITE_INTEGER)
	TypeFloat   = int(C.SQLITE_FLOAT)
	TypeText    = int(C.SQLITE_TEXT)
	TypeBlob    = int(C.SQLITE_BLOB)
	TypeNull    = int(C.SQLITE_NULL)
)

// Sentinels matched by errors.Is through *Error.Unwrap.
var (
	ErrBusy       = errors.New("sqlite: database is locked")
	ErrLocked     = errors.New("sqlite: database table is locked")
	ErrReadOnly   = errors.New("sqlite: attempt to write a readonly database")
	ErrCorrupt    = errors.New("sqlite: database disk image is malformed")
	ErrCantOpen   = errors.New("sqlite: unable to open database file")
	ErrNotADB     = errors.New("sqlite: file is not a database")
	ErrConstraint = errors.New("sqlite: constraint failed")
	// ErrNull reports a NULL where a typed read expected a value.
	ErrNull = errors.New("sqlite: unexpected NULL")
)

// Error is one SQLite failure: the result code plus the connection's message
// at the time of the call.
type Error struct {
	Code int
	Msg  string
}

func (e *Error) Error() string {
	if e.Msg == "" {
		return "sqlite: error " + strconv.Itoa(e.Code)
	}
	return "sqlite: " + e.Msg + " (code " + strconv.Itoa(e.Code) + ")"
}

// Unwrap maps the primary result code to a sentinel so errors.Is works.
func (e *Error) Unwrap() error {
	switch e.Code & 0xff {
	case ResultBusy:
		return ErrBusy
	case ResultLocked:
		return ErrLocked
	case ResultReadOnly:
		return ErrReadOnly
	case ResultCorrupt:
		return ErrCorrupt
	case ResultCantOpen:
		return ErrCantOpen
	case ResultNotADB:
		return ErrNotADB
	case ResultConstraint:
		return ErrConstraint
	default:
		return nil
	}
}

// Conn is one SQLite connection. It is not safe for concurrent use; fastdb
// documents one Conn per goroutine.
type Conn struct {
	db *C.sqlite3
}

// OpenReadOnly opens path with SQLITE_OPEN_READONLY. Paths beginning with
// "file:" are treated as URIs. SQLite opens lazily, so a missing file is
// reported here while a malformed database is reported by the first statement
// (fastdb.OpenReadOnly runs its pragmas immediately).
func OpenReadOnly(path string) (*Conn, error) {
	return open(path, C.SQLITE_OPEN_READONLY)
}

// OpenReadWrite opens path read-write, creating it when it does not exist
// (SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE), with the same NOMUTEX and URI
// conventions as OpenReadOnly. The write connection shares the same
// single-owner contract: the caller serializes transactions in Go.
func OpenReadWrite(path string) (*Conn, error) {
	return open(path, C.SQLITE_OPEN_READWRITE|C.SQLITE_OPEN_CREATE)
}

func open(path string, extra C.int) (*Conn, error) {
	flags := C.int(C.SQLITE_OPEN_NOMUTEX) | extra
	if strings.HasPrefix(path, "file:") {
		flags |= C.SQLITE_OPEN_URI
	}
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var db *C.sqlite3
	if rc := C.sqlite3_open_v2(cpath, &db, flags, nil); rc != C.SQLITE_OK {
		err := openError(rc, db)
		if db != nil {
			C.sqlite3_close_v2(db)
		}
		return nil, err
	}
	return &Conn{db: db}, nil
}

// Close finalizes nothing; fastdb finalizes cached statements first. Close is
// idempotent.
func (c *Conn) Close() error {
	if c.db == nil {
		return nil
	}
	rc := C.sqlite3_close_v2(c.db)
	c.db = nil
	if rc != C.SQLITE_OK {
		return &Error{Code: int(rc), Msg: C.GoString(C.sqlite3_errstr(rc))}
	}
	return nil
}

// BusyTimeout sets how long SQLite retries a locked database before returning
// SQLITE_BUSY.
func (c *Conn) BusyTimeout(ms int) error {
	return c.err(C.sqlite3_busy_timeout(c.db, C.int(ms)))
}

// Prepare compiles one statement. The SQL text is copied to C memory for the
// duration of the call only; sqlite3_prepare_v2 stores its own copy.
func (c *Conn) Prepare(sql string) (*Stmt, error) {
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	var st *C.sqlite3_stmt
	rc := C.sqlite3_prepare_v2(c.db, csql, C.int(len(sql)), &st, nil)
	if rc != C.SQLITE_OK {
		return nil, c.err(rc)
	}
	return &Stmt{conn: c, stmt: st}, nil
}

// Exec runs a statement for effect, stepping to completion. It is used for
// the connection pragmas.
func (c *Conn) Exec(sql string) error {
	st, err := c.Prepare(sql)
	if err != nil {
		return err
	}
	for {
		row, err := st.Step()
		if err != nil {
			st.Reset()
			st.Finalize()
			return err
		}
		if !row {
			break
		}
	}
	if err := st.Reset(); err != nil {
		st.Finalize()
		return err
	}
	return st.Finalize()
}

// LastInsertRowID returns the rowid of the most recent successful INSERT on
// the connection, in the transaction the insert committed to. The value is
// read immediately after the insert's Step, on the same connection, so it
// always names this connection's last insert — the same value
// database/sql's LastInsertId reports.
func (c *Conn) LastInsertRowID() int64 {
	return int64(C.sqlite3_last_insert_rowid(c.db))
}

// Changes returns the number of rows modified by the most recently completed
// statement on the connection.
func (c *Conn) Changes() int64 {
	return int64(C.sqlite3_changes(c.db))
}

func (c *Conn) err(rc C.int) error {
	if rc == C.SQLITE_OK {
		return nil
	}
	if c.db != nil {
		return &Error{Code: int(rc), Msg: C.GoString(C.sqlite3_errmsg(c.db))}
	}
	return &Error{Code: int(rc), Msg: C.GoString(C.sqlite3_errstr(rc))}
}

func openError(rc C.int, db *C.sqlite3) error {
	msg := ""
	if db != nil {
		msg = C.GoString(C.sqlite3_errmsg(db))
	}
	if msg == "" {
		msg = C.GoString(C.sqlite3_errstr(rc))
	}
	return &Error{Code: int(rc), Msg: msg}
}

// ArgKind classifies one ExecTyped argument. The zero value ArgSkip leaves the
// slot unbound; a statement whose every placeholder is bound passes only
// ArgInt, ArgText and ArgNull.
type ArgKind int

const (
	ArgSkip ArgKind = iota
	ArgInt
	ArgText
	ArgNull
)

// Arg is one positional argument to Stmt.ExecTyped: Kind selects which field
// is read (Int for ArgInt, Text for ArgText; ArgNull and ArgSkip read
// nothing). It is a plain value, so building the argument list allocates
// nothing.
type Arg struct {
	Kind ArgKind
	Int  int64
	Text string
}

// Stmt is one prepared statement. It is not safe for concurrent use.
type Stmt struct {
	conn *Conn
	stmt *C.sqlite3_stmt
	// lastID is fastdb_write_run's out-parameter scratch. Keeping it on the
	// statement means the fused write passes a pointer into this heap object
	// instead of a stack variable, which cgo would otherwise force to escape
	// on every call.
	lastID C.sqlite3_int64
}

// textData returns the data pointer of a non-empty string, or nil for the
// empty string (fastdb_bind_text maps NULL with n == 0 to an empty string).
func textData(v string) unsafe.Pointer {
	if len(v) == 0 {
		return nil
	}
	return unsafe.Pointer(unsafe.StringData(v))
}

// ExecTyped runs one write statement as a single cgo transition: it binds up
// to five typed positional arguments, steps the statement, optionally captures
// the connection's last-insert-rowid, and resets. Binding each argument,
// stepping, reading the rowid and resetting separately would cost one
// transition apiece; this fuses the common write shapes (the message and
// rich-text INSERTs and their siblings) into one.
//
// Error semantics match the unfused bind/step/reset sequence exactly: the
// first failing bind or step is returned mapped through the connection's error
// (read before the reset, as the unfused path read it before its deferred
// reset), the statement is reset on every path so it stays reusable, and a
// step that yields a row is reported with row=true and a nil error. On success
// the reset's own result is dropped, exactly as the write lane's deferred
// Reset drops it. wantLastID reads sqlite3_last_insert_rowid after the step and
// before the reset, so the returned id names this statement's insert.
func (s *Stmt) ExecTyped(wantLastID bool, a0, a1, a2, a3, a4 Arg) (lastID int64, row bool, err error) {
	want := C.int(0)
	if wantLastID {
		want = 1
	}
	rc := C.fastdb_write_run(s.stmt, &s.lastID, want,
		C.int(a0.Kind), C.sqlite3_int64(a0.Int), textData(a0.Text), C.int(len(a0.Text)),
		C.int(a1.Kind), C.sqlite3_int64(a1.Int), textData(a1.Text), C.int(len(a1.Text)),
		C.int(a2.Kind), C.sqlite3_int64(a2.Int), textData(a2.Text), C.int(len(a2.Text)),
		C.int(a3.Kind), C.sqlite3_int64(a3.Int), textData(a3.Text), C.int(len(a3.Text)),
		C.int(a4.Kind), C.sqlite3_int64(a4.Int), textData(a4.Text), C.int(len(a4.Text)),
	)
	runtime.KeepAlive(a0.Text)
	runtime.KeepAlive(a1.Text)
	runtime.KeepAlive(a2.Text)
	runtime.KeepAlive(a3.Text)
	runtime.KeepAlive(a4.Text)
	switch rc {
	case C.SQLITE_OK:
		if !wantLastID {
			return 0, false, nil
		}
		return int64(s.lastID), false, nil
	case C.FASTDB_WRITE_ROW:
		s.Reset()
		return 0, true, nil
	default:
		err = s.conn.err(rc)
		s.Reset()
		return 0, false, err
	}
}

// Step advances one row. It reports false at SQLITE_DONE and returns a typed
// error for any other failure.
func (s *Stmt) Step() (bool, error) {
	switch rc := C.sqlite3_step(s.stmt); rc {
	case C.SQLITE_ROW:
		return true, nil
	case C.SQLITE_DONE:
		return false, nil
	default:
		return false, s.conn.err(rc)
	}
}

// BindInt64 binds parameter i (1-based).
func (s *Stmt) BindInt64(i int, v int64) error {
	return s.conn.err(C.sqlite3_bind_int64(s.stmt, C.int(i), C.sqlite3_int64(v)))
}

// BindText binds parameter i (1-based). SQLite copies the bytes during the
// call (SQLITE_TRANSIENT); the Go string is not retained.
func (s *Stmt) BindText(i int, v string) error {
	var p unsafe.Pointer
	if len(v) > 0 {
		p = unsafe.Pointer(unsafe.StringData(v))
	}
	rc := C.fastdb_bind_text(s.stmt, C.int(i), p, C.int(len(v)))
	runtime.KeepAlive(v)
	return s.conn.err(rc)
}

// BindTextBytes binds parameter i (1-based) from a byte slice with the same
// copy-on-bind rule as BindText.
func (s *Stmt) BindTextBytes(i int, v []byte) error {
	var p unsafe.Pointer
	if len(v) > 0 {
		p = unsafe.Pointer(unsafe.SliceData(v))
	}
	rc := C.fastdb_bind_text(s.stmt, C.int(i), p, C.int(len(v)))
	runtime.KeepAlive(v)
	return s.conn.err(rc)
}

// BindNull binds parameter i (1-based) as SQL NULL.
func (s *Stmt) BindNull(i int) error {
	return s.conn.err(C.sqlite3_bind_null(s.stmt, C.int(i)))
}

// ClearBindings resets all parameters to NULL.
func (s *Stmt) ClearBindings() error {
	return s.conn.err(C.sqlite3_clear_bindings(s.stmt))
}

// Reset ends the current execution, leaving the statement reusable. The
// returned error is the result of the most recent Step; callers that already
// handled a step error drop it.
func (s *Stmt) Reset() error {
	return s.conn.err(C.sqlite3_reset(s.stmt))
}

// Finalize destroys the statement. The receiver must not be used afterwards.
func (s *Stmt) Finalize() error {
	if s.stmt == nil {
		return nil
	}
	rc := C.sqlite3_finalize(s.stmt)
	s.stmt = nil
	if rc != C.SQLITE_OK {
		return &Error{Code: int(rc), Msg: C.GoString(C.sqlite3_errstr(rc))}
	}
	return nil
}

// ColumnType returns the value class of column i (0-based).
func (s *Stmt) ColumnType(i int) int {
	return int(C.sqlite3_column_type(s.stmt, C.int(i)))
}

// ColumnInt64 returns column i as an integer.
func (s *Stmt) ColumnInt64(i int) int64 {
	return int64(C.sqlite3_column_int64(s.stmt, C.int(i)))
}

// ColumnBytes returns a view of column i's bytes. The view aliases SQLite's
// buffer for the current row: it is valid only until the next Step, Reset or
// Finalize. A zero-length or NULL value returns nil; ColumnType distinguishes
// them. The caller must not retain the view.
func (s *Stmt) ColumnBytes(i int) []byte {
	n := C.sqlite3_column_bytes(s.stmt, C.int(i))
	if n == 0 {
		return nil
	}
	p := C.sqlite3_column_text(s.stmt, C.int(i))
	if p == nil {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), int(n))
}

// ColumnText returns column i as a freshly allocated Go string. NULL is an
// error; an empty string is not.
func (s *Stmt) ColumnText(i int) (string, error) {
	if C.sqlite3_column_type(s.stmt, C.int(i)) == C.SQLITE_NULL {
		return "", ErrNull
	}
	n := C.sqlite3_column_bytes(s.stmt, C.int(i))
	if n == 0 {
		return "", nil
	}
	p := C.sqlite3_column_text(s.stmt, C.int(i))
	return C.GoStringN((*C.char)(unsafe.Pointer(p)), n), nil
}
