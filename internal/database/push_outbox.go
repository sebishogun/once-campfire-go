package database

import (
	"context"
	"time"
)

// PendingPush appends at most limit outstanding message IDs into dst. Intent
// survives a process crash; acknowledgement follows delivery, never enqueue.
func (d *DB) PendingPush(ctx context.Context, dst []int64, limit int) ([]int64, error) {
	return d.pendingPush(ctx, dst, limit, int64(1<<63-1))
}

func (d *DB) DuePush(ctx context.Context, dst []int64, limit int) ([]int64, error) {
	return d.pendingPush(ctx, dst, limit, time.Now().Unix())
}

func (d *DB) pendingPush(ctx context.Context, dst []int64, limit int, now int64) ([]int64, error) {
	rows, err := d.Read.QueryContext(ctx, "SELECT message_id FROM campfire_push_outbox WHERE retry_at<=? ORDER BY retry_at,message_id LIMIT ?", now, limit)
	if err != nil {
		return dst, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return dst, err
		}
		dst = append(dst, id)
	}
	return dst, rows.Err()
}

// RetryPush backs off a failed delivery so a poison recipient cannot keep
// later messages outside the bounded scan window indefinitely.
func (d *DB) RetryPush(ctx context.Context, id int64) error {
	_, err := d.Write.ExecContext(ctx, "UPDATE campfire_push_outbox SET retry_at=? WHERE message_id=?", time.Now().Add(30*time.Second).Unix(), id)
	return err
}

func (d *DB) AckPush(ctx context.Context, id int64) error {
	_, err := d.Write.ExecContext(ctx, "DELETE FROM campfire_push_outbox WHERE message_id=?", id)
	return err
}

func (d *DB) PushRoom(ctx context.Context, id int64) (Room, error) {
	r := Room{ID: id}
	err := d.Read.QueryRowContext(ctx, "SELECT coalesce(name,''),type FROM rooms WHERE id=?", id).Scan(&r.Name, &r.Type)
	return r, err
}
