// Package stampdb persists the traQ stamps, with the emoji of the Unicode ones, in the connector's own table.
package stampdb

import (
	"context"
	"embed"

	"go.mau.fi/util/dbutil"

	"github.com/kisepichu/traq-matrix-bridge/pkg/stampconv"
)

// VersionTableName is the version table of the connector's section of the bridge database.
const VersionTableName = "traq_version"

//go:embed upgrades/*.sql
var upgrades embed.FS

// UpgradeTable creates and upgrades the connector's tables.
var UpgradeTable = dbutil.BuildUpgradeTable().WithFSPath(upgrades, "upgrades").Finish()

// Store reads and writes the traq_stamp table.
type Store struct{ db *dbutil.Database }

// Pending is a Unicode stamp whose emoji has not been resolved from its file name yet.
type Pending struct{ ID, FileID string }

// New returns a store on db, the child database (br.DB.Database.Child(VersionTableName, UpgradeTable, ...)).
func New(db *dbutil.Database) *Store {
	return &Store{db: db}
}

// All returns every stamp.
func (s *Store) All(ctx context.Context) ([]stampconv.Stamp, error) {
	rows, err := s.db.Query(ctx, `SELECT id, name, is_unicode, emoji FROM traq_stamp`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []stampconv.Stamp
	for rows.Next() {
		var st stampconv.Stamp
		if err := rows.Scan(&st.ID, &st.Name, &st.Unicode, &st.Emoji); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// Put adds or replaces a stamp. fileID is the ID of the stamp's image.
func (s *Store) Put(ctx context.Context, st stampconv.Stamp, fileID string) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO traq_stamp (id, name, is_unicode, emoji, file_id) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE SET
			name=excluded.name, is_unicode=excluded.is_unicode, emoji=excluded.emoji, file_id=excluded.file_id
	`, st.ID, st.Name, st.Unicode, st.Emoji, fileID)
	return err
}

// Delete removes a stamp. A missing stamp is not an error.
func (s *Store) Delete(ctx context.Context, id string) error {
	_, err := s.db.Exec(ctx, `DELETE FROM traq_stamp WHERE id=$1`, id)
	return err
}

// PendingUnicode returns the Unicode stamps whose emoji is not resolved yet.
func (s *Store) PendingUnicode(ctx context.Context) ([]Pending, error) {
	rows, err := s.db.Query(ctx, `SELECT id, file_id FROM traq_stamp WHERE is_unicode AND emoji='' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Pending
	for rows.Next() {
		var p Pending
		if err := rows.Scan(&p.ID, &p.FileID); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Complete reports whether the table has at least one stamp and no pending Unicode stamp.
func (s *Store) Complete(ctx context.Context) (bool, error) {
	var total, pending int
	err := s.db.QueryRow(ctx, `
		SELECT COUNT(*), COUNT(CASE WHEN is_unicode AND emoji='' THEN 1 END) FROM traq_stamp
	`).Scan(&total, &pending)
	return total > 0 && pending == 0, err
}
