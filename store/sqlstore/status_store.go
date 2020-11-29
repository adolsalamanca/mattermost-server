// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package sqlstore

import (
	"database/sql"
	"net/http"
	"strings"

	sq "github.com/Masterminds/squirrel"

	"github.com/mattermost/mattermost-server/v5/model"
	"github.com/mattermost/mattermost-server/v5/store"
)

const (
	MISSING_STATUS_ERROR = "store.sql_status.get.missing.app_error"
)

type SqlStatusStore struct {
	SqlStore
}

func newSqlStatusStore(sqlStore SqlStore) store.StatusStore {
	s := &SqlStatusStore{sqlStore}

	for _, db := range sqlStore.GetAllConns() {
		table := db.AddTableWithName(model.Status{}, "Status").SetKeys(false, "UserId")
		table.ColMap("UserId").SetMaxSize(26)
		table.ColMap("Status").SetMaxSize(32)
		table.ColMap("ActiveChannel").SetMaxSize(26)
	}

	return s
}

func (s SqlStatusStore) createIndexesIfNotExists() {
	s.CreateIndexIfNotExists("idx_status_user_id", "Status", "UserId")
	s.CreateIndexIfNotExists("idx_status_status", "Status", "Status")
}

func (s SqlStatusStore) SaveOrUpdate(status *model.Status) error {
	if err := s.GetReplica().SelectOne(&model.Status{}, "SELECT * FROM Status WHERE UserId = :UserId", map[string]interface{}{"UserId": status.UserId}); err == nil {
		if _, err := s.GetMaster().Update(status); err != nil {
			return err
		}
	} else {
		if err := s.GetMaster().Insert(status); err != nil {
			if !(strings.Contains(err.Error(), "for key 'PRIMARY'") && strings.Contains(err.Error(), "Duplicate entry")) {
				return err
			}
		}
	}
	return nil
}

func (s SqlStatusStore) Get(userId string) (*model.Status, error) {
	var status model.Status

	if err := s.GetReplica().SelectOne(&status,
		`SELECT
			*
		FROM
			Status
		WHERE
			UserId = :UserId`, map[string]interface{}{"UserId": userId}); err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		return nil, err
	}
	return &status, nil
}

func (s SqlStatusStore) GetByIds(userIds []string) ([]*model.Status, error) {

	failure := func(err error) error {
		return model.NewAppError(
			"SqlStatusStore.GetByIds",
			"store.sql_status.get.app_error",
			nil,
			err.Error(),
			http.StatusInternalServerError,
		)
	}

	query := s.getQueryBuilder().
		Select("UserId, Status, Manual, LastActivityAt").
		From("Status").
		Where(sq.Eq{"UserId": userIds})
	queryString, args, err := query.ToSql()
	if err != nil {
		return nil, failure(err)
	}
	rows, err := s.GetReplica().Db.Query(queryString, args...)
	if err != nil {
		return nil, failure(err)
	}
	var statuses []*model.Status
	defer rows.Close()
	for rows.Next() {
		var status model.Status
		if err = rows.Scan(&status.UserId, &status.Status, &status.Manual, &status.LastActivityAt); err != nil {
			return nil, failure(err)
		}
		statuses = append(statuses, &status)
	}
	if err = rows.Err(); err != nil {
		return nil, failure(err)
	}

	return statuses, nil
}

func (s SqlStatusStore) ResetAll() error {
	if _, err := s.GetMaster().Exec("UPDATE Status SET Status = :Status WHERE Manual = false", map[string]interface{}{"Status": model.STATUS_OFFLINE}); err != nil {
		return err
	}
	return nil
}

func (s SqlStatusStore) GetTotalActiveUsersCount() (int64, error) {
	time := model.GetMillis() - (1000 * 60 * 60 * 24)
	count, err := s.GetReplica().SelectInt("SELECT COUNT(UserId) FROM Status WHERE LastActivityAt > :Time", map[string]interface{}{"Time": time})
	if err != nil {
		return count, err
	}
	return count, nil
}

func (s SqlStatusStore) UpdateLastActivityAt(userId string, lastActivityAt int64) error {
	if _, err := s.GetMaster().Exec("UPDATE Status SET LastActivityAt = :Time WHERE UserId = :UserId", map[string]interface{}{"UserId": userId, "Time": lastActivityAt}); err != nil {
		return err
	}

	return nil
}
