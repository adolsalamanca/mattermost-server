// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package sqlstore

import (
	"net/http"

	"github.com/mattermost/gorp"

	"github.com/mattermost/mattermost-server/v5/mlog"
	"github.com/mattermost/mattermost-server/v5/model"
	"github.com/mattermost/mattermost-server/v5/store"
)

type SqlPreferenceStore struct {
	SqlStore
}

func newSqlPreferenceStore(sqlStore SqlStore) store.PreferenceStore {
	s := &SqlPreferenceStore{sqlStore}

	for _, db := range sqlStore.GetAllConns() {
		table := db.AddTableWithName(model.Preference{}, "Preferences").SetKeys(false, "UserId", "Category", "Name")
		table.ColMap("UserId").SetMaxSize(26)
		table.ColMap("Category").SetMaxSize(32)
		table.ColMap("Name").SetMaxSize(32)
		table.ColMap("Value").SetMaxSize(2000)
	}

	return s
}

func (s SqlPreferenceStore) createIndexesIfNotExists() {
	s.CreateIndexIfNotExists("idx_preferences_user_id", "Preferences", "UserId")
	s.CreateIndexIfNotExists("idx_preferences_category", "Preferences", "Category")
	s.CreateIndexIfNotExists("idx_preferences_name", "Preferences", "Name")
}

func (s SqlPreferenceStore) deleteUnusedFeatures() {
	mlog.Debug("Deleting any unused pre-release features")

	sql := `DELETE
		FROM Preferences
	WHERE
	Category = :Category
	AND Value = :Value
	AND Name LIKE '` + store.FEATURE_TOGGLE_PREFIX + `%'`

	queryParams := map[string]string{
		"Category": model.PREFERENCE_CATEGORY_ADVANCED_SETTINGS,
		"Value":    "false",
	}
	s.GetMaster().Exec(sql, queryParams)
}

func (s SqlPreferenceStore) Save(preferences *model.Preferences) error {
	// wrap in a transaction so that if one fails, everything fails
	transaction, err := s.GetMaster().Begin()
	if err != nil {
		return err
	}

	defer finalizeTransaction(transaction)
	for _, preference := range *preferences {
		preference := preference
		if upsertErr := s.save(transaction, &preference); upsertErr != nil {
			return upsertErr
		}
	}

	if err := transaction.Commit(); err != nil {
		// don't need to rollback here since the transaction is already closed
		return err
	}
	return nil
}

func (s SqlPreferenceStore) save(transaction *gorp.Transaction, preference *model.Preference) error {
	preference.PreUpdate()

	if err := preference.IsValid(); err != nil {
		return err
	}

	params := map[string]interface{}{
		"UserId":   preference.UserId,
		"Category": preference.Category,
		"Name":     preference.Name,
		"Value":    preference.Value,
	}

	if s.DriverName() == model.DATABASE_DRIVER_MYSQL {
		if _, err := transaction.Exec(
			`INSERT INTO
				Preferences
				(UserId, Category, Name, Value)
			VALUES
				(:UserId, :Category, :Name, :Value)
			ON DUPLICATE KEY UPDATE
				Value = :Value`, params); err != nil {
			return err
		}
		return nil
	} else if s.DriverName() == model.DATABASE_DRIVER_POSTGRES {
		// postgres has no way to upsert values until version 9.5 and trying inserting and then updating causes transactions to abort
		count, err := transaction.SelectInt(
			`SELECT
				count(0)
			FROM
				Preferences
			WHERE
				UserId = :UserId
				AND Category = :Category
				AND Name = :Name`, params)
		if err != nil {
			return err
		}

		if count == 1 {
			return s.update(transaction, preference)
		}
		return s.insert(transaction, preference)
	}
	return err
}

func (s SqlPreferenceStore) insert(transaction *gorp.Transaction, preference *model.Preference) error {
	if err := transaction.Insert(preference); err != nil {
		if IsUniqueConstraintError(err, []string{"UserId", "preferences_pkey"}) {
			return model.NewAppError("SqlPreferenceStore.insert", "store.sql_preference.insert.exists.app_error", nil,
				"user_id="+preference.UserId+", category="+preference.Category+", name="+preference.Name+", "+err.Error(), http.StatusBadRequest)
		}
		return model.NewAppError("SqlPreferenceStore.insert", "store.sql_preference.insert.save.app_error", nil,
			"user_id="+preference.UserId+", category="+preference.Category+", name="+preference.Name+", "+err.Error(), http.StatusInternalServerError)
	}

	return nil
}

func (s SqlPreferenceStore) update(transaction *gorp.Transaction, preference *model.Preference) error {
	if _, err := transaction.Update(preference); err != nil {
		return model.NewAppError("SqlPreferenceStore.update", "store.sql_preference.update.app_error", nil,
			"user_id="+preference.UserId+", category="+preference.Category+", name="+preference.Name+", "+err.Error(), http.StatusInternalServerError)
	}

	return nil
}

func (s SqlPreferenceStore) Get(userId string, category string, name string) (*model.Preference, error) {
	var preference *model.Preference

	if err := s.GetReplica().SelectOne(&preference,
		`SELECT
			*
		FROM
			Preferences
		WHERE
			UserId = :UserId
			AND Category = :Category
			AND Name = :Name`, map[string]interface{}{"UserId": userId, "Category": category, "Name": name}); err != nil {
		return nil, err
	}
	return preference, nil
}

func (s SqlPreferenceStore) GetCategory(userId string, category string) (model.Preferences, error) {
	var preferences model.Preferences

	if _, err := s.GetReplica().Select(&preferences,
		`SELECT
				*
			FROM
				Preferences
			WHERE
				UserId = :UserId
				AND Category = :Category`, map[string]interface{}{"UserId": userId, "Category": category}); err != nil {
		return nil, err
	}

	return preferences, nil

}

func (s SqlPreferenceStore) GetAll(userId string) (model.Preferences, error) {
	var preferences model.Preferences

	if _, err := s.GetReplica().Select(&preferences,
		`SELECT
				*
			FROM
				Preferences
			WHERE
				UserId = :UserId`, map[string]interface{}{"UserId": userId}); err != nil {
		return nil, err
	}
	return preferences, nil
}

func (s SqlPreferenceStore) PermanentDeleteByUser(userId string) error {
	query :=
		`DELETE FROM
			Preferences
		WHERE
			UserId = :UserId`

	if _, err := s.GetMaster().Exec(query, map[string]interface{}{"UserId": userId}); err != nil {
		return err
	}

	return nil
}

func (s SqlPreferenceStore) Delete(userId, category, name string) error {
	query :=
		`DELETE FROM Preferences
		WHERE
			UserId = :UserId
			AND Category = :Category
			AND Name = :Name`

	_, err := s.GetMaster().Exec(query, map[string]interface{}{"UserId": userId, "Category": category, "Name": name})

	if err != nil {
		return err
	}

	return nil
}

func (s SqlPreferenceStore) DeleteCategory(userId string, category string) error {
	_, err := s.GetMaster().Exec(
		`DELETE FROM
			Preferences
		WHERE
			UserId = :UserId
			AND Category = :Category`, map[string]interface{}{"UserId": userId, "Category": category})

	if err != nil {
		return err
	}

	return nil
}

func (s SqlPreferenceStore) DeleteCategoryAndName(category string, name string) error {
	_, err := s.GetMaster().Exec(
		`DELETE FROM
			Preferences
		WHERE
			Name = :Name
			AND Category = :Category`, map[string]interface{}{"Name": name, "Category": category})

	if err != nil {
		return err
	}

	return nil
}

func (s SqlPreferenceStore) CleanupFlagsBatch(limit int64) (int64, error) {
	query :=
		`DELETE FROM
			Preferences
		WHERE
			Category = :Category
			AND Name IN (
				SELECT
					*
				FROM (
					SELECT
						Preferences.Name
					FROM
						Preferences
					LEFT JOIN
						Posts
					ON
						Preferences.Name = Posts.Id
					WHERE
						Preferences.Category = :Category
						AND Posts.Id IS null
					LIMIT
						:Limit
				)
				AS t
			)`

	sqlResult, err := s.GetMaster().Exec(query, map[string]interface{}{"Category": model.PREFERENCE_CATEGORY_FLAGGED_POST, "Limit": limit})
	if err != nil {
		return int64(0), err
	}

	rowsAffected, err := sqlResult.RowsAffected()
	if err != nil {
		return int64(0), err
	}

	return rowsAffected, nil
}
