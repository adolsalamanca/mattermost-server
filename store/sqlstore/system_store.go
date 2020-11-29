// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package sqlstore

import (
	"context"
	"database/sql"

	"github.com/mattermost/mattermost-server/v5/model"
	"github.com/mattermost/mattermost-server/v5/store"
)

type SqlSystemStore struct {
	SqlStore
}

func newSqlSystemStore(sqlStore SqlStore) store.SystemStore {
	s := &SqlSystemStore{sqlStore}

	for _, db := range sqlStore.GetAllConns() {
		table := db.AddTableWithName(model.System{}, "Systems").SetKeys(false, "Name")
		table.ColMap("Name").SetMaxSize(64)
		table.ColMap("Value").SetMaxSize(1024)
	}

	return s
}

func (s SqlSystemStore) createIndexesIfNotExists() {
}

func (s SqlSystemStore) Save(system *model.System) error {
	if err := s.GetMaster().Insert(system); err != nil {
		return err
	}
	return nil
}

func (s SqlSystemStore) SaveOrUpdate(system *model.System) error {
	if err := s.GetMaster().SelectOne(&model.System{}, "SELECT * FROM Systems WHERE Name = :Name", map[string]interface{}{"Name": system.Name}); err == nil {
		if _, err := s.GetMaster().Update(system); err != nil {
			return err
		}
	} else {
		if err := s.GetMaster().Insert(system); err != nil {
			return err
		}
	}
	return nil
}

func (s SqlSystemStore) Update(system *model.System) error {
	if _, err := s.GetMaster().Update(system); err != nil {
		return err
	}
	return nil
}

func (s SqlSystemStore) Get() (model.StringMap, error) {
	var systems []model.System
	props := make(model.StringMap)
	if _, err := s.GetReplica().Select(&systems, "SELECT * FROM Systems"); err != nil {
		return nil, err
	}
	for _, prop := range systems {
		props[prop.Name] = prop.Value
	}

	return props, nil
}

func (s SqlSystemStore) GetByName(name string) (*model.System, error) {
	var system model.System
	if err := s.GetMaster().SelectOne(&system, "SELECT * FROM Systems WHERE Name = :Name", map[string]interface{}{"Name": name}); err != nil {
		return nil, err
	}

	return &system, nil
}

func (s SqlSystemStore) PermanentDeleteByName(name string) (*model.System, error) {
	var system model.System
	if _, err := s.GetMaster().Exec("DELETE FROM Systems WHERE Name = :Name", map[string]interface{}{"Name": name}); err != nil {
		return nil, err
	}

	return &system, nil
}

// InsertIfExists inserts a given system value if it does not already exist. If a value
// already exists, it returns the old one, else returns the new one.
func (s SqlSystemStore) InsertIfExists(system *model.System) (*model.System, error) {
	tx, err := s.GetMaster().BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return nil, err
	}
	defer finalizeTransaction(tx)

	var origSystem model.System
	if err := tx.SelectOne(&origSystem, `SELECT * FROM Systems
		WHERE Name = :Name`,
		map[string]interface{}{"Name": system.Name}); err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	if origSystem.Value != "" {
		// Already a value exists, return that.
		return &origSystem, nil
	}

	// Key does not exist, need to insert.
	if err := tx.Insert(system); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return system, nil
}
