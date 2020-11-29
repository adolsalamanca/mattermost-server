// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package sqlstore

import (
	"database/sql"

	sq "github.com/Masterminds/squirrel"

	"github.com/mattermost/gorp"
	"github.com/mattermost/mattermost-server/v5/model"
	"github.com/mattermost/mattermost-server/v5/store"
)

type SqlJobStore struct {
	SqlStore
}

func newSqlJobStore(sqlStore SqlStore) store.JobStore {
	s := &SqlJobStore{sqlStore}

	for _, db := range sqlStore.GetAllConns() {
		table := db.AddTableWithName(model.Job{}, "Jobs").SetKeys(false, "Id")
		table.ColMap("Id").SetMaxSize(26)
		table.ColMap("Type").SetMaxSize(32)
		table.ColMap("Status").SetMaxSize(32)
		table.ColMap("Data").SetMaxSize(1024)
	}

	return s
}

func (jss SqlJobStore) createIndexesIfNotExists() {
	jss.CreateIndexIfNotExists("idx_jobs_type", "Jobs", "Type")
}

func (jss SqlJobStore) Save(job *model.Job) (*model.Job, error) {
	if err := jss.GetMaster().Insert(job); err != nil {
		return nil, err
	}
	return job, nil
}

func (jss SqlJobStore) UpdateOptimistically(job *model.Job, currentStatus string) (bool, error) {
	sql, args, err := jss.getQueryBuilder().
		Update("Jobs").
		Set("LastActivityAt", model.GetMillis()).
		Set("Status", job.Status).
		Set("Data", job.DataToJson()).
		Set("Progress", job.Progress).
		Where(sq.Eq{"Id": job.Id, "Status": currentStatus}).ToSql()
	if err != nil {
		return false, err
	}
	sqlResult, err := jss.GetMaster().Exec(sql, args...)
	if err != nil {
		return false, err
	}

	rows, err := sqlResult.RowsAffected()

	if err != nil {
		return false, err
	}

	if rows != 1 {
		return false, nil
	}

	return true, nil
}

func (jss SqlJobStore) UpdateStatus(id string, status string) (*model.Job, error) {
	job := &model.Job{
		Id:             id,
		Status:         status,
		LastActivityAt: model.GetMillis(),
	}

	if _, err := jss.GetMaster().UpdateColumns(func(col *gorp.ColumnMap) bool {
		return col.ColumnName == "Status" || col.ColumnName == "LastActivityAt"
	}, job); err != nil {
		return nil, err
	}

	return job, nil
}

func (jss SqlJobStore) UpdateStatusOptimistically(id string, currentStatus string, newStatus string) (bool, error) {
	sql := jss.getQueryBuilder().
		Update("Jobs").
		Set("LastActivityAt", model.GetMillis()).
		Set("Status", newStatus).
		Where(sq.Eq{"Id": id, "Status": currentStatus})

	if newStatus == model.JOB_STATUS_IN_PROGRESS {
		sql = sql.Set("StartAt", model.GetMillis())
	}
	query, args, err := sql.ToSql()
	if err != nil {
		return false, err
	}

	sqlResult, err := jss.GetMaster().Exec(query, args...)
	if err != nil {
		return false, err
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows != 1 {
		return false, nil
	}

	return true, nil
}

func (jss SqlJobStore) Get(id string) (*model.Job, error) {
	query, args, err := jss.getQueryBuilder().
		Select("*").
		From("Jobs").
		Where(sq.Eq{"Id": id}).ToSql()
	if err != nil {
		return nil, err
	}
	var status *model.Job
	if err = jss.GetReplica().SelectOne(&status, query, args...); err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		return nil, err
	}
	return status, nil
}

func (jss SqlJobStore) GetAllPage(offset int, limit int) ([]*model.Job, error) {
	query, args, err := jss.getQueryBuilder().
		Select("*").
		From("Jobs").
		OrderBy("CreateAt DESC").
		Limit(uint64(limit)).
		Offset(uint64(offset)).ToSql()
	if err != nil {
		return nil, err
	}

	var statuses []*model.Job
	if _, err = jss.GetReplica().Select(&statuses, query, args...); err != nil {
		return nil, err
	}
	return statuses, nil
}

func (jss SqlJobStore) GetAllByType(jobType string) ([]*model.Job, error) {
	query, args, err := jss.getQueryBuilder().
		Select("*").
		From("Jobs").
		Where(sq.Eq{"Type": jobType}).
		OrderBy("CreateAt DESC").ToSql()
	if err != nil {
		return nil, err
	}
	var statuses []*model.Job
	if _, err = jss.GetReplica().Select(&statuses, query, args...); err != nil {
		return nil, err
	}
	return statuses, nil
}

func (jss SqlJobStore) GetAllByTypePage(jobType string, offset int, limit int) ([]*model.Job, error) {
	query, args, err := jss.getQueryBuilder().
		Select("*").
		From("Jobs").
		Where(sq.Eq{"Type": jobType}).
		OrderBy("CreateAt DESC").
		Limit(uint64(limit)).
		Offset(uint64(offset)).ToSql()
	if err != nil {
		return nil, err
	}

	var statuses []*model.Job
	if _, err = jss.GetReplica().Select(&statuses, query, args...); err != nil {
		return nil, err
	}
	return statuses, nil
}

func (jss SqlJobStore) GetAllByStatus(status string) ([]*model.Job, error) {
	var statuses []*model.Job
	query, args, err := jss.getQueryBuilder().
		Select("*").
		From("Jobs").
		Where(sq.Eq{"Status": status}).
		OrderBy("CreateAt ASC").ToSql()
	if err != nil {
		return nil, err
	}

	if _, err = jss.GetReplica().Select(&statuses, query, args...); err != nil {
		return nil, err
	}
	return statuses, nil
}

func (jss SqlJobStore) GetNewestJobByStatusAndType(status string, jobType string) (*model.Job, error) {
	query, args, err := jss.getQueryBuilder().
		Select("*").
		From("Jobs").
		Where(sq.Eq{"Status": status, "Type": jobType}).
		OrderBy("CreateAt DESC").
		Limit(1).ToSql()
	if err != nil {
		return nil, err
	}

	var job *model.Job
	if err = jss.GetReplica().SelectOne(&job, query, args...); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return job, nil
}

func (jss SqlJobStore) GetCountByStatusAndType(status string, jobType string) (int64, error) {
	query, args, err := jss.getQueryBuilder().
		Select("COUNT(*)").
		From("Jobs").
		Where(sq.Eq{"Status": status, "Type": jobType}).ToSql()
	if err != nil {
		return 0, err
	}
	count, err := jss.GetReplica().SelectInt(query, args...)
	if err != nil {
		return int64(0), err
	}
	return count, nil
}

func (jss SqlJobStore) Delete(id string) (string, error) {
	sql, args, err := jss.getQueryBuilder().
		Delete("Jobs").
		Where(sq.Eq{"Id": id}).ToSql()
	if err != nil {
		return "", err
	}

	if _, err = jss.GetMaster().Exec(sql, args...); err != nil {
		return "", err
	}
	return id, nil
}
