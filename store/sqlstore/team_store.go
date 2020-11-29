// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package sqlstore

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/mattermost/gorp"
	"github.com/mattermost/mattermost-server/v5/model"
	"github.com/mattermost/mattermost-server/v5/store"
	"github.com/mattermost/mattermost-server/v5/utils"
)

const (
	TEAM_MEMBER_EXISTS_ERROR = "store.sql_team.save_member.exists.app_error"
)

type SqlTeamStore struct {
	SqlStore
}

type teamMember struct {
	TeamId      string
	UserId      string
	Roles       string
	DeleteAt    int64
	SchemeUser  sql.NullBool
	SchemeAdmin sql.NullBool
	SchemeGuest sql.NullBool
}

func NewTeamMemberFromModel(tm *model.TeamMember) *teamMember {
	return &teamMember{
		TeamId:      tm.TeamId,
		UserId:      tm.UserId,
		Roles:       tm.ExplicitRoles,
		DeleteAt:    tm.DeleteAt,
		SchemeGuest: sql.NullBool{Valid: true, Bool: tm.SchemeGuest},
		SchemeUser:  sql.NullBool{Valid: true, Bool: tm.SchemeUser},
		SchemeAdmin: sql.NullBool{Valid: true, Bool: tm.SchemeAdmin},
	}
}

type teamMemberWithSchemeRoles struct {
	TeamId                     string
	UserId                     string
	Roles                      string
	DeleteAt                   int64
	SchemeGuest                sql.NullBool
	SchemeUser                 sql.NullBool
	SchemeAdmin                sql.NullBool
	TeamSchemeDefaultGuestRole sql.NullString
	TeamSchemeDefaultUserRole  sql.NullString
	TeamSchemeDefaultAdminRole sql.NullString
}

type teamMemberWithSchemeRolesList []teamMemberWithSchemeRoles

func teamMemberSliceColumns() []string {
	return []string{"TeamId", "UserId", "Roles", "DeleteAt", "SchemeUser", "SchemeAdmin", "SchemeGuest"}
}

func teamMemberToSlice(member *model.TeamMember) []interface{} {
	resultSlice := []interface{}{}
	resultSlice = append(resultSlice, member.TeamId)
	resultSlice = append(resultSlice, member.UserId)
	resultSlice = append(resultSlice, member.ExplicitRoles)
	resultSlice = append(resultSlice, member.DeleteAt)
	resultSlice = append(resultSlice, member.SchemeUser)
	resultSlice = append(resultSlice, member.SchemeAdmin)
	resultSlice = append(resultSlice, member.SchemeGuest)
	return resultSlice
}

func wildcardSearchTerm(term string) string {
	return strings.ToLower("%" + term + "%")
}

type rolesInfo struct {
	roles         []string
	explicitRoles []string
	schemeGuest   bool
	schemeUser    bool
	schemeAdmin   bool
}

func getTeamRoles(schemeGuest, schemeUser, schemeAdmin bool, defaultTeamGuestRole, defaultTeamUserRole, defaultTeamAdminRole string, roles []string) rolesInfo {
	result := rolesInfo{
		roles:         []string{},
		explicitRoles: []string{},
		schemeGuest:   schemeGuest,
		schemeUser:    schemeUser,
		schemeAdmin:   schemeAdmin,
	}
	// Identify any scheme derived roles that are in "Roles" field due to not yet being migrated, and exclude
	// them from ExplicitRoles field.
	for _, role := range roles {
		switch role {
		case model.TEAM_GUEST_ROLE_ID:
			result.schemeGuest = true
		case model.TEAM_USER_ROLE_ID:
			result.schemeUser = true
		case model.TEAM_ADMIN_ROLE_ID:
			result.schemeAdmin = true
		default:
			result.explicitRoles = append(result.explicitRoles, role)
			result.roles = append(result.roles, role)
		}
	}

	// Add any scheme derived roles that are not in the Roles field due to being Implicit from the Scheme, and add
	// them to the Roles field for backwards compatibility reasons.
	var schemeImpliedRoles []string
	if result.schemeGuest {
		if defaultTeamGuestRole != "" {
			schemeImpliedRoles = append(schemeImpliedRoles, defaultTeamGuestRole)
		} else {
			schemeImpliedRoles = append(schemeImpliedRoles, model.TEAM_GUEST_ROLE_ID)
		}
	}
	if result.schemeUser {
		if defaultTeamUserRole != "" {
			schemeImpliedRoles = append(schemeImpliedRoles, defaultTeamUserRole)
		} else {
			schemeImpliedRoles = append(schemeImpliedRoles, model.TEAM_USER_ROLE_ID)
		}
	}
	if result.schemeAdmin {
		if defaultTeamAdminRole != "" {
			schemeImpliedRoles = append(schemeImpliedRoles, defaultTeamAdminRole)
		} else {
			schemeImpliedRoles = append(schemeImpliedRoles, model.TEAM_ADMIN_ROLE_ID)
		}
	}
	for _, impliedRole := range schemeImpliedRoles {
		alreadyThere := false
		for _, role := range result.roles {
			if role == impliedRole {
				alreadyThere = true
			}
		}
		if !alreadyThere {
			result.roles = append(result.roles, impliedRole)
		}
	}
	return result
}

func (db teamMemberWithSchemeRoles) ToModel() *model.TeamMember {
	// Identify any scheme derived roles that are in "Roles" field due to not yet being migrated, and exclude
	// them from ExplicitRoles field.
	schemeGuest := db.SchemeGuest.Valid && db.SchemeGuest.Bool
	schemeUser := db.SchemeUser.Valid && db.SchemeUser.Bool
	schemeAdmin := db.SchemeAdmin.Valid && db.SchemeAdmin.Bool

	defaultTeamGuestRole := ""
	if db.TeamSchemeDefaultGuestRole.Valid {
		defaultTeamGuestRole = db.TeamSchemeDefaultGuestRole.String
	}

	defaultTeamUserRole := ""
	if db.TeamSchemeDefaultUserRole.Valid {
		defaultTeamUserRole = db.TeamSchemeDefaultUserRole.String
	}

	defaultTeamAdminRole := ""
	if db.TeamSchemeDefaultAdminRole.Valid {
		defaultTeamAdminRole = db.TeamSchemeDefaultAdminRole.String
	}

	rolesResult := getTeamRoles(schemeGuest, schemeUser, schemeAdmin, defaultTeamGuestRole, defaultTeamUserRole, defaultTeamAdminRole, strings.Fields(db.Roles))

	tm := &model.TeamMember{
		TeamId:        db.TeamId,
		UserId:        db.UserId,
		Roles:         strings.Join(rolesResult.roles, " "),
		DeleteAt:      db.DeleteAt,
		SchemeGuest:   rolesResult.schemeGuest,
		SchemeUser:    rolesResult.schemeUser,
		SchemeAdmin:   rolesResult.schemeAdmin,
		ExplicitRoles: strings.Join(rolesResult.explicitRoles, " "),
	}
	return tm
}

func (db teamMemberWithSchemeRolesList) ToModel() []*model.TeamMember {
	tms := make([]*model.TeamMember, 0)

	for _, tm := range db {
		tms = append(tms, tm.ToModel())
	}

	return tms
}

func newSqlTeamStore(sqlStore SqlStore) store.TeamStore {
	s := &SqlTeamStore{
		sqlStore,
	}

	for _, db := range sqlStore.GetAllConns() {
		table := db.AddTableWithName(model.Team{}, "Teams").SetKeys(false, "Id")
		table.ColMap("Id").SetMaxSize(26)
		table.ColMap("DisplayName").SetMaxSize(64)
		table.ColMap("Name").SetMaxSize(64).SetUnique(true)
		table.ColMap("Description").SetMaxSize(255)
		table.ColMap("Email").SetMaxSize(128)
		table.ColMap("CompanyName").SetMaxSize(64)
		table.ColMap("AllowedDomains").SetMaxSize(1000)
		table.ColMap("InviteId").SetMaxSize(32)

		tablem := db.AddTableWithName(teamMember{}, "TeamMembers").SetKeys(false, "TeamId", "UserId")
		tablem.ColMap("TeamId").SetMaxSize(26)
		tablem.ColMap("UserId").SetMaxSize(26)
		tablem.ColMap("Roles").SetMaxSize(64)
	}

	return s
}

func (s SqlTeamStore) createIndexesIfNotExists() {
	s.CreateIndexIfNotExists("idx_teams_name", "Teams", "Name")
	s.RemoveIndexIfExists("idx_teams_description", "Teams")
	s.CreateIndexIfNotExists("idx_teams_invite_id", "Teams", "InviteId")
	s.CreateIndexIfNotExists("idx_teams_update_at", "Teams", "UpdateAt")
	s.CreateIndexIfNotExists("idx_teams_create_at", "Teams", "CreateAt")
	s.CreateIndexIfNotExists("idx_teams_delete_at", "Teams", "DeleteAt")
	s.CreateIndexIfNotExists("idx_teams_scheme_id", "Teams", "SchemeId")

	s.CreateIndexIfNotExists("idx_teammembers_team_id", "TeamMembers", "TeamId")
	s.CreateIndexIfNotExists("idx_teammembers_user_id", "TeamMembers", "UserId")
	s.CreateIndexIfNotExists("idx_teammembers_delete_at", "TeamMembers", "DeleteAt")
}

// Save adds the team to the database if a team with the same name does not already
// exist in the database. It returns the team added if the operation is successful.
func (s SqlTeamStore) Save(team *model.Team) (*model.Team, error) {
	if len(team.Id) > 0 {
		return nil, model.NewAppError("SqlTeamStore.Save",
			"store.sql_team.save.existing.app_error", nil, "id="+team.Id, http.StatusBadRequest)
	}

	team.PreSave()

	if err := team.IsValid(); err != nil {
		return nil, err
	}

	if err := s.GetMaster().Insert(team); err != nil {
		if IsUniqueConstraintError(err, []string{"Name", "teams_name_key"}) {
			return nil, err
		}
		return nil, err
	}
	return team, nil
}

// Update updates the details of the team passed as the parameter using the team Id
// if the team exists in the database.
// It returns the updated team if the operation is successful.
func (s SqlTeamStore) Update(team *model.Team) (*model.Team, error) {

	team.PreUpdate()

	if err := team.IsValid(); err != nil {
		return nil, err
	}

	oldResult, err := s.GetMaster().Get(model.Team{}, team.Id)
	if err != nil {
		return nil, err

	}

	if oldResult == nil {
		return nil, err
	}

	oldTeam := oldResult.(*model.Team)
	team.CreateAt = oldTeam.CreateAt
	team.UpdateAt = model.GetMillis()

	count, err := s.GetMaster().Update(team)
	if err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, err
	}

	return team, nil
}

// Get returns from the database the team that matches the id provided as parameter.
// If the team doesn't exist it returns a model.AppError with a
// http.StatusNotFound in the StatusCode field.
func (s SqlTeamStore) Get(id string) (*model.Team, error) {
	obj, err := s.GetReplica().Get(model.Team{}, id)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, err
	}

	return obj.(*model.Team), nil
}

// GetByInviteId returns from the database the team that matches the inviteId provided as parameter.
// If the parameter provided is empty or if there is no match in the database, it returns a model.AppError
// with a http.StatusNotFound in the StatusCode field.
func (s SqlTeamStore) GetByInviteId(inviteId string) (*model.Team, error) {
	team := model.Team{}

	err := s.GetReplica().SelectOne(&team, "SELECT * FROM Teams WHERE InviteId = :InviteId", map[string]interface{}{"InviteId": inviteId})
	if err != nil {
		return nil, err
	}

	if len(inviteId) == 0 || team.InviteId != inviteId {
		return nil, err
	}
	return &team, nil
}

// GetByName returns from the database the team that matches the name provided as parameter.
// If there is no match in the database, it returns a model.AppError with a
// http.StatusNotFound in the StatusCode field.
func (s SqlTeamStore) GetByName(name string) (*model.Team, error) {

	team := model.Team{}

	err := s.GetReplica().SelectOne(&team, "SELECT * FROM Teams WHERE Name = :Name", map[string]interface{}{"Name": name})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		return nil, err
	}
	return &team, nil
}

func (s SqlTeamStore) GetByNames(names []string) ([]*model.Team, error) {
	uniqueNames := utils.RemoveDuplicatesFromStringArray(names)

	query := s.getQueryBuilder().
		Select("*").
		From("Teams").
		Where(sq.Eq{"Name": uniqueNames})

	queryString, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	teams := []*model.Team{}
	_, err = s.GetReplica().Select(&teams, queryString, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		return nil, err
	}
	if len(teams) != len(uniqueNames) {
		return nil, err
	}
	return teams, nil
}

// SearchAll returns from the database a list of teams that match the Name or DisplayName
// passed as the term search parameter.
func (s SqlTeamStore) SearchAll(term string) ([]*model.Team, error) {
	var teams []*model.Team

	term = sanitizeSearchTerm(term, "\\")
	term = wildcardSearchTerm(term)

	operatorKeyword := "ILIKE"
	if s.DriverName() == model.DATABASE_DRIVER_MYSQL {
		operatorKeyword = "LIKE"
	}
	queryString := fmt.Sprintf("SELECT * FROM Teams WHERE Name %[1]s :Term OR DisplayName %[1]s :Term", operatorKeyword)
	if _, err := s.GetReplica().Select(&teams, queryString, map[string]interface{}{"Term": term}); err != nil {
		return nil, err
	}

	return teams, nil
}

// SearchAllPaged returns a teams list and the total count of teams that matched the search.
func (s SqlTeamStore) SearchAllPaged(term string, page int, perPage int) ([]*model.Team, int64, error) {
	var teams []*model.Team
	var totalCount int64
	offset := page * perPage

	term = sanitizeSearchTerm(term, "\\")
	term = wildcardSearchTerm(term)
	operatorKeyword := "ILIKE"
	if s.DriverName() == model.DATABASE_DRIVER_MYSQL {
		operatorKeyword = "LIKE"
	}
	queryString := fmt.Sprintf("SELECT * FROM Teams WHERE Name %[1]s :Term OR DisplayName %[1]s :Term ORDER BY DisplayName, Name LIMIT :Limit  OFFSET :Offset", operatorKeyword)
	if _, err := s.GetReplica().Select(&teams, queryString, map[string]interface{}{"Term": term, "Limit": perPage, "Offset": offset}); err != nil {
		return nil, 0, err
	}

	queryString = fmt.Sprintf("SELECT COUNT(*) FROM Teams WHERE Name %[1]s :Term OR DisplayName %[1]s :Term", operatorKeyword)
	totalCount, err := s.GetReplica().SelectInt(queryString, map[string]interface{}{"Term": term})
	if err != nil {
		return nil, 0, err
	}

	return teams, totalCount, nil
}

// SearchOpen returns from the database a list of public teams that match the Name or DisplayName
// passed as the term search parameter.
func (s SqlTeamStore) SearchOpen(term string) ([]*model.Team, error) {
	var teams []*model.Team

	term = sanitizeSearchTerm(term, "\\")
	term = wildcardSearchTerm(term)
	operatorKeyword := "ILIKE"
	if s.DriverName() == model.DATABASE_DRIVER_MYSQL {
		operatorKeyword = "LIKE"
	}
	queryString := fmt.Sprintf("SELECT * FROM Teams WHERE Type = 'O' AND AllowOpenInvite = true AND (Name %[1]s :Term OR DisplayName %[1]s :Term)", operatorKeyword)
	if _, err := s.GetReplica().Select(&teams, queryString, map[string]interface{}{"Term": term}); err != nil {
		return nil, err
	}

	return teams, nil
}

// SearchPrivate returns from the database a list of private teams that match the Name or DisplayName
// passed as the term search parameter.
func (s SqlTeamStore) SearchPrivate(term string) ([]*model.Team, error) {
	var teams []*model.Team

	term = sanitizeSearchTerm(term, "\\")
	term = wildcardSearchTerm(term)
	operatorKeyword := "ILIKE"
	if s.DriverName() == model.DATABASE_DRIVER_MYSQL {
		operatorKeyword = "LIKE"
	}
	query := fmt.Sprintf(`
	SELECT *
		FROM
			Teams
		WHERE
			(Type != 'O' OR AllowOpenInvite = false) AND
			(Name %[1]s :Term OR DisplayName %[1]s :Term)`, operatorKeyword)
	if _, err := s.GetReplica().Select(&teams, query, map[string]interface{}{"Term": term}); err != nil {
		return nil, err
	}
	return teams, nil
}

// GetAll returns all teams
func (s SqlTeamStore) GetAll() ([]*model.Team, error) {
	var teams []*model.Team

	_, err := s.GetReplica().Select(&teams, "SELECT * FROM Teams ORDER BY DisplayName")
	if err != nil {
		return nil, err
	}
	return teams, nil
}

// GetAllPage returns teams, up to a total limit passed as parameter and paginated by offset number passed as parameter.
func (s SqlTeamStore) GetAllPage(offset int, limit int) ([]*model.Team, error) {
	var teams []*model.Team

	if _, err := s.GetReplica().Select(&teams,
		`SELECT
			*
		FROM
			Teams
		ORDER BY
			DisplayName
		LIMIT
			:Limit
		OFFSET
			:Offset`, map[string]interface{}{"Offset": offset, "Limit": limit}); err != nil {
		return nil, model.NewAppError("SqlTeamStore.GetAllTeams",
			"store.sql_team.get_all.app_error", nil, err.Error(), http.StatusInternalServerError)
	}

	return teams, nil
}

// GetTeamsByUserId returns from the database all teams that userId belongs to.
func (s SqlTeamStore) GetTeamsByUserId(userId string) ([]*model.Team, error) {
	var teams []*model.Team
	if _, err := s.GetReplica().Select(&teams, "SELECT Teams.* FROM Teams, TeamMembers WHERE TeamMembers.TeamId = Teams.Id AND TeamMembers.UserId = :UserId AND TeamMembers.DeleteAt = 0 AND Teams.DeleteAt = 0", map[string]interface{}{"UserId": userId}); err != nil {
		return nil, err
	}

	return teams, nil
}

// GetAllPrivateTeamListing returns all private teams.
func (s SqlTeamStore) GetAllPrivateTeamListing() ([]*model.Team, error) {
	query := "SELECT * FROM Teams WHERE AllowOpenInvite = 0 ORDER BY DisplayName"

	if s.DriverName() == model.DATABASE_DRIVER_POSTGRES {
		query = "SELECT * FROM Teams WHERE AllowOpenInvite = false ORDER BY DisplayName"
	}

	var data []*model.Team
	if _, err := s.GetReplica().Select(&data, query); err != nil {
		return nil, err
	}

	return data, nil
}

// GetAllPublicTeamPageListing returns public teams, up to a total limit passed as parameter and paginated by offset number passed as parameter.
func (s SqlTeamStore) GetAllPublicTeamPageListing(offset int, limit int) ([]*model.Team, error) {
	query := "SELECT * FROM Teams WHERE AllowOpenInvite = 1 ORDER BY DisplayName LIMIT :Limit OFFSET :Offset"

	if s.DriverName() == model.DATABASE_DRIVER_POSTGRES {
		query = "SELECT * FROM Teams WHERE AllowOpenInvite = true ORDER BY DisplayName LIMIT :Limit OFFSET :Offset"
	}

	var data []*model.Team
	if _, err := s.GetReplica().Select(&data, query, map[string]interface{}{"Offset": offset, "Limit": limit}); err != nil {
		return nil, err
	}

	return data, nil
}

// GetAllPrivateTeamPageListing returns private teams, up to a total limit passed as paramater and paginated by offset number passed as parameter.
func (s SqlTeamStore) GetAllPrivateTeamPageListing(offset int, limit int) ([]*model.Team, error) {
	query := "SELECT * FROM Teams WHERE AllowOpenInvite = 0 ORDER BY DisplayName LIMIT :Limit OFFSET :Offset"

	if s.DriverName() == model.DATABASE_DRIVER_POSTGRES {
		query = "SELECT * FROM Teams WHERE AllowOpenInvite = false ORDER BY DisplayName LIMIT :Limit OFFSET :Offset"
	}

	var data []*model.Team
	if _, err := s.GetReplica().Select(&data, query, map[string]interface{}{"Offset": offset, "Limit": limit}); err != nil {
		return nil, err
	}

	return data, nil
}

// GetAllTeamListing returns all public teams.
func (s SqlTeamStore) GetAllTeamListing() ([]*model.Team, error) {
	query := "SELECT * FROM Teams WHERE AllowOpenInvite = 1 ORDER BY DisplayName"

	if s.DriverName() == model.DATABASE_DRIVER_POSTGRES {
		query = "SELECT * FROM Teams WHERE AllowOpenInvite = true ORDER BY DisplayName"
	}

	var data []*model.Team
	if _, err := s.GetReplica().Select(&data, query); err != nil {
		return nil, err
	}

	return data, nil
}

// GetAllTeamPageListing returns public teams, up to a total limit passed as parameter and paginated by offset number passed as parameter.
func (s SqlTeamStore) GetAllTeamPageListing(offset int, limit int) ([]*model.Team, error) {
	query := "SELECT * FROM Teams WHERE AllowOpenInvite = 1 ORDER BY DisplayName LIMIT :Limit OFFSET :Offset"

	if s.DriverName() == model.DATABASE_DRIVER_POSTGRES {
		query = "SELECT * FROM Teams WHERE AllowOpenInvite = true ORDER BY DisplayName LIMIT :Limit OFFSET :Offset"
	}

	var teams []*model.Team
	if _, err := s.GetReplica().Select(&teams, query, map[string]interface{}{"Offset": offset, "Limit": limit}); err != nil {
		return nil, err
	}

	return teams, nil
}

// PermanentDelete permanently deletes from the database the team entry that matches the teamId passed as parameter.
// To soft-delete the team you can Update it with the DeleteAt field set to the current millisecond using model.GetMillis()
func (s SqlTeamStore) PermanentDelete(teamId string) error {
	if _, err := s.GetMaster().Exec("DELETE FROM Teams WHERE Id = :TeamId", map[string]interface{}{"TeamId": teamId}); err != nil {
		return err
	}
	return nil
}

// AnalyticsPublicTeamCount returns the number of active public teams.
func (s SqlTeamStore) AnalyticsPublicTeamCount() (int64, error) {

	c, err := s.GetReplica().SelectInt("SELECT COUNT(*) FROM Teams WHERE DeleteAt = 0 AND AllowOpenInvite = 1", map[string]interface{}{})

	if s.DriverName() == model.DATABASE_DRIVER_POSTGRES {
		c, err = s.GetReplica().SelectInt("SELECT COUNT(*) FROM Teams WHERE DeleteAt = 0 AND AllowOpenInvite = true", map[string]interface{}{})
	}

	if err != nil {
		return int64(0), err
	}

	return c, nil
}

// AnalyticsPrivateTeamCount returns the number of active private teams.
func (s SqlTeamStore) AnalyticsPrivateTeamCount() (int64, error) {
	c, err := s.GetReplica().SelectInt("SELECT COUNT(*) FROM Teams WHERE DeleteAt = 0 AND AllowOpenInvite = 0", map[string]interface{}{})

	if s.DriverName() == model.DATABASE_DRIVER_POSTGRES {
		c, err = s.GetReplica().SelectInt("SELECT COUNT(*) FROM Teams WHERE DeleteAt = 0 AND AllowOpenInvite = false", map[string]interface{}{})
	}

	if err != nil {
		return int64(0), err
	}

	return c, nil
}

// AnalyticsTeamCount returns the total number of teams including deleted teams if parameter passed is set to 'true'.
func (s SqlTeamStore) AnalyticsTeamCount(includeDeleted bool) (int64, error) {
	query := s.getQueryBuilder().Select("COUNT(*) FROM Teams")
	if !includeDeleted {
		query = query.Where(sq.Eq{"DeleteAt": 0})
	}

	queryString, args, err := query.ToSql()
	if err != nil {
		return 0, err
	}

	c, err := s.GetReplica().SelectInt(queryString, args...)

	if err != nil {
		return int64(0), err
	}

	return c, nil
}

func (s SqlTeamStore) getTeamMembersWithSchemeSelectQuery() sq.SelectBuilder {
	return s.getQueryBuilder().
		Select(
			"TeamMembers.*",
			"TeamScheme.DefaultTeamGuestRole TeamSchemeDefaultGuestRole",
			"TeamScheme.DefaultTeamUserRole TeamSchemeDefaultUserRole",
			"TeamScheme.DefaultTeamAdminRole TeamSchemeDefaultAdminRole",
		).
		From("TeamMembers").
		LeftJoin("Teams ON TeamMembers.TeamId = Teams.Id").
		LeftJoin("Schemes TeamScheme ON Teams.SchemeId = TeamScheme.Id")
}

func (s SqlTeamStore) SaveMultipleMembers(members []*model.TeamMember, maxUsersPerTeam int) ([]*model.TeamMember, error) {
	newTeamMembers := map[string]int{}
	users := map[string]bool{}
	for _, member := range members {
		newTeamMembers[member.TeamId] = 0
	}

	for _, member := range members {
		newTeamMembers[member.TeamId]++
		users[member.UserId] = true

		if err := member.IsValid(); err != nil {
			return nil, err
		}
	}

	teams := []string{}
	for team := range newTeamMembers {
		teams = append(teams, team)
	}

	defaultTeamRolesByTeam := map[string]struct {
		Id    string
		Guest sql.NullString
		User  sql.NullString
		Admin sql.NullString
	}{}

	queryRoles := s.getQueryBuilder().
		Select(
			"Teams.Id as Id",
			"TeamScheme.DefaultTeamGuestRole as Guest",
			"TeamScheme.DefaultTeamUserRole as User",
			"TeamScheme.DefaultTeamAdminRole as Admin",
		).
		From("Teams").
		LeftJoin("Schemes TeamScheme ON Teams.SchemeId = TeamScheme.Id").
		Where(sq.Eq{"Teams.Id": teams})

	sqlRolesQuery, argsRoles, err := queryRoles.ToSql()
	if err != nil {
		return nil, err
	}
	var defaultTeamsRoles []struct {
		Id    string
		Guest sql.NullString
		User  sql.NullString
		Admin sql.NullString
	}
	_, err = s.GetMaster().Select(&defaultTeamsRoles, sqlRolesQuery, argsRoles...)
	if err != nil {
		return nil, err
	}

	for _, defaultRoles := range defaultTeamsRoles {
		defaultTeamRolesByTeam[defaultRoles.Id] = defaultRoles
	}

	if maxUsersPerTeam >= 0 {
		queryCount := s.getQueryBuilder().
			Select(
				"COUNT(0) as Count, TeamMembers.TeamId as TeamId",
			).
			From("TeamMembers").
			Join("Users ON TeamMembers.UserId = Users.Id").
			Where(sq.Eq{"TeamMembers.TeamId": teams}).
			Where(sq.Eq{"TeamMembers.DeleteAt": 0}).
			Where(sq.Eq{"Users.DeleteAt": 0}).
			GroupBy("TeamMembers.TeamId")

		sqlCountQuery, argsCount, errCount := queryCount.ToSql()
		if errCount != nil {
			return nil, err
		}

		var counters []struct {
			Count  int    `db:"Count"`
			TeamId string `db:"TeamId"`
		}

		_, err = s.GetMaster().Select(&counters, sqlCountQuery, argsCount...)
		if err != nil {
			return nil, err
		}

		for teamId, newMembers := range newTeamMembers {
			existingMembers := 0
			for _, counter := range counters {
				if counter.TeamId == teamId {
					existingMembers = counter.Count
				}
			}
			if existingMembers+newMembers > maxUsersPerTeam {
				return nil, err
			}
		}
	}

	query := s.getQueryBuilder().Insert("TeamMembers").Columns(teamMemberSliceColumns()...)
	for _, member := range members {
		query = query.Values(teamMemberToSlice(member)...)
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	if _, err := s.GetMaster().Exec(sql, args...); err != nil {
		if IsUniqueConstraintError(err, []string{"TeamId", "teammembers_pkey", "PRIMARY"}) {
			return nil, err
		}
		return nil, err
	}

	newMembers := []*model.TeamMember{}
	for _, member := range members {
		s.InvalidateAllTeamIdsForUser(member.UserId)
		defaultTeamGuestRole := defaultTeamRolesByTeam[member.TeamId].Guest.String
		defaultTeamUserRole := defaultTeamRolesByTeam[member.TeamId].User.String
		defaultTeamAdminRole := defaultTeamRolesByTeam[member.TeamId].Admin.String
		rolesResult := getTeamRoles(member.SchemeGuest, member.SchemeUser, member.SchemeAdmin, defaultTeamGuestRole, defaultTeamUserRole, defaultTeamAdminRole, strings.Fields(member.ExplicitRoles))
		newMember := *member
		newMember.SchemeGuest = rolesResult.schemeGuest
		newMember.SchemeUser = rolesResult.schemeUser
		newMember.SchemeAdmin = rolesResult.schemeAdmin
		newMember.Roles = strings.Join(rolesResult.roles, " ")
		newMember.ExplicitRoles = strings.Join(rolesResult.explicitRoles, " ")
		newMembers = append(newMembers, &newMember)
	}

	return newMembers, nil
}

func (s SqlTeamStore) SaveMember(member *model.TeamMember, maxUsersPerTeam int) (*model.TeamMember, error) {
	members, err := s.SaveMultipleMembers([]*model.TeamMember{member}, maxUsersPerTeam)
	if err != nil {
		return nil, err
	}
	return members[0], nil
}

func (s SqlTeamStore) UpdateMultipleMembers(members []*model.TeamMember) ([]*model.TeamMember, error) {
	teams := []string{}
	for _, member := range members {
		member.PreUpdate()

		if err := member.IsValid(); err != nil {
			return nil, err
		}

		if _, err := s.GetMaster().Update(NewTeamMemberFromModel(member)); err != nil {
			return nil, err
		}
		teams = append(teams, member.TeamId)
	}

	query := s.getQueryBuilder().
		Select(
			"Teams.Id as Id",
			"TeamScheme.DefaultTeamGuestRole as Guest",
			"TeamScheme.DefaultTeamUserRole as User",
			"TeamScheme.DefaultTeamAdminRole as Admin",
		).
		From("Teams").
		LeftJoin("Schemes TeamScheme ON Teams.SchemeId = TeamScheme.Id").
		Where(sq.Eq{"Teams.Id": teams})

	sqlQuery, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}
	var defaultTeamsRoles []struct {
		Id    string
		Guest sql.NullString
		User  sql.NullString
		Admin sql.NullString
	}
	_, err = s.GetMaster().Select(&defaultTeamsRoles, sqlQuery, args...)
	if err != nil {
		return nil, err
	}

	defaultTeamRolesByTeam := map[string]struct {
		Id    string
		Guest sql.NullString
		User  sql.NullString
		Admin sql.NullString
	}{}
	for _, defaultRoles := range defaultTeamsRoles {
		defaultTeamRolesByTeam[defaultRoles.Id] = defaultRoles
	}

	updatedMembers := []*model.TeamMember{}
	for _, member := range members {
		s.InvalidateAllTeamIdsForUser(member.UserId)
		defaultTeamGuestRole := defaultTeamRolesByTeam[member.TeamId].Guest.String
		defaultTeamUserRole := defaultTeamRolesByTeam[member.TeamId].User.String
		defaultTeamAdminRole := defaultTeamRolesByTeam[member.TeamId].Admin.String
		rolesResult := getTeamRoles(member.SchemeGuest, member.SchemeUser, member.SchemeAdmin, defaultTeamGuestRole, defaultTeamUserRole, defaultTeamAdminRole, strings.Fields(member.ExplicitRoles))
		updatedMember := *member
		updatedMember.SchemeGuest = rolesResult.schemeGuest
		updatedMember.SchemeUser = rolesResult.schemeUser
		updatedMember.SchemeAdmin = rolesResult.schemeAdmin
		updatedMember.Roles = strings.Join(rolesResult.roles, " ")
		updatedMember.ExplicitRoles = strings.Join(rolesResult.explicitRoles, " ")
		updatedMembers = append(updatedMembers, &updatedMember)
	}

	return updatedMembers, nil
}

func (s SqlTeamStore) UpdateMember(member *model.TeamMember) (*model.TeamMember, error) {
	members, err := s.UpdateMultipleMembers([]*model.TeamMember{member})
	if err != nil {
		return nil, err
	}
	return members[0], nil
}

func (s SqlTeamStore) GetMember(teamId string, userId string) (*model.TeamMember, error) {
	query := s.getTeamMembersWithSchemeSelectQuery().
		Where(sq.Eq{"TeamMembers.TeamId": teamId}).
		Where(sq.Eq{"TeamMembers.UserId": userId})

	queryString, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	var dbMember teamMemberWithSchemeRoles
	err = s.GetReplica().SelectOne(&dbMember, queryString, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		return nil, err
	}

	return dbMember.ToModel(), nil
}

func (s SqlTeamStore) GetMembers(teamId string, offset int, limit int, teamMembersGetOptions *model.TeamMembersGetOptions) ([]*model.TeamMember, error) {
	query := s.getTeamMembersWithSchemeSelectQuery().
		Where(sq.Eq{"TeamMembers.TeamId": teamId}).
		Where(sq.Eq{"TeamMembers.DeleteAt": 0}).
		Limit(uint64(limit)).
		Offset(uint64(offset))

	if teamMembersGetOptions == nil || teamMembersGetOptions.Sort == "" {
		query = query.OrderBy("UserId")
	}

	if teamMembersGetOptions != nil {
		if teamMembersGetOptions.Sort == model.USERNAME || teamMembersGetOptions.ExcludeDeletedUsers {
			query = query.LeftJoin("Users ON TeamMembers.UserId = Users.Id")
		}

		if teamMembersGetOptions.ExcludeDeletedUsers {
			query = query.Where(sq.Eq{"Users.DeleteAt": 0})
		}

		if teamMembersGetOptions.Sort == model.USERNAME {
			query = query.OrderBy(model.USERNAME)
		}

		query = applyTeamMemberViewRestrictionsFilter(query, teamId, teamMembersGetOptions.ViewRestrictions)
	}

	queryString, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	var dbMembers teamMemberWithSchemeRolesList
	_, err = s.GetReplica().Select(&dbMembers, queryString, args...)
	if err != nil {
		return nil, err
	}

	return dbMembers.ToModel(), nil
}

func (s SqlTeamStore) GetTotalMemberCount(teamId string, restrictions *model.ViewUsersRestrictions) (int64, error) {
	query := s.getQueryBuilder().
		Select("count(DISTINCT TeamMembers.UserId)").
		From("TeamMembers, Users").
		Where("TeamMembers.DeleteAt = 0").
		Where("TeamMembers.UserId = Users.Id").
		Where(sq.Eq{"TeamMembers.TeamId": teamId})

	query = applyTeamMemberViewRestrictionsFilterForStats(query, teamId, restrictions)
	queryString, args, err := query.ToSql()
	if err != nil {
		return int64(0), err
	}

	count, err := s.GetReplica().SelectInt(queryString, args...)
	if err != nil {
		return int64(0), err
	}
	return count, nil
}

func (s SqlTeamStore) GetActiveMemberCount(teamId string, restrictions *model.ViewUsersRestrictions) (int64, error) {
	query := s.getQueryBuilder().
		Select("count(DISTINCT TeamMembers.UserId)").
		From("TeamMembers, Users").
		Where("TeamMembers.DeleteAt = 0").
		Where("TeamMembers.UserId = Users.Id").
		Where("Users.DeleteAt = 0").
		Where(sq.Eq{"TeamMembers.TeamId": teamId})

	query = applyTeamMemberViewRestrictionsFilterForStats(query, teamId, restrictions)
	queryString, args, err := query.ToSql()
	if err != nil {
		return 0, err
	}

	count, err := s.GetReplica().SelectInt(queryString, args...)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (s SqlTeamStore) GetMembersByIds(teamId string, userIds []string, restrictions *model.ViewUsersRestrictions) ([]*model.TeamMember, error) {
	if len(userIds) == 0 {
		return nil, err
	}

	query := s.getTeamMembersWithSchemeSelectQuery().
		Where(sq.Eq{"TeamMembers.TeamId": teamId}).
		Where(sq.Eq{"TeamMembers.UserId": userIds}).
		Where(sq.Eq{"TeamMembers.DeleteAt": 0})

	query = applyTeamMemberViewRestrictionsFilter(query, teamId, restrictions)

	queryString, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	var dbMembers teamMemberWithSchemeRolesList
	if _, err := s.GetReplica().Select(&dbMembers, queryString, args...); err != nil {
		return nil, err
	}
	return dbMembers.ToModel(), nil
}

func (s SqlTeamStore) GetTeamsForUser(userId string) ([]*model.TeamMember, error) {
	query := s.getTeamMembersWithSchemeSelectQuery().
		Where(sq.Eq{"TeamMembers.UserId": userId})

	queryString, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	var dbMembers teamMemberWithSchemeRolesList
	_, err = s.GetReplica().Select(&dbMembers, queryString, args...)
	if err != nil {
		return nil, err
	}

	return dbMembers.ToModel(), nil
}

func (s SqlTeamStore) GetTeamsForUserWithPagination(userId string, page, perPage int) ([]*model.TeamMember, error) {
	query := s.getTeamMembersWithSchemeSelectQuery().
		Where(sq.Eq{"TeamMembers.UserId": userId}).
		Limit(uint64(perPage)).
		Offset(uint64(page * perPage))

	queryString, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	var dbMembers teamMemberWithSchemeRolesList
	_, err = s.GetReplica().Select(&dbMembers, queryString, args...)
	if err != nil {
		return nil, err
	}

	return dbMembers.ToModel(), nil
}

func (s SqlTeamStore) GetChannelUnreadsForAllTeams(excludeTeamId, userId string) ([]*model.ChannelUnread, error) {
	var data []*model.ChannelUnread
	_, err := s.GetReplica().Select(&data,
		`SELECT
			Channels.TeamId TeamId, Channels.Id ChannelId, (Channels.TotalMsgCount - ChannelMembers.MsgCount) MsgCount, ChannelMembers.MentionCount MentionCount, ChannelMembers.NotifyProps NotifyProps
		FROM
			Channels, ChannelMembers
		WHERE
			Id = ChannelId
			AND UserId = :UserId
			AND DeleteAt = 0
			AND TeamId != :TeamId`,
		map[string]interface{}{"UserId": userId, "TeamId": excludeTeamId})

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s SqlTeamStore) GetChannelUnreadsForTeam(teamId, userId string) ([]*model.ChannelUnread, error) {
	query := `
		SELECT
			Channels.TeamId TeamId, Channels.Id ChannelId, (Channels.TotalMsgCount - ChannelMembers.MsgCount) MsgCount, ChannelMembers.MentionCount MentionCount, ChannelMembers.NotifyProps NotifyProps
		FROM
			Channels, ChannelMembers
		WHERE
			Id = ChannelId
			AND UserId = :UserId
			AND TeamId = :TeamId
			AND DeleteAt = 0`

	var channels []*model.ChannelUnread
	_, err := s.GetReplica().Select(&channels, query, map[string]interface{}{"TeamId": teamId, "UserId": userId})

	if err != nil {
		return nil, err
	}
	return channels, nil
}

func (s SqlTeamStore) RemoveMembers(teamId string, userIds []string) error {
	query := s.getQueryBuilder().
		Delete("TeamMembers").
		Where(sq.Eq{"TeamId": teamId}).
		Where(sq.Eq{"UserId": userIds})

	sql, args, err := query.ToSql()
	if err != nil {
		return err
	}
	_, err = s.GetMaster().Exec(sql, args...)
	if err != nil {
		return err
	}
	return nil
}

// RemoveMember remove from the database the team members that match the userId and teamId passed as parameter.
func (s SqlTeamStore) RemoveMember(teamId string, userId string) error {
	return s.RemoveMembers(teamId, []string{userId})
}

// RemoveAllMembersByTeam removes from the database the team members that belong to the teamId passed as parameter.
func (s SqlTeamStore) RemoveAllMembersByTeam(teamId string) error {
	_, err := s.GetMaster().Exec("DELETE FROM TeamMembers WHERE TeamId = :TeamId", map[string]interface{}{"TeamId": teamId})
	if err != nil {
		return err
	}
	return nil
}

// RemoveAllMembersByUser removes from the database the team members that match the userId passed as parameter.
func (s SqlTeamStore) RemoveAllMembersByUser(userId string) error {
	_, err := s.GetMaster().Exec("DELETE FROM TeamMembers WHERE UserId = :UserId", map[string]interface{}{"UserId": userId})
	if err != nil {
		return err
	}
	return nil
}

func (s SqlTeamStore) UpdateLastTeamIconUpdate(teamId string, curTime int64) error {
	if _, err := s.GetMaster().Exec("UPDATE Teams SET LastTeamIconUpdate = :Time, UpdateAt = :Time WHERE Id = :teamId", map[string]interface{}{"Time": curTime, "teamId": teamId}); err != nil {
		return err
	}
	return nil
}

// GetTeamsByScheme returns from the database all teams that match the schemeId provided as parameter, up to
// a total limit passed as paramater and paginated by offset number passed as parameter.
func (s SqlTeamStore) GetTeamsByScheme(schemeId string, offset int, limit int) ([]*model.Team, error) {
	var teams []*model.Team
	_, err := s.GetReplica().Select(&teams, "SELECT * FROM Teams WHERE SchemeId = :SchemeId ORDER BY DisplayName LIMIT :Limit OFFSET :Offset", map[string]interface{}{"SchemeId": schemeId, "Offset": offset, "Limit": limit})
	if err != nil {
		return nil, err
	}
	return teams, nil
}

// This function does the Advanced Permissions Phase 2 migration for TeamMember objects. It performs the migration
// in batches as a single transaction per batch to ensure consistency but to also minimise execution time to avoid
// causing unnecessary table locks. **THIS FUNCTION SHOULD NOT BE USED FOR ANY OTHER PURPOSE.** Executing this function
// *after* the new Schemes functionality has been used on an installation will have unintended consequences.
func (s SqlTeamStore) MigrateTeamMembers(fromTeamId string, fromUserId string) (map[string]string, error) {
	var transaction *gorp.Transaction
	var err error

	if transaction, err = s.GetMaster().Begin(); err != nil {
		return nil, err
	}
	defer finalizeTransaction(transaction)

	var teamMembers []teamMember
	if _, err := transaction.Select(&teamMembers, "SELECT * from TeamMembers WHERE (TeamId, UserId) > (:FromTeamId, :FromUserId) ORDER BY TeamId, UserId LIMIT 100", map[string]interface{}{"FromTeamId": fromTeamId, "FromUserId": fromUserId}); err != nil {
		return nil, err
	}

	if len(teamMembers) == 0 {
		// No more team members in query result means that the migration has finished.
		return nil, nil
	}

	for i := range teamMembers {
		member := teamMembers[i]
		roles := strings.Fields(member.Roles)
		var newRoles []string
		if !member.SchemeAdmin.Valid {
			member.SchemeAdmin = sql.NullBool{Bool: false, Valid: true}
		}
		if !member.SchemeUser.Valid {
			member.SchemeUser = sql.NullBool{Bool: false, Valid: true}
		}
		if !member.SchemeGuest.Valid {
			member.SchemeGuest = sql.NullBool{Bool: false, Valid: true}
		}
		for _, role := range roles {
			if role == model.TEAM_ADMIN_ROLE_ID {
				member.SchemeAdmin = sql.NullBool{Bool: true, Valid: true}
			} else if role == model.TEAM_USER_ROLE_ID {
				member.SchemeUser = sql.NullBool{Bool: true, Valid: true}
			} else if role == model.TEAM_GUEST_ROLE_ID {
				member.SchemeGuest = sql.NullBool{Bool: true, Valid: true}
			} else {
				newRoles = append(newRoles, role)
			}
		}
		member.Roles = strings.Join(newRoles, " ")

		if _, err := transaction.Update(&member); err != nil {
			return nil, err
		}

	}

	if err := transaction.Commit(); err != nil {
		return nil, err
	}

	data := make(map[string]string)
	data["TeamId"] = teamMembers[len(teamMembers)-1].TeamId
	data["UserId"] = teamMembers[len(teamMembers)-1].UserId

	return data, nil
}

func (s SqlTeamStore) ResetAllTeamSchemes() error {
	if _, err := s.GetMaster().Exec("UPDATE Teams SET SchemeId=''"); err != nil {
		return err
	}
	return nil
}

func (s SqlTeamStore) ClearCaches() {}

func (s SqlTeamStore) InvalidateAllTeamIdsForUser(userId string) {}

func (s SqlTeamStore) ClearAllCustomRoleAssignments() error {

	builtInRoles := model.MakeDefaultRoles()
	lastUserId := strings.Repeat("0", 26)
	lastTeamId := strings.Repeat("0", 26)

	for {
		var transaction *gorp.Transaction
		var err error

		if transaction, err = s.GetMaster().Begin(); err != nil {
			return err
		}
		defer finalizeTransaction(transaction)

		var teamMembers []*teamMember
		if _, err := transaction.Select(&teamMembers, "SELECT * from TeamMembers WHERE (TeamId, UserId) > (:TeamId, :UserId) ORDER BY TeamId, UserId LIMIT 1000", map[string]interface{}{"TeamId": lastTeamId, "UserId": lastUserId}); err != nil {
			return err
		}

		if len(teamMembers) == 0 {
			break
		}

		for _, member := range teamMembers {
			lastUserId = member.UserId
			lastTeamId = member.TeamId

			var newRoles []string

			for _, role := range strings.Fields(member.Roles) {
				for name := range builtInRoles {
					if name == role {
						newRoles = append(newRoles, role)
						break
					}
				}
			}

			newRolesString := strings.Join(newRoles, " ")
			if newRolesString != member.Roles {
				if _, err := transaction.Exec("UPDATE TeamMembers SET Roles = :Roles WHERE UserId = :UserId AND TeamId = :TeamId", map[string]interface{}{"Roles": newRolesString, "TeamId": member.TeamId, "UserId": member.UserId}); err != nil {
					return err
				}
			}
		}

		if err := transaction.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// AnalyticsGetTeamCountForScheme returns the number of active teams that match the schemeId passed as parameter.
func (s SqlTeamStore) AnalyticsGetTeamCountForScheme(schemeId string) (int64, error) {
	count, err := s.GetReplica().SelectInt("SELECT count(*) FROM Teams WHERE SchemeId = :SchemeId AND DeleteAt = 0", map[string]interface{}{"SchemeId": schemeId})
	if err != nil {
		return 0, err
	}

	return count, nil
}

// GetAllForExportAfter returns teams for export, up to a total limit passed as paramater where Teams.Id is greater than the afterId passed as parameter.
func (s SqlTeamStore) GetAllForExportAfter(limit int, afterId string) ([]*model.TeamForExport, error) {
	var data []*model.TeamForExport
	if _, err := s.GetReplica().Select(&data, `
		SELECT
			Teams.*,
			Schemes.Name as SchemeName
		FROM
			Teams
		LEFT JOIN
			Schemes ON Teams.SchemeId = Schemes.Id
		WHERE
			Teams.Id > :AfterId
		ORDER BY
			Id
		LIMIT
			:Limit`,
		map[string]interface{}{"AfterId": afterId, "Limit": limit}); err != nil {
		return nil, err
	}

	return data, nil
}

// GetUserTeamIds get the team ids to which the user belongs to. allowFromCache parameter does not have any effect in this Store
func (s SqlTeamStore) GetUserTeamIds(userID string, allowFromCache bool) ([]string, error) {
	var teamIds []string
	_, err := s.GetReplica().Select(&teamIds,
		`SELECT
			TeamId
		FROM
			TeamMembers
		INNER JOIN
			Teams ON TeamMembers.TeamId = Teams.Id
		WHERE
			TeamMembers.UserId = :UserId
			AND TeamMembers.DeleteAt = 0
			AND Teams.DeleteAt = 0`,
		map[string]interface{}{"UserId": userID})
	if err != nil {
		return []string{}, err
	}

	return teamIds, nil
}

func (s SqlTeamStore) GetTeamMembersForExport(userId string) ([]*model.TeamMemberForExport, error) {
	var members []*model.TeamMemberForExport
	_, err := s.GetReplica().Select(&members, `
		SELECT
			TeamMembers.TeamId,
			TeamMembers.UserId,
			TeamMembers.Roles,
			TeamMembers.DeleteAt,
			(TeamMembers.SchemeGuest IS NOT NULL AND TeamMembers.SchemeGuest) as SchemeGuest,
			TeamMembers.SchemeUser,
			TeamMembers.SchemeAdmin,
			Teams.Name as TeamName
		FROM
			TeamMembers
		INNER JOIN
			Teams ON TeamMembers.TeamId = Teams.Id
		WHERE
			TeamMembers.UserId = :UserId
			AND Teams.DeleteAt = 0`,
		map[string]interface{}{"UserId": userId})
	if err != nil {
		return nil, err
	}
	return members, nil
}

func (s SqlTeamStore) UserBelongsToTeams(userId string, teamIds []string) (bool, error) {
	idQuery := sq.Eq{
		"UserId":   userId,
		"TeamId":   teamIds,
		"DeleteAt": 0,
	}

	query, params, err := s.getQueryBuilder().Select("Count(*)").From("TeamMembers").Where(idQuery).ToSql()
	if err != nil {
		return false, err
	}

	c, err := s.GetReplica().SelectInt(query, params...)
	if err != nil {
		return false, err
	}

	return c > 0, nil
}

func (s SqlTeamStore) UpdateMembersRole(teamID string, userIDs []string) error {
	sql := fmt.Sprintf(`
		UPDATE
			TeamMembers
		SET
			SchemeAdmin = CASE WHEN UserId IN ('%s') THEN
				TRUE
			ELSE
				FALSE
			END
		WHERE
			TeamId = :TeamId
			AND (SchemeGuest = false OR SchemeGuest IS NULL)
			AND DeleteAt = 0`, strings.Join(userIDs, "', '"))

	if _, err := s.GetMaster().Exec(sql, map[string]interface{}{"TeamId": teamID}); err != nil {
		return err
	}

	return nil
}

func applyTeamMemberViewRestrictionsFilter(query sq.SelectBuilder, teamId string, restrictions *model.ViewUsersRestrictions) sq.SelectBuilder {
	if restrictions == nil {
		return query
	}

	// If you have no access to teams or channels, return and empty result.
	if restrictions.Teams != nil && len(restrictions.Teams) == 0 && restrictions.Channels != nil && len(restrictions.Channels) == 0 {
		return query.Where("1 = 0")
	}

	teams := make([]interface{}, len(restrictions.Teams))
	for i, v := range restrictions.Teams {
		teams[i] = v
	}
	channels := make([]interface{}, len(restrictions.Channels))
	for i, v := range restrictions.Channels {
		channels[i] = v
	}

	resultQuery := query.Join("Users ru ON (TeamMembers.UserId = ru.Id)")
	if restrictions.Teams != nil && len(restrictions.Teams) > 0 {
		resultQuery = resultQuery.Join(fmt.Sprintf("TeamMembers rtm ON ( rtm.UserId = ru.Id AND rtm.DeleteAt = 0 AND rtm.TeamId IN (%s))", sq.Placeholders(len(teams))), teams...)
	}
	if restrictions.Channels != nil && len(restrictions.Channels) > 0 {
		resultQuery = resultQuery.Join(fmt.Sprintf("ChannelMembers rcm ON ( rcm.UserId = ru.Id AND rcm.ChannelId IN (%s))", sq.Placeholders(len(channels))), channels...)
	}

	return resultQuery.Distinct()
}

func applyTeamMemberViewRestrictionsFilterForStats(query sq.SelectBuilder, teamId string, restrictions *model.ViewUsersRestrictions) sq.SelectBuilder {
	if restrictions == nil {
		return query
	}

	// If you have no access to teams or channels, return and empty result.
	if restrictions.Teams != nil && len(restrictions.Teams) == 0 && restrictions.Channels != nil && len(restrictions.Channels) == 0 {
		return query.Where("1 = 0")
	}

	teams := make([]interface{}, len(restrictions.Teams))
	for i, v := range restrictions.Teams {
		teams[i] = v
	}
	channels := make([]interface{}, len(restrictions.Channels))
	for i, v := range restrictions.Channels {
		channels[i] = v
	}

	resultQuery := query
	if restrictions.Teams != nil && len(restrictions.Teams) > 0 {
		resultQuery = resultQuery.Join(fmt.Sprintf("TeamMembers rtm ON ( rtm.UserId = Users.Id AND rtm.DeleteAt = 0 AND rtm.TeamId IN (%s))", sq.Placeholders(len(teams))), teams...)
	}
	if restrictions.Channels != nil && len(restrictions.Channels) > 0 {
		resultQuery = resultQuery.Join(fmt.Sprintf("ChannelMembers rcm ON ( rcm.UserId = Users.Id AND rcm.ChannelId IN (%s))", sq.Placeholders(len(channels))), channels...)
	}

	return resultQuery
}

func (s SqlTeamStore) GroupSyncedTeamCount() (int64, error) {
	query := s.getQueryBuilder().Select("COUNT(*)").From("Teams").Where(sq.Eq{"GroupConstrained": true, "DeleteAt": 0})

	sql, args, err := query.ToSql()
	if err != nil {
		return 0, err
	}

	count, err := s.GetReplica().SelectInt(sql, args...)
	if err != nil {
		return 0, err
	}

	return count, nil
}
