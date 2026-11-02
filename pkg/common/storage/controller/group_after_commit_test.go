package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cache invalidation after the commit, for the group writes other than CreateGroup, and with a context
// a cancelled request cannot stop (group_test.go covers CreateGroup's ordering).

type afterCommitGroupDB struct {
	database.Group
	updateMap func(context.Context, string, map[string]any) error
}

func (db afterCommitGroupDB) UpdateMap(ctx context.Context, groupID string, args map[string]any) error {
	return db.updateMap(ctx, groupID, args)
}

type afterCommitMemberDB struct {
	database.GroupMember
	create func(context.Context, []*model.GroupMember) error
	incr   func(context.Context, string, []string, int32) error
	delete func(context.Context, string, []string) error
}

func (db afterCommitMemberDB) Create(ctx context.Context, members []*model.GroupMember) error {
	return db.create(ctx, members)
}

func (db afterCommitMemberDB) MemberGroupIncrVersion(ctx context.Context, groupID string, userIDs []string, state int32) error {
	return db.incr(ctx, groupID, userIDs, state)
}

func (db afterCommitMemberDB) Delete(ctx context.Context, groupID string, userIDs []string) error {
	return db.delete(ctx, groupID, userIDs)
}

type afterCommitKey struct{}

// run calls one group write with a request context that is cancelled right after the commit, the case
// a timed-out caller produces. It returns how often the cache was invalidated and whether every
// invalidation came after the commit, outside the session, with the caller's values and no cancellation.
func runAfterCommit(t *testing.T, write func(*groupDatabase, context.Context) error, db *groupDatabase, commitErr error) (int, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), afterCommitKey{}, "request"))
	defer cancel()
	type sessionKey struct{}
	committed := false
	invalidations := 0
	db.ctxTx = createGroupTestTx(func(ctx context.Context, callback func(context.Context) error) error {
		if err := callback(context.WithValue(ctx, sessionKey{}, true)); err != nil {
			return err
		}
		if commitErr != nil {
			return commitErr
		}
		committed = true
		cancel()
		return nil
	})
	db.cache = &createGroupTestCache{invalidate: func(ctx context.Context) error {
		invalidations++
		assert.True(t, committed, "cache invalidation must follow the transaction commit")
		assert.Nil(t, ctx.Value(sessionKey{}), "do not reuse the completed MongoDB session context")
		assert.Equal(t, "request", ctx.Value(afterCommitKey{}), "keep the caller's context values")
		assert.NoError(t, ctx.Err(), "a cancelled request must not skip the invalidation")
		return nil
	}}
	err := write(db, ctx)
	return invalidations, err
}

func TestCreateGroupInvalidatesAfterCommitDespiteCancellation(t *testing.T) {
	db := &groupDatabase{groupMemberDB: afterCommitMemberDB{create: func(context.Context, []*model.GroupMember) error { return nil }}}
	n, err := runAfterCommit(t, func(db *groupDatabase, ctx context.Context) error {
		return db.CreateGroup(ctx, nil, []*model.GroupMember{{GroupID: "group", UserID: "member"}})
	}, db, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestUpdateGroupInvalidatesAfterCommit(t *testing.T) {
	db := &groupDatabase{
		groupDB:       afterCommitGroupDB{updateMap: func(context.Context, string, map[string]any) error { return nil }},
		groupMemberDB: afterCommitMemberDB{incr: func(context.Context, string, []string, int32) error { return nil }},
	}
	n, err := runAfterCommit(t, func(db *groupDatabase, ctx context.Context) error {
		return db.UpdateGroup(ctx, "group", map[string]any{"ex": "{}"})
	}, db, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestDeleteGroupMemberInvalidatesAfterCommit(t *testing.T) {
	db := &groupDatabase{groupMemberDB: afterCommitMemberDB{delete: func(context.Context, string, []string) error { return nil }}}
	n, err := runAfterCommit(t, func(db *groupDatabase, ctx context.Context) error {
		return db.DeleteGroupMember(ctx, "group", []string{"rider"})
	}, db, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestUpdateAndDeleteDoNotInvalidateOnFailure(t *testing.T) {
	writeErr, commitErr := errors.New("write failed"), errors.New("commit failed")
	ok := func(context.Context, string, []string, int32) error { return nil }
	for _, tt := range []struct {
		name      string
		db        *groupDatabase
		write     func(*groupDatabase, context.Context) error
		commitErr error
		wantErr   error
	}{
		{name: "update: group write", wantErr: writeErr,
			db: &groupDatabase{
				groupDB:       afterCommitGroupDB{updateMap: func(context.Context, string, map[string]any) error { return writeErr }},
				groupMemberDB: afterCommitMemberDB{incr: ok}},
			write: func(db *groupDatabase, ctx context.Context) error {
				return db.UpdateGroup(ctx, "group", map[string]any{})
			}},
		{name: "update: version write", wantErr: writeErr,
			db: &groupDatabase{
				groupDB:       afterCommitGroupDB{updateMap: func(context.Context, string, map[string]any) error { return nil }},
				groupMemberDB: afterCommitMemberDB{incr: func(context.Context, string, []string, int32) error { return writeErr }}},
			write: func(db *groupDatabase, ctx context.Context) error {
				return db.UpdateGroup(ctx, "group", map[string]any{})
			}},
		{name: "update: commit", commitErr: commitErr, wantErr: commitErr,
			db: &groupDatabase{
				groupDB:       afterCommitGroupDB{updateMap: func(context.Context, string, map[string]any) error { return nil }},
				groupMemberDB: afterCommitMemberDB{incr: ok}},
			write: func(db *groupDatabase, ctx context.Context) error {
				return db.UpdateGroup(ctx, "group", map[string]any{})
			}},
		{name: "delete: member write", wantErr: writeErr,
			db: &groupDatabase{groupMemberDB: afterCommitMemberDB{delete: func(context.Context, string, []string) error { return writeErr }}},
			write: func(db *groupDatabase, ctx context.Context) error {
				return db.DeleteGroupMember(ctx, "group", []string{"rider"})
			}},
		{name: "delete: commit", commitErr: commitErr, wantErr: commitErr,
			db: &groupDatabase{groupMemberDB: afterCommitMemberDB{delete: func(context.Context, string, []string) error { return nil }}},
			write: func(db *groupDatabase, ctx context.Context) error {
				return db.DeleteGroupMember(ctx, "group", []string{"rider"})
			}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n, err := runAfterCommit(t, tt.write, tt.db, tt.commitErr)
			require.ErrorIs(t, err, tt.wantErr)
			assert.Zero(t, n)
		})
	}
}
