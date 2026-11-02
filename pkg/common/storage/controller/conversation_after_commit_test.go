package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CreateGroupChatConversation runs inside every invite: its cache clear must follow the commit, or a
// read in between keeps a new member's conversation list without the group for the cache lifetime.

type afterCommitConversationDB struct {
	database.Conversation
	findUserID  func(context.Context, []string, []string) ([]string, error)
	create      func(context.Context, []*model.Conversation) error
	updateByMap func(context.Context, []string, string, map[string]any) (int64, error)
}

func (db afterCommitConversationDB) FindUserID(ctx context.Context, userIDs []string, conversationIDs []string) ([]string, error) {
	return db.findUserID(ctx, userIDs, conversationIDs)
}

func (db afterCommitConversationDB) Create(ctx context.Context, conversations []*model.Conversation) error {
	return db.create(ctx, conversations)
}

func (db afterCommitConversationDB) UpdateByMap(ctx context.Context, userIDs []string, conversationID string, args map[string]any) (int64, error) {
	return db.updateByMap(ctx, userIDs, conversationID, args)
}

type afterCommitConversationCache struct {
	cache.ConversationCache
	invalidate func(context.Context) error
}

func (c *afterCommitConversationCache) CloneConversationCache() cache.ConversationCache { return c }
func (c *afterCommitConversationCache) ChainExecDel(ctx context.Context) error {
	return c.invalidate(ctx)
}
func (c *afterCommitConversationCache) DelConversationIDs(...string) cache.ConversationCache {
	return c
}
func (c *afterCommitConversationCache) DelUserConversationIDsHash(...string) cache.ConversationCache {
	return c
}
func (c *afterCommitConversationCache) DelConversations(string, ...string) cache.ConversationCache {
	return c
}
func (c *afterCommitConversationCache) DelConversationNotReceiveMessageUserIDs(...string) cache.ConversationCache {
	return c
}

func runConversationAfterCommit(t *testing.T, db *conversationDatabase, commitErr error) (int, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), afterCommitKey{}, "request"))
	defer cancel()
	type sessionKey struct{}
	committed, invalidations := false, 0
	db.tx = createGroupTestTx(func(ctx context.Context, callback func(context.Context) error) error {
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
	db.cache = &afterCommitConversationCache{invalidate: func(ctx context.Context) error {
		invalidations++
		assert.True(t, committed, "cache invalidation must follow the transaction commit")
		assert.Nil(t, ctx.Value(sessionKey{}), "do not reuse the completed MongoDB session context")
		assert.Equal(t, "request", ctx.Value(afterCommitKey{}), "keep the caller's context values")
		assert.NoError(t, ctx.Err(), "a cancelled request must not skip the invalidation")
		return nil
	}}
	err := db.CreateGroupChatConversation(ctx, "group", []string{"existing", "new-member"})
	return invalidations, err
}

func newConversationDB(createErr error) *conversationDatabase {
	return &conversationDatabase{conversationDB: afterCommitConversationDB{
		findUserID:  func(context.Context, []string, []string) ([]string, error) { return []string{"existing"}, nil },
		create:      func(context.Context, []*model.Conversation) error { return createErr },
		updateByMap: func(context.Context, []string, string, map[string]any) (int64, error) { return 1, nil },
	}}
}

func TestCreateGroupChatConversationInvalidatesAfterCommit(t *testing.T) {
	n, err := runConversationAfterCommit(t, newConversationDB(nil), nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestCreateGroupChatConversationDoesNotInvalidateOnFailure(t *testing.T) {
	writeErr, commitErr := errors.New("write failed"), errors.New("commit failed")
	n, err := runConversationAfterCommit(t, newConversationDB(writeErr), nil)
	require.ErrorIs(t, err, writeErr)
	assert.Zero(t, n)
	n, err = runConversationAfterCommit(t, newConversationDB(nil), commitErr)
	require.ErrorIs(t, err, commitErr)
	assert.Zero(t, n)
}
