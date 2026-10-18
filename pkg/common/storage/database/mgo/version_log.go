package mgo

import (
	"context"
	"errors"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/versionctx"
	"github.com/openimsdk/tools/db/mongoutil"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func NewVersionLog(coll *mongo.Collection) (database.VersionLog, error) {
	lm := &VersionLogMgo{coll: coll}
	if err := lm.initIndex(context.Background()); err != nil {
		return nil, errs.WrapMsg(err, "init version log index failed", "coll", coll.Name())
	}
	// Transactions need a replica set (DocumentDB counts as one); a single mongod has none.
	var hello bson.M
	if err := coll.Database().Client().Database("admin").RunCommand(context.Background(), bson.M{"isMaster": 1}).Decode(&hello); err != nil {
		return nil, errs.WrapMsg(err, "check whether mongo supports transactions", "coll", coll.Name())
	}
	_, lm.txOK = hello["setName"]
	return lm, nil
}

type VersionLogMgo struct {
	coll *mongo.Collection
	txOK bool // the server takes transactions (writeLogBatch2 runs its two updates in one)
}

// versionLogBetweenSteps runs between writeLogBatch2's two updates. Tests only: nil in the server.
var versionLogBetweenSteps func() error

func (l *VersionLogMgo) initIndex(ctx context.Context) error {
	_, err := l.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.M{
			"d_id": 1,
		},
		Options: options.Index().SetUnique(true),
	})

	return err
}

func (l *VersionLogMgo) IncrVersion(ctx context.Context, dId string, eIds []string, state int32) error {
	_, err := l.IncrVersionResult(ctx, dId, eIds, state)
	return err
}

func (l *VersionLogMgo) IncrVersionResult(ctx context.Context, dId string, eIds []string, state int32) (*model.VersionLog, error) {
	vl, err := l.incrVersionResult(ctx, dId, eIds, state)
	if err != nil {
		return nil, err
	}
	versionctx.GetVersionLog(ctx).Append(versionctx.Collection{
		Name: l.coll.Name(),
		Doc:  vl,
	})
	return vl, nil
}

func (l *VersionLogMgo) incrVersionResult(ctx context.Context, dId string, eIds []string, state int32) (*model.VersionLog, error) {
	if len(eIds) == 0 {
		return nil, errs.ErrArgs.WrapMsg("elem id is empty", "dId", dId)
	}
	now := time.Now()
	if res, err := l.writeLogBatch2(ctx, dId, eIds, state, now); err == nil {
		return res, nil
	} else if !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, err
	}
	if res, err := l.initDoc(ctx, dId, eIds, state, now); err == nil {
		return res, nil
	} else if !mongo.IsDuplicateKeyError(err) {
		return nil, err
	}
	return l.writeLogBatch2(ctx, dId, eIds, state, now)
}

func (l *VersionLogMgo) initDoc(ctx context.Context, dId string, eIds []string, state int32, now time.Time) (*model.VersionLog, error) {
	wl := model.VersionLogTable{
		ID:         primitive.NewObjectID(),
		DID:        dId,
		Logs:       make([]model.VersionLogElem, 0, len(eIds)),
		Version:    database.FirstVersion,
		Deleted:    database.DefaultDeleteVersion,
		LastUpdate: now,
	}
	for _, eId := range eIds {
		wl.Logs = append(wl.Logs, model.VersionLogElem{
			EID:        eId,
			State:      state,
			Version:    database.FirstVersion,
			LastUpdate: now,
		})
	}
	if _, err := l.coll.InsertOne(ctx, &wl); err != nil {
		return nil, err
	}
	return wl.VersionLog(), nil
}

// writeLogBatch2 bumps the document's version and records eIds at the new version, replacing any
// older entry for them. Upstream does this in one findOneAndUpdate with an aggregation pipeline, which
// Amazon DocumentDB refuses ("Pipeline based document update", 2026-10-18). Here it is two plain updates
// (writeLogSteps), run in one transaction so that they commit together, as the single update did: a
// failure between them must not leave the older entries removed and the new ones missing, or a client
// that syncs by version would never learn of the change. A caller's own transaction (group creation)
// already covers them; without transaction support (a single mongod) they run as two updates.
// Returns mongo.ErrNoDocuments when the document does not exist, as before.
func (l *VersionLogMgo) writeLogBatch2(ctx context.Context, dId string, eIds []string, state int32, now time.Time) (*model.VersionLog, error) {
	if mongo.SessionFromContext(ctx) != nil || !l.txOK {
		return l.writeLogSteps(ctx, dId, eIds, state, now)
	}
	sess, err := l.coll.Database().Client().StartSession()
	if err != nil {
		return nil, errs.WrapMsg(err, "version log start session")
	}
	defer sess.EndSession(ctx)
	res, err := sess.WithTransaction(ctx, func(sctx mongo.SessionContext) (any, error) {
		return l.writeLogSteps(sctx, dId, eIds, state, now)
	})
	if err != nil {
		return nil, err
	}
	return res.(*model.VersionLog), nil
}

// writeLogSteps: remove the older entries and read the version, then add 1 to the version and append
// the entries only if the version is still the one read. Outside a transaction a write that came
// between makes it read again; inside one the first update holds the document, so it cannot.
func (l *VersionLogMgo) writeLogSteps(ctx context.Context, dId string, eIds []string, state int32, now time.Time) (*model.VersionLog, error) {
	if eIds == nil {
		eIds = []string{}
	}
	projection := bson.M{"logs": 0}
	for attempt := 0; attempt < 10; attempt++ {
		pulled, err := mongoutil.FindOneAndUpdate[*model.VersionLog](ctx, l.coll,
			bson.M{"d_id": dId},
			bson.M{"$pull": bson.M{"logs": bson.M{"e_id": bson.M{"$in": eIds}}}},
			options.FindOneAndUpdate().SetUpsert(false).SetReturnDocument(options.After).SetProjection(projection))
		if err != nil {
			return nil, err
		}
		if versionLogBetweenSteps != nil {
			if err := versionLogBetweenSteps(); err != nil {
				return nil, err
			}
		}
		next := pulled.Version + 1
		elems := make([]model.VersionLogElem, 0, len(eIds))
		for _, eId := range eIds {
			elems = append(elems, model.VersionLogElem{EID: eId, Version: next, State: state, LastUpdate: now})
		}
		res, err := mongoutil.FindOneAndUpdate[*model.VersionLog](ctx, l.coll,
			bson.M{"d_id": dId, "version": pulled.Version},
			bson.M{
				"$set":  bson.M{"version": next, "last_update": now},
				"$push": bson.M{"logs": bson.M{"$each": elems}},
			},
			options.FindOneAndUpdate().SetUpsert(false).SetReturnDocument(options.After).SetProjection(projection))
		if errors.Is(err, mongo.ErrNoDocuments) {
			continue // another write changed the version between the two updates: read it again
		}
		if err != nil {
			return nil, err
		}
		res.Logs = elems
		return res, nil
	}
	return nil, errs.WrapMsg(errors.New("version log write kept losing to concurrent writes"), "writeLogBatch2", "dId", dId)
}

func (l *VersionLogMgo) findDoc(ctx context.Context, dId string) (*model.VersionLog, error) {
	vl, err := mongoutil.FindOne[*model.VersionLogTable](ctx, l.coll, bson.M{"d_id": dId}, options.FindOne().SetProjection(bson.M{"logs": 0}))
	if err != nil {
		return nil, err
	}
	return vl.VersionLog(), nil
}

func (l *VersionLogMgo) FindChangeLog(ctx context.Context, dId string, version uint, limit int) (*model.VersionLog, error) {
	if wl, err := l.findChangeLog(ctx, dId, version, limit); err == nil {
		return wl, nil
	} else if !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, err
	}
	log.ZDebug(ctx, "init doc", "dId", dId)
	if res, err := l.initDoc(ctx, dId, nil, 0, time.Now()); err == nil {
		log.ZDebug(ctx, "init doc success", "dId", dId)
		return res, nil
	} else if mongo.IsDuplicateKeyError(err) {
		return l.findChangeLog(ctx, dId, version, limit)
	} else {
		return nil, err
	}
}

func (l *VersionLogMgo) BatchFindChangeLog(ctx context.Context, dIds []string, versions []uint, limits []int) (vLogs []*model.VersionLog, err error) {
	for i := 0; i < len(dIds); i++ {
		if vLog, err := l.findChangeLog(ctx, dIds[i], versions[i], limits[i]); err == nil {
			vLogs = append(vLogs, vLog)
		} else if !errors.Is(err, mongo.ErrNoDocuments) {
			log.ZError(ctx, "findChangeLog error:", errs.Wrap(err))
		}
		log.ZDebug(ctx, "init doc", "dId", dIds[i])
		if res, err := l.initDoc(ctx, dIds[i], nil, 0, time.Now()); err == nil {
			log.ZDebug(ctx, "init doc success", "dId", dIds[i])
			vLogs = append(vLogs, res)
		} else if mongo.IsDuplicateKeyError(err) {
			l.findChangeLog(ctx, dIds[i], versions[i], limits[i])
		} else {
			log.ZError(ctx, "init doc error:", errs.Wrap(err))
		}
	}
	return vLogs, errs.Wrap(err)
}

func (l *VersionLogMgo) findChangeLog(ctx context.Context, dId string, version uint, limit int) (*model.VersionLog, error) {
	if version == 0 && limit == 0 {
		return l.findDoc(ctx, dId)
	}
	pipeline := []bson.M{
		{
			"$match": bson.M{
				"d_id": dId,
			},
		},
		{
			"$addFields": bson.M{
				"logs": bson.M{
					"$cond": bson.M{
						"if": bson.M{
							"$or": []bson.M{
								{"$lt": []any{"$version", version}},
								{"$gte": []any{"$deleted", version}},
							},
						},
						"then": []any{},
						"else": "$logs",
					},
				},
			},
		},
		{
			"$addFields": bson.M{
				"logs": bson.M{
					"$filter": bson.M{
						"input": "$logs",
						"as":    "l",
						"cond": bson.M{
							"$gt": []any{"$$l.version", version},
						},
					},
				},
			},
		},
		{
			"$addFields": bson.M{
				"log_len": bson.M{"$size": "$logs"},
			},
		},
		{
			"$addFields": bson.M{
				"logs": bson.M{
					"$cond": bson.M{
						"if": bson.M{
							"$gt": []any{"$log_len", limit},
						},
						"then": []any{},
						"else": "$logs",
					},
				},
			},
		},
	}
	if limit <= 0 {
		pipeline = pipeline[:len(pipeline)-1]
	}
	vl, err := mongoutil.Aggregate[*model.VersionLog](ctx, l.coll, pipeline)
	if err != nil {
		return nil, err
	}
	if len(vl) == 0 {
		return nil, mongo.ErrNoDocuments
	}
	return vl[0], nil
}

func (l *VersionLogMgo) DeleteAfterUnchangedLog(ctx context.Context, deadline time.Time) error {
	return mongoutil.DeleteMany(ctx, l.coll, bson.M{
		"last_update": bson.M{
			"$lt": deadline,
		},
	})
}

func (l *VersionLogMgo) Delete(ctx context.Context, dId string) error {
	return mongoutil.DeleteOne(ctx, l.coll, bson.M{"d_id": dId})
}
