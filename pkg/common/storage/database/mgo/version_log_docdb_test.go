package mgo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// writeLogBatch2 without an aggregation pipeline (Amazon DocumentDB). Needs a MongoDB:
// OPENIM_TEST_MONGO_URI=mongodb://localhost:27017 go test ./pkg/common/storage/database/mgo -run VersionLogNoPipeline
func versionLogForTest(t *testing.T) (*VersionLogMgo, *mongo.Collection) {
	uri := os.Getenv("OPENIM_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("OPENIM_TEST_MONGO_URI not set")
	}
	ctx := context.Background()
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	coll := cli.Database("openim_test").Collection(fmt.Sprintf("version_log_%d", time.Now().UnixNano()))
	t.Cleanup(func() { _ = coll.Drop(ctx); _ = cli.Disconnect(ctx) })
	vl, err := NewVersionLog(coll)
	if err != nil {
		t.Fatal(err)
	}
	return vl.(*VersionLogMgo), coll
}

func readDoc(t *testing.T, coll *mongo.Collection, dId string) model.VersionLogTable {
	var doc model.VersionLogTable
	if err := coll.FindOne(context.Background(), bson.M{"d_id": dId}).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestVersionLogNoPipelineMissingDoc(t *testing.T) {
	vl, _ := versionLogForTest(t)
	if _, err := vl.writeLogBatch2(context.Background(), "none", []string{"a"}, model.VersionStateInsert, time.Now()); !errors.Is(err, mongo.ErrNoDocuments) {
		t.Fatalf("want mongo.ErrNoDocuments for a missing document, got %v", err)
	}
}

func TestVersionLogNoPipelineReplacesEntries(t *testing.T) {
	vl, coll := versionLogForTest(t)
	ctx := context.Background()
	first, err := vl.incrVersionResult(ctx, "g1", []string{"u1", "u2"}, model.VersionStateInsert)
	if err != nil {
		t.Fatal(err)
	}
	res, err := vl.incrVersionResult(ctx, "g1", []string{"u2", "u3"}, model.VersionStateUpdate)
	if err != nil {
		t.Fatal(err)
	}
	if res.Version != first.Version+1 || len(res.Logs) != 2 || res.Logs[0].Version != res.Version {
		t.Fatalf("result: version %d after %d, logs %+v", res.Version, first.Version, res.Logs)
	}
	doc := readDoc(t, coll, "g1")
	got := map[string]model.VersionLogElem{}
	for _, l := range doc.Logs {
		if _, dup := got[l.EID]; dup {
			t.Fatalf("entry %s twice: %+v", l.EID, doc.Logs)
		}
		got[l.EID] = l
	}
	if doc.Version != res.Version || len(got) != 3 ||
		got["u1"].Version != first.Version || got["u1"].State != model.VersionStateInsert ||
		got["u2"].Version != res.Version || got["u2"].State != model.VersionStateUpdate ||
		got["u3"].Version != res.Version {
		t.Fatalf("document: version %d, logs %+v", doc.Version, doc.Logs)
	}
}

func TestVersionLogNoPipelineConcurrentWriters(t *testing.T) {
	vl, coll := versionLogForTest(t)
	ctx := context.Background()
	if _, err := vl.incrVersionResult(ctx, "g2", []string{"seed"}, model.VersionStateInsert); err != nil {
		t.Fatal(err)
	}
	const writers = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := vl.incrVersionResult(ctx, "g2", []string{fmt.Sprintf("m%d", i)}, model.VersionStateInsert)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	doc := readDoc(t, coll, "g2")
	seen := map[uint]bool{}
	for _, l := range doc.Logs {
		if l.EID != "seed" {
			if seen[l.Version] || l.Version > doc.Version {
				t.Fatalf("entry versions not unique or past the document: %d of %d, logs %+v", l.Version, doc.Version, doc.Logs)
			}
			seen[l.Version] = true
		}
	}
	if len(doc.Logs) != writers+1 || len(seen) != writers {
		t.Fatalf("want %d entries with %d distinct versions, got %+v", writers+1, writers, doc.Logs)
	}
}
