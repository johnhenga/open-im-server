package kafka

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/mcontext"
	"google.golang.org/protobuf/proto"
)

// OpenIM's own Kafka paths (producer, both consumer-group commit modes, a rebalance, the startup
// checks) against a real broker, to check a Kafka version before OpenIM runs on it. Needs a broker:
//
//	OPENIM_TEST_KAFKA_ADDR=localhost:9092 go test ./pkg/common/storage/kafka -run Broker -v
//
// Optional: OPENIM_TEST_KAFKA_USER, _PASSWORD, _MECHANISM (as kafka.yml saslMechanism), _TLS=1,
// _CA (CA file), _RF (copies per partition, default 1). Each run makes its own topics, 8 partitions.
func brokerConfigForTest(t *testing.T) *Config {
	addr := os.Getenv("OPENIM_TEST_KAFKA_ADDR")
	if addr == "" {
		t.Skip("OPENIM_TEST_KAFKA_ADDR not set")
	}
	conf := &Config{
		Username:      os.Getenv("OPENIM_TEST_KAFKA_USER"),
		Password:      os.Getenv("OPENIM_TEST_KAFKA_PASSWORD"),
		SASLMechanism: os.Getenv("OPENIM_TEST_KAFKA_MECHANISM"),
		CompressType:  "none",
		Addr:          strings.Split(addr, ","),
	}
	conf.TLS.EnableTLS = os.Getenv("OPENIM_TEST_KAFKA_TLS") == "1"
	conf.TLS.CACrt = os.Getenv("OPENIM_TEST_KAFKA_CA")
	return conf
}

func createTopicForTest(t *testing.T, conf *Config) string {
	kfk, err := BuildConsumerGroupConfig(conf, sarama.OffsetNewest, false)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sarama.NewClusterAdmin(conf.Addr, kfk)
	if err != nil {
		t.Fatal(err)
	}
	rf, _ := strconv.Atoi(os.Getenv("OPENIM_TEST_KAFKA_RF"))
	if rf == 0 {
		rf = 1
	}
	topic := fmt.Sprintf("openim-test-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 8, ReplicationFactor: int16(rf)}, false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(topic); _ = admin.Close() })
	return topic
}

// sendForTest sends n messages the way OpenIM's msg RPC does: proto value, conversation key,
// context headers. Message i is keyed by conversation i%10 and carries ClientMsgID "<prefix><i>".
func sendForTest(t *testing.T, conf *Config, topic, prefix string, n int) {
	kfk, err := BuildProducerConfig(*conf)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewKafkaProducer(kfk, conf.Addr, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer p.producer.Close()
	partitionOf := map[string]int32{}
	for i := 0; i < n; i++ {
		ctx := mcontext.SetOpUserID(mcontext.NewCtx(fmt.Sprintf("op-%s%d", prefix, i)), "imAdmin")
		key := fmt.Sprintf("si_%d", i%10)
		partition, _, err := p.SendMessage(ctx, key, &sdkws.MsgData{ClientMsgID: fmt.Sprintf("%s%d", prefix, i), SendID: "imAdmin"})
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		if old, ok := partitionOf[key]; ok && old != partition {
			t.Fatalf("key %s went to partition %d and %d", key, old, partition)
		}
		partitionOf[key] = partition
	}
}

// handlerForTest marks every message, like OpenIM's handlers; commitEach also calls Commit, like the
// toRedis handler (auto-commit off).
type handlerForTest struct {
	commitEach bool
	ready      chan struct{}
	once       sync.Once
	mu         sync.Mutex
	got        map[string]int
	claims     map[int32]bool
	reading    int
	badHeader  string
}

func newHandlerForTest(commitEach bool) *handlerForTest {
	return &handlerForTest{commitEach: commitEach, ready: make(chan struct{}), got: map[string]int{}, claims: map[int32]bool{}}
}

func (h *handlerForTest) Setup(sess sarama.ConsumerGroupSession) error {
	h.mu.Lock()
	h.claims = map[int32]bool{}
	h.reading = 0
	for _, ps := range sess.Claims() {
		for _, p := range ps {
			h.claims[p] = true
		}
	}
	h.mu.Unlock()
	return nil
}

func (h *handlerForTest) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim runs once sarama has fixed the claim's start offset (the newest one for a new group,
// as in OpenIM), so the member is ready when every claim has reached it.
func (h *handlerForTest) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.mu.Lock()
	h.reading++
	if h.reading == len(h.claims) {
		h.once.Do(func() { close(h.ready) })
	}
	h.mu.Unlock()
	for msg := range claim.Messages() {
		var data sdkws.MsgData
		if err := proto.Unmarshal(msg.Value, &data); err != nil {
			return err
		}
		ctx := GetContextWithMQHeader(msg.Headers)
		h.mu.Lock()
		h.got[data.ClientMsgID]++
		if mcontext.GetOperationID(ctx) != "op-"+data.ClientMsgID || mcontext.GetOpUserID(ctx) != "imAdmin" {
			h.badHeader = fmt.Sprintf("%s: operationID %q opUserID %q", data.ClientMsgID, mcontext.GetOperationID(ctx), mcontext.GetOpUserID(ctx))
		}
		h.mu.Unlock()
		sess.MarkMessage(msg, "")
		if h.commitEach {
			sess.Commit()
		}
	}
	return nil
}

func (h *handlerForTest) count(prefix string) (distinct, total int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, n := range h.got {
		if strings.HasPrefix(id, prefix) {
			distinct++
			total += n
		}
	}
	return
}

// startGroupForTest joins the group through NewMConsumerGroup and RegisterHandleAndConsumer, as
// OpenIM does, and waits until it reads all its partitions.
func startGroupForTest(t *testing.T, conf *Config, group, topic string, h *handlerForTest, autoCommit bool) (stop func()) {
	mc, err := NewMConsumerGroup(conf, group, []string{topic}, autoCommit)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { mc.RegisterHandleAndConsumer(ctx, h); close(done) }()
	select {
	case <-h.ready:
	case <-time.After(60 * time.Second):
		t.Fatal("consumer group got no partitions in 60 s")
	}
	return func() {
		cancel()
		if err := mc.Close(); err != nil {
			t.Errorf("close group: %v", err)
		}
		<-done
	}
}

func waitForTest(t *testing.T, what string, cond func() bool) {
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after 60 s waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestBrokerStartupChecks(t *testing.T) {
	conf := brokerConfigForTest(t)
	topic := createTopicForTest(t, conf)
	if err := CheckHealth(context.Background(), conf); err != nil {
		t.Fatalf("CheckHealth: %v", err)
	}
	if err := CheckTopics(context.Background(), conf, []string{topic}); err != nil {
		t.Fatalf("CheckTopics: %v", err)
	}
	if err := CheckTopics(context.Background(), conf, []string{topic + "-missing"}); err == nil {
		t.Fatal("CheckTopics passed for a topic that does not exist")
	}
}

// After a restart the group must go on from its committed offsets: nothing lost, nothing replayed.
func testResumeForTest(t *testing.T, autoCommit, commitEach bool) {
	conf := brokerConfigForTest(t)
	topic := createTopicForTest(t, conf)
	group := topic + "-group"

	h := newHandlerForTest(commitEach)
	stop := startGroupForTest(t, conf, group, topic, h, autoCommit)
	sendForTest(t, conf, topic, "a", 200)
	waitForTest(t, "200 messages", func() bool { d, _ := h.count("a"); return d == 200 })
	stop()

	sendForTest(t, conf, topic, "b", 50)
	h2 := newHandlerForTest(commitEach)
	stop2 := startGroupForTest(t, conf, group, topic, h2, autoCommit)
	defer stop2()
	waitForTest(t, "50 new messages", func() bool { d, _ := h2.count("b"); return d == 50 })
	time.Sleep(2 * time.Second) // a replay of the first batch would arrive by now
	if d, total := h.count("a"); total != d {
		t.Errorf("first run: %d messages delivered %d times", d, total)
	}
	if d, _ := h2.count("a"); d != 0 {
		t.Errorf("restart replayed %d of the first 200 messages", d)
	}
	if d, total := h2.count("b"); total != d {
		t.Errorf("after restart: %d messages delivered %d times", d, total)
	}
	for _, x := range []*handlerForTest{h, h2} {
		if x.badHeader != "" {
			t.Errorf("headers did not come back: %s", x.badHeader)
		}
	}
}

// toMongo, toPush and toOfflinePush: auto-commit on, MarkMessage only.
func TestBrokerResumeAutoCommit(t *testing.T) { testResumeForTest(t, true, false) }

// toRedis: auto-commit off, MarkMessage then Commit.
func TestBrokerResumeManualCommit(t *testing.T) { testResumeForTest(t, false, true) }

// Two pods in one group share the partitions; when one stops, the other takes them all over.
func TestBrokerRebalance(t *testing.T) {
	conf := brokerConfigForTest(t)
	topic := createTopicForTest(t, conf)
	group := topic + "-group"

	h1 := newHandlerForTest(false)
	stop1 := startGroupForTest(t, conf, group, topic, h1, true)
	h2 := newHandlerForTest(false)
	stop2 := startGroupForTest(t, conf, group, topic, h2, true)
	claimsOf := func(h *handlerForTest) int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.claims) }
	waitForTest(t, "the partitions shared by two members", func() bool {
		a, b := claimsOf(h1), claimsOf(h2)
		return a > 0 && b > 0 && a+b == 8
	})
	sendForTest(t, conf, topic, "a", 200)
	waitForTest(t, "200 messages over two members", func() bool {
		d1, _ := h1.count("a")
		d2, _ := h2.count("a")
		return d1+d2 == 200
	})
	if d1, _ := h1.count("a"); d1 == 0 {
		t.Error("member 1 got no messages")
	}
	if d2, _ := h2.count("a"); d2 == 0 {
		t.Error("member 2 got no messages")
	}

	stop2()
	waitForTest(t, "member 1 to hold all 8 partitions", func() bool { return claimsOf(h1) == 8 })
	sendForTest(t, conf, topic, "b", 100)
	waitForTest(t, "100 messages on the member left", func() bool { d, _ := h1.count("b"); return d == 100 })
	stop1()
}

// Sends for OPENIM_TEST_KAFKA_SEND_SECONDS while someone restarts brokers (as an MSK patch does, one
// at a time), with one consumer group member reading. Every message the producer called sent must
// arrive; failed sends are what a sender would see as errors, and are only counted.
func TestBrokerSendThroughRestarts(t *testing.T) {
	conf := brokerConfigForTest(t)
	seconds, _ := strconv.Atoi(os.Getenv("OPENIM_TEST_KAFKA_SEND_SECONDS"))
	if seconds == 0 {
		t.Skip("OPENIM_TEST_KAFKA_SEND_SECONDS not set")
	}
	topic := createTopicForTest(t, conf)
	h := newHandlerForTest(false)
	stop := startGroupForTest(t, conf, topic+"-group", topic, h, true)
	defer stop()

	kfk, err := BuildProducerConfig(*conf)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewKafkaProducer(kfk, conf.Addr, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer p.producer.Close()
	var sent []string
	failed := 0
	var longest time.Duration
	for end := time.Now().Add(time.Duration(seconds) * time.Second); time.Now().Before(end); {
		id := fmt.Sprintf("r%d", len(sent)+failed)
		start := time.Now()
		_, _, err := p.SendMessage(mcontext.SetOpUserID(mcontext.NewCtx("op-"+id), "imAdmin"), fmt.Sprintf("si_%d", (len(sent)+failed)%10), &sdkws.MsgData{ClientMsgID: id})
		if d := time.Since(start); d > longest {
			longest = d
		}
		if err != nil {
			failed++
			t.Logf("%s send failed after %s: %v", time.Now().Format("15:04:05.000"), time.Since(start).Round(time.Millisecond), err)
		} else {
			sent = append(sent, id)
		}
		time.Sleep(20 * time.Millisecond)
	}
	waitForTest(t, "every sent message", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, id := range sent {
			if h.got[id] == 0 {
				return false
			}
		}
		return true
	})
	d, total := h.count("r")
	t.Logf("sent %d, send failed %d, slowest send %s, received %d distinct, %d duplicate deliveries", len(sent), failed, longest.Round(time.Millisecond), d, total-d)
	if h.badHeader != "" {
		t.Errorf("headers did not come back: %s", h.badHeader)
	}
}
