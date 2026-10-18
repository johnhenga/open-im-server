package kafka

import (
	"testing"

	"github.com/IBM/sarama"
)

func TestSetSASLMechanism(t *testing.T) {
	for _, tc := range []struct {
		mechanism string
		want      sarama.SASLMechanism
		scram     bool
	}{
		{"", sarama.SASLTypePlaintext, false},
		{"PLAIN", sarama.SASLTypePlaintext, false},
		{"scram-sha-256", sarama.SASLTypeSCRAMSHA256, true},
		{"SCRAM-SHA-512", sarama.SASLTypeSCRAMSHA512, true},
	} {
		conf := &Config{Username: "openim", Password: "pw", SASLMechanism: tc.mechanism}
		prod, err := BuildProducerConfig(*conf)
		if err != nil {
			t.Fatalf("%q producer: %v", tc.mechanism, err)
		}
		cons, err := BuildConsumerGroupConfig(conf, sarama.OffsetNewest, true)
		if err != nil {
			t.Fatalf("%q consumer: %v", tc.mechanism, err)
		}
		for _, kfk := range []*sarama.Config{prod, cons} {
			// Validate also fills an empty mechanism with PLAIN, as sarama does before it connects.
			if err := kfk.Validate(); err != nil {
				t.Fatalf("%q: sarama refuses the config: %v", tc.mechanism, err)
			}
			if !kfk.Net.SASL.Enable || kfk.Net.SASL.Mechanism != tc.want || (kfk.Net.SASL.SCRAMClientGeneratorFunc != nil) != tc.scram {
				t.Fatalf("%q: enable %v mechanism %q scram generator %v", tc.mechanism, kfk.Net.SASL.Enable, kfk.Net.SASL.Mechanism, kfk.Net.SASL.SCRAMClientGeneratorFunc != nil)
			}
		}
		if tc.scram {
			c := prod.Net.SASL.SCRAMClientGeneratorFunc()
			if err := c.Begin("openim", "pw", ""); err != nil {
				t.Fatal(err)
			}
			if first, err := c.Step(""); err != nil || first == "" || c.Done() {
				t.Fatalf("%q: first SCRAM message %q, err %v, done %v", tc.mechanism, first, err, c.Done())
			}
		}
	}
}

func TestSetSASLNoLoginAndUnknownMechanism(t *testing.T) {
	kfk, err := BuildProducerConfig(Config{SASLMechanism: "SCRAM-SHA-512"})
	if err != nil {
		t.Fatal(err)
	}
	if kfk.Net.SASL.Enable {
		t.Fatal("no username or password must leave SASL off")
	}
	if _, err := BuildProducerConfig(Config{Username: "u", Password: "p", SASLMechanism: "GSSAPI"}); err == nil {
		t.Fatal("want an error for a mechanism this build does not speak")
	}
	if _, err := BuildConsumerGroupConfig(&Config{Username: "u", Password: "p", SASLMechanism: "OAUTHBEARER"}, sarama.OffsetNewest, false); err == nil {
		t.Fatal("want an error for a mechanism this build does not speak")
	}
}
