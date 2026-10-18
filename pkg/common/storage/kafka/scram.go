package kafka

import (
	"strings"

	"github.com/IBM/sarama"
	"github.com/openimsdk/tools/errs"
	"github.com/xdg-go/scram"
)

// setSASL turns the SASL login on when a username or password is set. saslMechanism is PLAIN (the
// default, as before) or SCRAM-SHA-256 / SCRAM-SHA-512. Amazon MSK has no PLAIN: it takes
// SCRAM-SHA-512 over TLS.
func setSASL(kfk *sarama.Config, conf *Config) error {
	if conf.Username == "" && conf.Password == "" {
		return nil
	}
	kfk.Net.SASL.Enable = true
	kfk.Net.SASL.User = conf.Username
	kfk.Net.SASL.Password = conf.Password
	var hash scram.HashGeneratorFcn
	mechanism := sarama.SASLMechanism(strings.ToUpper(conf.SASLMechanism))
	switch mechanism {
	case "", sarama.SASLTypePlaintext:
		return nil
	case sarama.SASLTypeSCRAMSHA256:
		hash = scram.SHA256
	case sarama.SASLTypeSCRAMSHA512:
		hash = scram.SHA512
	default:
		return errs.New("unsupported kafka saslMechanism", "saslMechanism", conf.SASLMechanism).Wrap()
	}
	kfk.Net.SASL.Mechanism = mechanism
	kfk.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &scramClient{hash: hash} }
	return nil
}

// scramClient is sarama's SCRAMClient on top of xdg-go/scram.
type scramClient struct {
	*scram.ClientConversation
	hash scram.HashGeneratorFcn
}

func (c *scramClient) Begin(userName, password, authzID string) error {
	client, err := c.hash.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.ClientConversation = client.NewConversation()
	return nil
}
