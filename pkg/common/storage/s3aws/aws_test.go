package s3aws

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// signedLink returns a presigned GET for conf, with the default chain fed from env only.
func signedLink(t *testing.T, conf Config) string {
	t.Setenv("AWS_CONFIG_FILE", os.DevNull)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", os.DevNull)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDFROMCHAIN")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "chain-secret")
	ctx := context.Background()
	a, err := NewAws(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	u, err := a.AccessURL(ctx, "u1/photo.jpg", time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestNoKeyUsesDefaultChain(t *testing.T) {
	u := signedLink(t, Config{Region: "ap-southeast-1", Bucket: "media"})
	for _, want := range []string{"X-Amz-Credential=AKIDFROMCHAIN", "X-Amz-Expires=3600", "media"} {
		if !strings.Contains(u, want) {
			t.Fatalf("signed link %q lacks %q", u, want)
		}
	}
}

func TestFixedKeyStillWins(t *testing.T) {
	u := signedLink(t, Config{Region: "ap-southeast-1", Bucket: "media", AccessKeyID: "AKIDFIXED", SecretAccessKey: "s"})
	if !strings.Contains(u, "X-Amz-Credential=AKIDFIXED") {
		t.Fatalf("signed link %q is not signed with the fixed key", u)
	}
}
