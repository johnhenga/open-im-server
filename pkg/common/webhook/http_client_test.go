// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openimsdk/open-im-server/v3/pkg/callbackstruct"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/tools/errs"
)

func syncPost(url string, failedContinue bool) error {
	before := &config.BeforeConfig{Enable: true, Timeout: 1, FailedContinue: failedContinue}
	req := &callbackstruct.CallbackBeforeSendGroupMsgReq{}
	resp := &callbackstruct.CallbackBeforeSendGroupMsgResp{}
	return NewWebhookClient(url).SyncPost(context.Background(), "callbackBeforeSendGroupMsgCommand", req, resp, before)
}

func serve(body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
}

// A webhook server that is down must not fail the operation when failedContinue is set.
func TestSyncPostUnreachable(t *testing.T) {
	srv := serve("")
	url := srv.URL
	srv.Close()
	if err := syncPost(url, true); err != nil {
		t.Fatalf("failedContinue=true: want nil, got %v", err)
	}
	if err := syncPost(url, false); err == nil {
		t.Fatal("failedContinue=false: want an error, got nil")
	}
}

func TestSyncPostNotJSON(t *testing.T) {
	srv := serve("<html>502 Bad Gateway</html>")
	defer srv.Close()
	if err := syncPost(srv.URL, true); err != nil {
		t.Fatalf("failedContinue=true: want nil, got %v", err)
	}
	if err := syncPost(srv.URL, false); err == nil {
		t.Fatal("failedContinue=false: want an error, got nil")
	}
}

// A rejection the webhook sends on purpose is returned even with failedContinue.
func TestSyncPostRejection(t *testing.T) {
	srv := serve(`{"actionCode":0,"errCode":60001,"errMsg":"blocked","nextCode":1}`)
	defer srv.Close()
	err := syncPost(srv.URL, true)
	codeErr, ok := errs.Unwrap(err).(errs.CodeError)
	if !ok || codeErr.Code() != 60001 {
		t.Fatalf("want code 60001, got %v", err)
	}
}

func TestSyncPostContinue(t *testing.T) {
	srv := serve(`{"actionCode":0,"errCode":0,"errMsg":"","nextCode":0}`)
	defer srv.Close()
	if err := syncPost(srv.URL, false); err != nil {
		t.Fatalf("want nil, got %v", err)
	}
}
