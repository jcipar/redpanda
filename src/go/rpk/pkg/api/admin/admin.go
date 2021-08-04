// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	log "github.com/sirupsen/logrus"
)

const (
	httpPrefix  = "http://"
	httpsPrefix = "https://"
)

type AdminAPI struct {
	urls   []string
	client *http.Client
}

func NewAdminAPI(urls []string, tlsConfig *tls.Config) (*AdminAPI, error) {
	adminUrls := make([]string, len(urls))
	for i := 0; i < len(urls); i++ {
		prefix := ""
		url := urls[i]
		// Go's http library requires that the URL have a protocol.
		if !(strings.HasPrefix(url, httpPrefix) ||
			strings.HasPrefix(url, httpsPrefix)) {

			prefix = httpPrefix

			if tlsConfig != nil {
				// If TLS will be enabled, use HTTPS as the protocol
				prefix = httpsPrefix
			}

			url = strings.TrimRight(url, "/")
		}
		adminUrls[i] = fmt.Sprintf("%s%s", prefix, url)
	}

	tr := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	client := &http.Client{Transport: tr}
	return &AdminAPI{urls: adminUrls, client: client}, nil
}

// As of v21.4.15, the Redpanda admin API doesn't do request forwarding, which
// means that some requests (such as the ones made to /users) will fail unless
// the reached node is the leader. Therefore, a request needs to be made to
// each node, and of those requests at least one should succeed.
// FIXME (@david): when https://github.com/vectorizedio/redpanda/issues/1265
// is fixed.
func sendToMultiple(
	urls []string, method string, body interface{}, client *http.Client,
) (*http.Response, error) {
	sendClosure := func(url string, res chan<- *http.Response) func() error {
		return func() error {
			r, err := send(url, method, body, client)
			res <- r
			return err
		}
	}

	res := make(chan *http.Response, len(urls))
	grp := multierror.Group{}
	for _, url := range urls {
		grp.Go(sendClosure(url, res))
	}

	err := grp.Wait()
	close(res)

	if err == nil || len(err.Errors) < len(urls) {
		for r := range res {
			if r != nil && r.StatusCode < 400 {
				return r, nil
			}
		}
	}
	return nil, err.ErrorOrNil()
}

func send(
	url, method string, body interface{}, client *http.Client,
) (*http.Response, error) {
	var bodyBuffer io.Reader
	if body != nil {
		bs, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("couldn't encode request: %v", err)
		}
		bodyBuffer = bytes.NewBuffer(bs)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		method,
		url,
		bodyBuffer,
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		// When the server expects a TLS connection, but the TLS config isn't
		// set/ passed, The client returns an error like
		// Get "http://localhost:9644/v1/security/users": EOF
		// which doesn't make it obvious to the user what's going on.
		if strings.Contains(err.Error(), "EOF") {
			log.Debug(err)
			return nil, errors.New("the server expected a TLS connection")
		}
	}

	if res != nil && res.StatusCode >= 400 {
		resBody, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Error(err)
		}
		return res, fmt.Errorf(
			"Request failed with status %d: %s",
			res.StatusCode,
			string(resBody),
		)
	}

	return res, err
}
