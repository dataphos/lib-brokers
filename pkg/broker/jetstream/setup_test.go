// Copyright 2024 Syntio Ltd.
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

package jetstream_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

const TestPort = 4222

var (
	testServer *server.Server
	jsCtx      nats.JetStreamContext
)

func RunServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port

	return RunServerWithOptions(&opts)
}

func RunServerWithOptions(opts *server.Options) *server.Server {
	return natsserver.RunServer(opts)
}

func initializeTestEnv() (*server.Server, nats.JetStreamContext, []string) { //nolint:ireturn //have to return interface
	serverOnPort := RunServerOnPort(TestPort)
	err := serverOnPort.EnableJetStream(&server.JetStreamConfig{})
	if err != nil { //nolint:wsl //file gofumpt-ed
		log.Fatal("Error while enabling JetStream: ", err)
	}

	// Use nats as normal.
	sURL := fmt.Sprintf("nats://127.0.0.1:%d", TestPort)
	nc, _ := nats.Connect(sURL)
	jetStream, err := nc.JetStream()
	if err != nil { //nolint:wsl //file gofumpt-ed
		log.Fatal("error creating jetstream context:", err)
	}

	// the second and third stream are used because consuming from multiple subjects on a stream with a single
	// consumer causes problems.
	streamNames := []string{"Testing", "TestingSubjects", "TestingSubjectsBatch"}
	streamSubjects := []string{"Test.*", "Test.DifferentSubjects.*", "Test.DifferentSubjectsBatch.*"}

	for streamNum, streamName := range streamNames {
		if _, err = jetStream.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubjects[streamNum]},
		}); err != nil {
			log.Fatal("error adding stream:", err)
		}
	}

	return serverOnPort, jetStream, streamNames
}

func closeTestEnv(testServer *server.Server, js nats.JetStreamContext, streamNames []string) {
	cleanClose := true

	for _, name := range streamNames {
		err := js.DeleteStream(name)
		if err != nil {
			log.Println(err)

			cleanClose = false
		}
	}

	// shut down the server before failing. When testing locally, this might delete the data so that it doesn't
	// interfere with subsequent runs.
	testServer.Shutdown()

	if !cleanClose {
		log.Fatal("Errors closing test environment")
	}
}

func TestMain(m *testing.M) {
	var streamNames []string
	testServer, jsCtx, streamNames = initializeTestEnv()

	code := m.Run()

	closeTestEnv(testServer, jsCtx, streamNames)
	os.Exit(code)
}
