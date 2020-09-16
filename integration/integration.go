// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/integration/framework"
	"github.com/pingcap/ticdc/integration/framework/canal"
	canal2 "github.com/pingcap/ticdc/integration/tests/canal"
	"go.uber.org/zap/zapcore"
)

func main() {
	dockerComposeFile := flag.String("docker-compose-file", "", "the path of the Docker-compose yml file")

	testCases := []framework.Task{
		canal2.NewSimpleCase(),
		canal2.NewDeleteCase(),
		canal2.NewManyTypesCase(),
		canal2.NewUnsignedCase(),
		canal2.NewCompositePKeyCase(),
		canal2.NewAlterCase(), // this case is slow, so put it last
	}

	log.SetLevel(zapcore.DebugLevel)
	env := canal.NewCanalKafkaDockerEnv(*dockerComposeFile)
	env.Setup()

	for i := range testCases {
		env.RunTest(testCases[i])
		if i < len(testCases)-1 {
			env.Reset()
		}
	}

	env.TearDown()
}
