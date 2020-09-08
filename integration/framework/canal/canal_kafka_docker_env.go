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

package canal

import (
	"context"
	"database/sql"
	"github.com/pingcap/ticdc/integration/framework"
	"os/exec"
	"time"

	"github.com/integralist/go-findroot/find"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	dockerComposeFilePath   = "/docker-compose-canal.yml"
	controllerContainerName = "ticdc_controller_1"
	upstreamDSN             = "root@tcp(127.0.0.1:4000)/"
	downstreamDSN           = "root@tcp(127.0.0.1:5000)/"
)

// CanalKafkaDockerEnv represents the docker-compose service defined in docker-compose-canal.yml
type CanalKafkaDockerEnv struct {
	framework.DockerComposeOperator
}

// NewCanalKafkaDockerEnv creates a new CanalKafkaDockerEnv
func NewCanalKafkaDockerEnv(dockerComposeFile string) *CanalKafkaDockerEnv {
	var file string
	if dockerComposeFile == "" {
		st, err := find.Repo()
		if err != nil {
			log.Fatal("Could not find git repo root", zap.Error(err))
		}
		file = st.Path + dockerComposeFilePath
	} else {
		file = dockerComposeFile
	}

	return &CanalKafkaDockerEnv{framework.DockerComposeOperator{
		FileName:      file,
		Controller:    controllerContainerName,
		// canal's health checker should be navigated
		HealthChecker: nil,
	}}
}

// Reset implements Environment
func (e *CanalKafkaDockerEnv) Reset() {
	e.TearDown()
	e.Setup()
}

// RunTest implements Environment
func (e *CanalKafkaDockerEnv) RunTest(task framework.Task) {
	cmdLine := "/cdc " + task.GetCDCProfile().String()
	bytes, err := e.ExecInController(cmdLine)
	if err != nil {
		log.Fatal("RunTest failed: cannot setup changefeed",
			zap.Error(err),
			zap.ByteString("stdout", bytes),
			zap.ByteString("stderr", err.(*exec.ExitError).Stderr))
	}

	upstream, err := sql.Open("mysql", upstreamDSN)
	if err != nil {
		log.Fatal("RunTest: cannot connect to upstream database", zap.Error(err))
	}

	_, err = upstream.Exec("set @@global.tidb_enable_clustered_index=0")
	if err != nil {
		log.Info("tidb_enable_clustered_index not supported.")
	} else {
		time.Sleep(2 * time.Second)
	}

	downstream, err := sql.Open("mysql", downstreamDSN)
	if err != nil {
		log.Fatal("RunTest: cannot connect to downstream database", zap.Error(err))
	}

	taskCtx := &framework.TaskContext{
		Upstream:   upstream,
		Downstream: downstream,
		Env:        e,
		WaitForReady: func() error {
			return nil
		},
		Ctx: context.Background(),
	}

	err = task.Prepare(taskCtx)
	if err != nil {
		e.TearDown()
		log.Fatal("RunTest: task preparation failed", zap.String("name", task.Name()), zap.Error(err))
	}

	log.Info("Start running task", zap.String("name", task.Name()))
	err = task.Run(taskCtx)
	if err != nil {
		e.TearDown()
		log.Fatal("RunTest: task failed", zap.String("name", task.Name()), zap.Error(err))
	}
	log.Info("Finished running task", zap.String("name", task.Name()))
}

// SetListener implements Environment. Currently unfinished, will be used to monitor Kafka output
func (e *CanalKafkaDockerEnv) SetListener(states interface{}, listener framework.MqListener) {
	// TODO
}
