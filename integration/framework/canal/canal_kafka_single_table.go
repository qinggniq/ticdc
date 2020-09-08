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
	"database/sql"
	"github.com/pingcap/ticdc/integration/framework"
	"time"

	"github.com/pingcap/log"
)

const (
	testDbName = "testdb"
)

// CanalSingleTableTask provides a basic implementation for an Avro test case
type CanalSingleTableTask struct {
	TableName string
}

// Name implements Task
func (a *CanalSingleTableTask) Name() string {
	log.Warn("CanalSingleTableTask should be embedded in another Task")
	return "CanalSingleTableTask-" + a.TableName
}

// GetCDCProfile implements Task
func (a *CanalSingleTableTask) GetCDCProfile() *framework.CDCProfile {
	return &framework.CDCProfile{
		PDUri:   "http://upstream-pd:2379",
		SinkURI: "kafka://kafka:9092/testdb?protocol=canal",
		Opts:    map[string]string{"force-handle-key-pkey": "true"},
		ConfigFile: "/config/canal-test-config.toml",
	}
}

// Prepare implements Task
func (a *CanalSingleTableTask) Prepare(taskContext *framework.TaskContext) error {
	err := taskContext.CreateDB(testDbName)
	if err != nil {
		return err
	}

	_ = taskContext.Upstream.Close()
	taskContext.Upstream, err = sql.Open("mysql", upstreamDSN+testDbName)
	if err != nil {
		return err
	}

	_ = taskContext.Downstream.Close()
	taskContext.Downstream, err = sql.Open("mysql", downstreamDSN+testDbName)
	if err != nil {
		return err
	}
	taskContext.Downstream.SetConnMaxLifetime(5 * time.Second)

	// TODO canal should check adapter state in here

	if taskContext.WaitForReady != nil {
		log.Info("Waiting for env to be ready")
		return taskContext.WaitForReady()
	}

	return nil
}

// Run implements Task
func (a *CanalSingleTableTask) Run(taskContext *framework.TaskContext) error {
	log.Warn("CanalSingleTableTask has been run")
	return nil
}
