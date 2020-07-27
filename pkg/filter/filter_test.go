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

package filter

import (
	"testing"

	"github.com/pingcap/parser/mysql"

	"github.com/pingcap/ticdc/pkg/config"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
)

type filterSuite struct{}

var _ = check.Suite(&filterSuite{})

func Test(t *testing.T) { check.TestingT(t) }

func (s *filterSuite) TestShouldUseDefaultRules(c *check.C) {
	filter, err := NewFilter(config.GetDefaultReplicaConfig())
	c.Assert(err, check.IsNil)
	c.Assert(filter.ShouldIgnoreTable("information_schema", ""), check.IsTrue)
	c.Assert(filter.ShouldIgnoreTable("information_schema", "statistics"), check.IsTrue)
	c.Assert(filter.ShouldIgnoreTable("performance_schema", ""), check.IsTrue)
	c.Assert(filter.ShouldIgnoreTable("metric_schema", "query_duration"), check.IsTrue)
	c.Assert(filter.ShouldIgnoreTable("sns", "user"), check.IsFalse)
	c.Assert(filter.ShouldIgnoreTable("tidb_cdc", "repl_mark_a_a"), check.IsFalse)
}

func (s *filterSuite) TestShouldUseCustomRules(c *check.C) {
	filter, err := NewFilter(&config.ReplicaConfig{
		Filter: &config.FilterConfig{
			Rules: []string{"sns.*", "ecom.*", "!sns.log", "!ecom.test"},
		},
		Cyclic: &config.CyclicConfig{Enable: true},
	})
	c.Assert(err, check.IsNil)
	assertIgnore := func(db, tbl string, boolCheck check.Checker) {
		c.Assert(filter.ShouldIgnoreTable(db, tbl), boolCheck)
	}
	assertIgnore("other", "", check.IsTrue)
	assertIgnore("other", "what", check.IsTrue)
	assertIgnore("sns", "", check.IsFalse)
	assertIgnore("ecom", "order", check.IsFalse)
	assertIgnore("ecom", "order", check.IsFalse)
	assertIgnore("ecom", "test", check.IsTrue)
	assertIgnore("sns", "log", check.IsTrue)
	assertIgnore("information_schema", "", check.IsTrue)
	assertIgnore("tidb_cdc", "repl_mark_a_a", check.IsFalse)
}

func (s *filterSuite) TestShouldIgnoreTxn(c *check.C) {
	filter, err := NewFilter(&config.ReplicaConfig{
		Filter: &config.FilterConfig{
			IgnoreTxnStartTs: []uint64{1, 3},
			Rules:            []string{"sns.*", "ecom.*", "!sns.log", "!ecom.test"},
			IgnoreColumnType: []string{"blob", "bit", "int", "binary"},
		},
	})
	c.Assert(err, check.IsNil)
	testCases := []struct {
		schema      string
		table       string
		ts          uint64
		columnTypes []byte
		ignore      bool
	}{
		{"sns", "ttta", 1, nil, true},
		{"ecom", "aabb", 2, nil, false},
		{"sns", "log", 3, nil, true},
		{"sns", "log", 4, nil, true},
		{"ecom", "test", 5, nil, true},
		{"test", "test", 6, nil, true},
		{"ecom", "log", 6, nil, false},
		{"ecom", "log", 6, []byte{mysql.TypeBit, mysql.TypeTinyBlob}, true},
		{"ecom", "log", 6, []byte{mysql.TypeJSON, mysql.TypeTinyBlob}, false},
		{"ecom", "log", 6, []byte{mysql.TypeString}, true},
	}

	for _, tc := range testCases {
		c.Assert(filter.ShouldIgnoreDMLEvent(tc.ts, tc.schema, tc.table, tc.columnTypes), check.Equals, tc.ignore)
		c.Assert(filter.ShouldIgnoreDDLEvent(tc.ts, tc.schema, tc.table, tc.columnTypes), check.Equals, tc.ignore)
	}
}

func (s *filterSuite) TestShouldDiscardDDL(c *check.C) {
	config := &config.ReplicaConfig{
		Filter: &config.FilterConfig{
			DDLAllowlist: []model.ActionType{model.ActionAddForeignKey},
		},
	}
	filter, err := NewFilter(config)
	c.Assert(err, check.IsNil)
	c.Assert(filter.ShouldDiscardDDL(model.ActionDropSchema), check.IsFalse)
	c.Assert(filter.ShouldDiscardDDL(model.ActionAddForeignKey), check.IsFalse)
	c.Assert(filter.ShouldDiscardDDL(model.ActionCreateSequence), check.IsTrue)
}
