// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Regression for DORIS-25330: insert must fail promptly instead of hanging
// to execTimeout when VNodeChannel::open_wait reports error with done=false.
suite("test_insert_sub_be_open_fail", "nonConcurrent") {
    def beNums = sql("show backends").size()
    def replicationNum = Math.min(beNums, 3)
    def tbl = "test_insert_sub_be_open_fail_tbl"
    def injection = "VNodeChannel.open_wait.failed"

    try {
        // Force v1 path (VNodeChannel); v2 (memtable on sink node) bypasses the
        // debug point and also does not match the field-bug BE call stack.
        sql """ set enable_memtable_on_sink_node = false """

        sql """ DROP TABLE IF EXISTS ${tbl} """
        sql """
            CREATE TABLE ${tbl} (
                `k1` int,
                `v1` int
            ) ENGINE=olap
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            properties("replication_num" = "${replicationNum}")
        """

        // Bound the hang so a regression does not block the whole suite.
        sql """ set insert_timeout = 60 """

        GetDebugPoint().clearDebugPointsForAllBEs()
        // No target_be_id -> fail every channel; works for any BE count.
        GetDebugPoint().enableDebugPointForAllBEs(injection)

        long start = System.currentTimeMillis()
        boolean caught = false
        try {
            sql """ insert into ${tbl} values (1, 1), (2, 2), (3, 3) """
        } catch (Exception e) {
            caught = true
            logger.info("insert failed as expected: ${e.getMessage()}")
        }
        long elapsedMs = System.currentTimeMillis() - start
        logger.info("insert elapsed ${elapsedMs} ms, caught=${caught}, beNums=${beNums}")

        assertTrue(caught, "insert should fail when VNodeChannel::open_wait fails")
        // Hang would cap at insert_timeout (60s); 30s leaves slack for CI noise.
        assertTrue(elapsedMs < 30_000,
                "insert should fail within 30s, actual ${elapsedMs} ms (FE likely hanging on done=false report)")
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
        sql """ DROP TABLE IF EXISTS ${tbl} """
        sql """ set enable_memtable_on_sink_node = true """
    }
}
