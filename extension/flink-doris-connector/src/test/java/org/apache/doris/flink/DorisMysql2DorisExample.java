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

package org.apache.doris.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DorisMysql2DorisExample {

    public static void main(String[] args) throws Exception {
//        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        TableEnvironment tEnv = TableEnvironment.create(settings);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // source only supports parallelism of 1

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String sourceSql = "CREATE TABLE doris_test ( \n" +
                "id INT," +
                "name STRING," +
                "score DECIMAL(5,2)," +
                "yuwen float," +
                "shuxue double," +
                "date1 DATE," +
                "datetime2 timestamp," +
                "timestamp3 timestamp" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.url' = 'jdbc:mysql://172.22.193.65:3306/tmp',\n" +
                "    'connector.table' = 't_test_5',\n" +
                "    'connector.username' = 'root',\n" +
                "    'connector.password' = 'password'\n" +
                ")";

        String sinkSql = "CREATE TABLE doris_test_sink ( \n" +
                "id INT," +
                "name STRING," +
                "score DECIMAL(5,2)," +
                "yuwen float," +
                "shuxue double," +
                "date1 DATE," +
                "datetime2 timestamp," +
                "timestamp3 timestamp" +
                " )WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '10.220.146.10:8030',\n" +
                "  'table.identifier' = 'test_2.t_test_flink',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '',\n" +
                "  'sink.batch.size' = '1',\n" +
                "  'sink.batch.interval' = '100s',\n" +
                "  'sink.properties.line_delimiter' = 'aaaaab',\n" +
                "  'sink.properties.column_separator' = 'aaaaaa'\n" +
                ")";

        tEnv.executeSql(sourceSql);
        //tEnv.executeSql(sinkSql);
        // define a dynamic aggregating query
        final Table result = tEnv.sqlQuery("select id,name,score,yuwen,shuxue,date1,datetime2,timestamp3 from doris_test limit 10");

        // print the result to the console
        tEnv.toRetractStream(result, Row.class).print();
        env.execute();
//        tEnv.executeSql("INSERT INTO doris_test_sink select id,name,score,yuwen,shuxue,date1.toDate(),datetime2,timestamp3 from doris_test limit 10");

    }
}
