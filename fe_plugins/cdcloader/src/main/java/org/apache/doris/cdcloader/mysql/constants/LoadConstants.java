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

package org.apache.doris.cdcloader.mysql.constants;

public class LoadConstants {
    public static final String DB_SOURCE_TYPE = "db_source_type";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String DATABASE_NAME = "database_name";
    public static final String INCLUDE_TABLES_LIST = "include_tables_list";
    public static final String EXCLUDE_TABLES_LIST = "exclude_tables_list";
    public static final String INCLUDE_SCHEMA_CHANGES = "include_schema_changes";
    public static final String TABLE_PROPS_PREFIX = "table.create.properties.";
    public static final String SCAN_STARTUP_MODE = "scan.startup.mode";
    public static final String SCAN_STARTUP_SPECIFIC_OFFSET = "scan.startup.specific-offset";
    public static final String SCAN_STARTIP_TIMESTAMP = "scan.startup.timestamp-millis";
    public static final String MAX_BATCH_ROWS = "max_batch_rows";
    public static final String MAX_BATCH_SIZE = "max_batch_size";
    public static final String EOF_MESSAGE = "EOF";
}
