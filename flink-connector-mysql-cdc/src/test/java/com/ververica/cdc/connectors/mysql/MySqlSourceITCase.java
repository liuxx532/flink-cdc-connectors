/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.utils.UniqueDatabase;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertTrue;

/** Integration tests for {@link MySqlSource}. */
public class MySqlSourceITCase extends MySqlTestBase {

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    private final UniqueDatabase fullTypesDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "column_type_test", "mysqluser", "mysqlpw");

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingAllEvents() throws Exception {
        inventoryDatabase.createAndInitialize();
        SourceFunction<String> sourceFunction =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        // monitor all tables under inventory database
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .deserializer(new StringDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(sourceFunction).print().setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }

    @Test
    public void testConsumingAllEventsWithJsonFormatIncludeSchema() throws Exception {
        testConsumingAllEventsWithJsonFormat(true);
    }

    @Test
    public void testConsumingAllTypesWithJsonFormatExcludeSchema() throws Exception {
        testConsumingAllEventsWithJsonFormat(false);
    }

    private void testConsumingAllEventsWithJsonFormat(Boolean includeSchema) throws Exception {
        fullTypesDatabase.createAndInitialize();
        SourceFunction<String> sourceFunction =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        // monitor all tables under column_type_test database
                        .databaseList(fullTypesDatabase.getDatabaseName())
                        .username(fullTypesDatabase.getUsername())
                        .password(fullTypesDatabase.getPassword())
                        .deserializer(new JsonDebeziumDeserializationSchema(includeSchema))
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env,
                        EnvironmentSettings.newInstance()
                                .useBlinkPlanner()
                                .inStreamingMode()
                                .build());
        final String expectedFile =
                includeSchema
                        ? "file/debezium-data-schema-include.json"
                        : "file/debezium-data-schema-exclude.json";
        final JSONObject expected =
                JSONObject.parseObject(readLines(expectedFile), JSONObject.class);
        JSONObject expectSnapshot = expected.getJSONObject("expected_snapshot");

        DataStreamSource<String> source = env.addSource(sourceFunction);
        tEnv.createTemporaryView("full_types", source);
        TableResult result = tEnv.executeSql("SELECT * FROM full_types");

        // check the snapshot result
        CloseableIterator<Row> snapshot = result.collect();
        waitForSnapshotStarted(snapshot);
        assertTrue(
                dataInJsonIsEquals(
                        fetchRows(snapshot, 1).get(0).toString(), expectSnapshot.toString()));
        try (Connection connection = fullTypesDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
        }

        // check the binlog result
        CloseableIterator<Row> binlog = result.collect();
        JSONObject expectBinlog = expected.getJSONObject("expected_binlog");
        assertTrue(
                dataInJsonIsEquals(
                        fetchRows(binlog, 1).get(0).toString(), expectBinlog.toString()));
        result.getJobClient().get().cancel().get();
    }

    private static List<Object> fetchRows(Iterator<Row> iter, int size) {
        List<Object> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            // ignore rowKind marker
            rows.add(row.getField(0));
            size--;
        }
        return rows;
    }

    private static void waitForSnapshotStarted(CloseableIterator<Row> iterator) throws Exception {
        while (!iterator.hasNext()) {
            Thread.sleep(100);
        }
    }

    private static byte[] readLines(String resource) throws IOException, URISyntaxException {
        Path path =
                Paths.get(
                        Objects.requireNonNull(
                                        MySqlSourceITCase.class
                                                .getClassLoader()
                                                .getResource(resource))
                                .toURI());
        return Files.readAllBytes(path);
    }

    private static boolean dataInJsonIsEquals(String actual, String expect) {
        JSONObject actualJsonObject = JSONObject.parseObject(actual);
        JSONObject expectJsonObject = JSONObject.parseObject(expect);
        if (expectJsonObject.getJSONObject("payload") != null
                && actualJsonObject.getJSONObject("payload") != null) {
            expectJsonObject = expectJsonObject.getJSONObject("payload");
            actualJsonObject = actualJsonObject.getJSONObject("payload");
        }
        return Objects.equals(
                        expectJsonObject.getJSONObject("after"),
                        actualJsonObject.getJSONObject("after"))
                && Objects.equals(
                        expectJsonObject.getJSONObject("before"),
                        actualJsonObject.getJSONObject("before"))
                && Objects.equals(expectJsonObject.get("op"), actualJsonObject.get("op"));
    }
}
