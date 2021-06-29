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

package com.alibaba.ververica.cdc.connectors.mongodb.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.ververica.cdc.connectors.mongodb.MongoDBSource.ERROR_TOLERANCE_ALL;
import static com.alibaba.ververica.cdc.connectors.mongodb.MongoDBSource.POLL_AWAIT_TIME_MILLIS_DEFAULT;
import static com.alibaba.ververica.cdc.connectors.mongodb.MongoDBSource.POLL_MAX_BATCH_SIZE_DEFAULT;
import static org.apache.flink.table.api.TableSchema.fromResolvedSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test for {@link MongoDBTableSource} created by {@link MongoDBTableSourceFactory}. */
public class MongoDBTableFactoryTest {
    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("_id", DataTypes.STRING().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    new ArrayList<>(),
                    UniqueConstraint.primaryKey("pk", Arrays.asList("_id")));

    private static final String MY_URI =
            "mongodb://flinkuser:flinkpw@localhost:27017,localhost:27018";
    private static final String MY_DATABASE = "myDB";
    private static final String MY_TABLE = "myTable";
    private static final String ERROR_TOLERANCE = "none";
    private static final Boolean ERROR_LOGS_ENABLE = true;
    private static final Boolean COPY_EXISTING = true;

    @Test
    public void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MongoDBTableSource expectedSource =
                new MongoDBTableSource(
                        TableSchemaUtils.getPhysicalSchema(fromResolvedSchema(SCHEMA)),
                        MY_URI,
                        MY_DATABASE,
                        MY_TABLE,
                        ERROR_TOLERANCE,
                        ERROR_LOGS_ENABLE,
                        COPY_EXISTING,
                        null,
                        null,
                        null,
                        POLL_MAX_BATCH_SIZE_DEFAULT,
                        POLL_AWAIT_TIME_MILLIS_DEFAULT,
                        null);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("errors.tolerance", "all");
        options.put("errors.log.enable", "false");
        options.put("copy.existing", "false");
        options.put("copy.existing.pipeline", "[ { \"$match\": { \"closed\": \"false\" } } ]");
        options.put("copy.existing.max.threads", "1");
        options.put("copy.existing.queue.size", "101");
        options.put("poll.max.batch.size", "102");
        options.put("poll.await.time.ms", "103");
        options.put("heartbeat.interval.ms", "104");
        DynamicTableSource actualSource = createTableSource(options);

        MongoDBTableSource expectedSource =
                new MongoDBTableSource(
                        TableSchemaUtils.getPhysicalSchema(fromResolvedSchema(SCHEMA)),
                        MY_URI,
                        MY_DATABASE,
                        MY_TABLE,
                        ERROR_TOLERANCE_ALL,
                        false,
                        false,
                        "[ { \"$match\": { \"closed\": \"false\" } } ]",
                        1,
                        101,
                        102,
                        103,
                        104);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testValidation() {
        // validate unsupported option
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("unknown", "abc");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(t, "Unsupported options:\n\nunknown")
                            .isPresent());
        }
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "mongodb-cdc");
        options.put("uri", MY_URI);
        options.put("database", MY_DATABASE);
        options.put("collection", MY_TABLE);
        return options;
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                fromResolvedSchema(SCHEMA).toSchema(),
                                "mock source",
                                new ArrayList<>(),
                                options),
                        SCHEMA),
                new Configuration(),
                MongoDBTableFactoryTest.class.getClassLoader(),
                false);
    }
}
