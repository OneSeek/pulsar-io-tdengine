/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.redis.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.io.tdengine.sink.TDengineSink;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * TDengine Sink test
 */
//@Slf4j
public class TDengineSinkTest {


    @BeforeMethod
    public void setUp() throws Exception {

    }

    @AfterMethod
    public void tearDown() throws Exception {

    }

    @Test
    public void TestOpenAndWriteSink() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put("jdbcUrl", "jdbc:TAOS://localhost:6041/db");
        configs.put("userName", "root");
        configs.put("password", "taosdata");
        configs.put("tableName", "test_jdbc");

        TDengineSink sink = new TDengineSink();

        // prepare a foo Record
        Record<byte[]> record = build("taosTopic", "taosKey", "taosValue");

        // open should success
        sink.open(configs, null);

        // write should success.
//        sink.write(record);
//        log.info("executed write");

        // sleep to wait backend flush complete
        Thread.sleep(1000);

    }

    private Record<byte[]> build(String topic, String key, String value) {
        // prepare a SinkRecord
        SinkRecord<byte[]> record = new SinkRecord<>(new Record<byte[]>() {
            @Override
            public Optional<String> getKey() {
                return Optional.empty();
            }

            @Override
            public byte[] getValue() {
                return value.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public Optional<String> getDestinationTopic() {
                if (topic != null) {
                    return Optional.of(topic);
                } else {
                    return Optional.empty();
                }
            }
        }, value.getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
