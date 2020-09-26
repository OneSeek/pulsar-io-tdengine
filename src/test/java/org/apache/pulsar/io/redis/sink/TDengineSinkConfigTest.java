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


import org.apache.pulsar.io.tdengine.sink.TDengineSinkConfig;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * TDengineSinkConfig test
 */
public class TDengineSinkConfigTest {

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sinkConfig.yaml");
        String path = yamlFile.getAbsolutePath();
        TDengineSinkConfig config = TDengineSinkConfig.load(path);
        assertNotNull(config);
        assertEquals("jdbc:TAOS://localhost:6041/db", config.getJdbcUrl());
        assertEquals("root", config.getUserName());
        assertEquals("taosdata", config.getPassword());
        assertEquals("test_jdbc", config.getTableName());
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("jdbcUrl", "jdbc:TAOS://localhost:6041/db");
        map.put("userName", "root");
        map.put("password", "taosdata");
        map.put("tableName", "test_jdbc");


        TDengineSinkConfig config = TDengineSinkConfig.load(map);
        assertNotNull(config);
        assertEquals("jdbc:TAOS://localhost:6041/db", config.getJdbcUrl());
        assertEquals("root", config.getUserName());
        assertEquals("taosdata", config.getPassword());
        assertEquals("test_jdbc", config.getTableName());
    }

    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("jdbcUrl", "jdbc:TAOS://localhost:6041/db");
        map.put("userName", "root");
        map.put("password", "taosdata");
        map.put("tableName", "test_jdbc");

        TDengineSinkConfig config = TDengineSinkConfig.load(map);
        config.validate();
    }

    @Test
    public final void missingValidValidateTableNameTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("jdbcUrl", "jdbc:TAOS://localhost:6041/db");
        map.put("userName", "root");
        map.put("password", "taosdata");
        map.put("tableName", "test_jdbc");

        TDengineSinkConfig config = TDengineSinkConfig.load(map);
        config.validate();
    }


    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
