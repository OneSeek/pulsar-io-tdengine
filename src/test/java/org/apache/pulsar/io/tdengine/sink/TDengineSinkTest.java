
package org.apache.pulsar.io.tdengine.sink;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.commons.io.FileUtils.getFile;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

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
        configs.put("url", "http://localhost:6041");
        configs.put("userName", "root");
        configs.put("password", "taosdata");
        configs.put("tableName", "db.test_jdbc");

        TDengineSink sink = new TDengineSink();

        // prepare a foo Record
        Record<byte[]> record = build("taosTopic", "taosKey", "taosValue");

        // open should success
        sink.open(configs, null);

        // write should success.
        sink.write(record);
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
