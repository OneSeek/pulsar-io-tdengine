package org.apache.pulsar.io.tdengine;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.util.Map;

@Connector(
        name = "tdenginedb",
        type = IOType.SINK,
        help = "The TDengineDBGenericRecordSink is used for moving messages from Pulsar to InfluxDB.",
        configClass = TDengineDBSinkConfig.class
)
@Slf4j
public class TDengineDBSink implements Sink<GenericRecord> {
    protected Sink<GenericRecord> sink;
    @Override
    public void open(Map<String, Object> map, SinkContext sinkContext) throws Exception {

        val config = TDengineDBSinkConfig.load(map);
        config.validate();
        sink = new TDengineDBSink();

        sink.open(map, sinkContext);
    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {
        sink.write(record);
    }


    @Override
    public void close() throws Exception {
        sink.close();
    }

}