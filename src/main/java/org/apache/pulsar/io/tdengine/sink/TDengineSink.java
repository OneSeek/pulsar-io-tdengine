package org.apache.pulsar.io.tdengine.sink;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.util.Map;

@Connector(
        name = "tdengine",
        type = IOType.SINK,
        help = "The TDengineSink is used for moving messages from Pulsar to TDengineDB.",
        configClass = TDengineSink.class
)
@Slf4j
public class TDengineSink implements Sink<byte[]> {
    private final TSDBSession tsdbSession = new TSDBSession();
    @Override
    public void open(Map<String, Object> map, SinkContext sinkContext) throws Exception {

        val config = TDengineSinkConfig.load(map);
        config.validate();
        tsdbSession.create(config);

    }

    @Override
    public void write(Record<byte[]> record){
        tsdbSession.write(record);
    }


    @Override
    public void close() throws Exception {

    }
}