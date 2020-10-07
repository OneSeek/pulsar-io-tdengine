package org.apache.pulsar.io.tdengine.sink;

import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.tdengine.sink.utils.HttpClientPoolUtil;


public class TSDBSession {
    TDengineSinkConfig config;
    String url;
    public void create(TDengineSinkConfig config){
        this.config = config;

        //http://<ip>:6041/rest/login/<username>/<password>
        url = config.getUrl()+"/rest/login/"+config.getUserName()
                +"/"+config.getPassword();
    }

    public void write(Record<GenericRecord> record) {
        String table = config.getTableName();
        String massage = record.getMessage().toString();
        String sql = "insert into "+table+" values(now,"+massage+")";

        HttpClientPoolUtil.execute(url,sql);
    }

    
}
