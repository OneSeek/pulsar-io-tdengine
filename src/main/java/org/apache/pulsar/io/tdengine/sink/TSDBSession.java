package org.apache.pulsar.io.tdengine.sink;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.tdengine.sink.utils.HttpClientPoolUtil;

public class TSDBSession {
    TDengineSinkConfig config;
    String url_login;
    String url;
    public void create(TDengineSinkConfig config){
        this.config = config;

        //http://<ip>:6041/rest/login/<username>/<password>
        url_login = config.getUrl()+"/rest/login/"+config.getUserName()
                +"/"+config.getPassword();

        url = config.getUrl()+"/rest/sql";
    }

    public void write(Record<byte[]> record) {

        String result = HttpClientPoolUtil.execute(url_login);
        System.out.println("创建TDengine连接成功："+result);

        String table = config.getTableName();
        String message = new String(record.getValue());
        System.out.println("massage: " + message);
        String sql = "insert into "+table+" values(now,\""+message+"\")";
        System.out.println(sql);
        String result1 = HttpClientPoolUtil.execute(url,sql);
        System.out.println(result1);
    }

    
}
