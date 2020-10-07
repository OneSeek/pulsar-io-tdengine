package org.apache.pulsar.io.tdengine.sink;

import org.apache.commons.codec.binary.Base64;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.tdengine.sink.utils.HttpClientPoolUtil;

import java.io.UnsupportedEncodingException;

public class TSDBSession {
    TDengineSinkConfig config;
    String url_login;
    String url;
    String auth;

    public void create(TDengineSinkConfig config) throws UnsupportedEncodingException {
        this.config = config;

        //http://<ip>:6041/rest/login/<username>/<password>
        url_login = config.getUrl()+"/rest/login/"+config.getUserName()
                +"/"+config.getPassword();

        String name = config.getUserName();
        String password = config.getPassword();
        String authString = name + ":" + password;
        byte[] authEncBytes = Base64.encodeBase64(authString.getBytes("utf-8"));
        auth = new String(authEncBytes);

        url = config.getUrl()+"/rest/sql";
    }

    public void write(Record<byte[]> record) {

        String table = config.getTableName();
        String message = new String(record.getValue());

        String sql = "insert into "+table+" values(now,\""+message+"\")";

        String result = HttpClientPoolUtil.execute(url,sql,auth);

    }

    
}
