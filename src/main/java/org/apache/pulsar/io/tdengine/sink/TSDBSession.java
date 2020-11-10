package org.apache.pulsar.io.tdengine.sink;

import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TSDBSession {
    public static TDengineSinkConfig config;
    public static Connection conn;
    public static Statement stmt;
    public void create(TDengineSinkConfig config) throws SQLException {
        TSDBSession.config = config;
        String jdbcUrl = config.getJdbcUrl()+"?user="+config.getUserName()
                +"?password="+config.getPassword();

        conn = DriverManager.getConnection(jdbcUrl);

        stmt = conn.createStatement();

    }

    public void write(Record<GenericRecord> record) throws SQLException {
        String table = config.getTableName();
        String massage = record.getMessage().toString();
        int affectedRows = stmt.executeUpdate("insert into "+table+" values(now,"+massage+")");
    }

    
}
