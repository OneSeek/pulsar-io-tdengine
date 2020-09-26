package org.apache.pulsar.io.tdengine.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TSDBSession {
    public void create(TDengineSinkConfig config) throws SQLException {
        String jdbcUrl = config.getJdbcUrl()+"?user="+config.getUserName()
                +"?password="+config.getPassword();

        Connection conn = DriverManager.getConnection(jdbcUrl);

        Statement stmt = conn.createStatement();

    }

    
}
