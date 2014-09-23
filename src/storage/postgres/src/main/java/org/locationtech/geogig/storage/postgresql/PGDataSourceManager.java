package org.locationtech.geogig.storage.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.locationtech.geogig.storage.ConnectionManager;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import com.jolbox.bonecp.ConnectionHandle;
import com.jolbox.bonecp.hooks.AbstractConnectionHook;
import com.jolbox.bonecp.hooks.ConnectionHook;

class PGDataSourceManager extends ConnectionManager<Config, BoneCPDataSource> {

    private static ConnectionHook STATEMENTS_CLEANER = new AbstractConnectionHook() {

        @Override
        public void onDestroy(ConnectionHandle connection) {
            if (connection.isClosed()) {
                return;
            }
            if (!connection.isConnectionAlive()) {
                return;
            }
            Connection wrapped;
            try {
                wrapped = connection.unwrap(Connection.class);
            } catch (SQLException e1) {
                e1.printStackTrace();
                return;
            }
            Map<String, PreparedStatement> connStatements;
            Map<Connection, Map<String, PreparedStatement>> openStatements = PGStorage.OPEN_STATEMENTS;
            synchronized (openStatements) {
                connStatements = openStatements.remove(wrapped);
            }
            if (connStatements != null) {
                for (PreparedStatement st : connStatements.values()) {
                    try {
                        st.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    };

    @Override
    protected BoneCPDataSource connect(Config config) {
        ForwardingDataSource ds = new ForwardingDataSource(config);
        //System.err.println(ds.getUrl());
        BoneCPConfig bonceCPConfig = new BoneCPConfig();
        bonceCPConfig.setMaxConnectionsPerPartition(20);
        bonceCPConfig.setDatasourceBean(ds);

        BoneCPDataSource connPool = new BoneCPDataSource(bonceCPConfig);
        connPool.setConnectionHook(STATEMENTS_CLEANER);
        return connPool;
    }

    @Override
    protected void disconnect(BoneCPDataSource ds) {
        ds.close();
    }

}
