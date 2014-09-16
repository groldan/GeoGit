/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.postgresql;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.plumbing.ResolveGeogigDir;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import com.jolbox.bonecp.ConnectionHandle;
import com.jolbox.bonecp.hooks.AbstractConnectionHook;
import com.jolbox.bonecp.hooks.ConnectionHook;

/**
 * Utility class for SQLite storage.
 * 
 * @author Justin Deoliveira, Boundless
 */
public class PGStorage {
    /**
     * Format name used for configuration.
     */
    public static final String FORMAT_NAME = "postgres";

    /**
     * Implementation version.
     */
    public static final String VERSION = "0.1";

    private static Map<Connection, Map<String, PreparedStatement>> OPEN_STATEMENTS = new IdentityHashMap<>();

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
            Map<Connection, Map<String, PreparedStatement>> openStatements = OPEN_STATEMENTS;
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

    static PreparedStatement prepareStatement(final Connection connection, final String sql)
            throws SQLException {

        final Connection wrapped = connection.unwrap(Connection.class);

        PreparedStatement statement;
        Map<Connection, Map<String, PreparedStatement>> openStatements = OPEN_STATEMENTS;
        Map<String, PreparedStatement> connStatements = openStatements.get(wrapped);
        if (connStatements == null) {
            connStatements = Maps.newConcurrentMap();
            synchronized (openStatements) {
                openStatements.put(wrapped, connStatements);
            }
        }
        statement = connStatements.get(sql);
        if (statement == null) {
            statement = connection.prepareStatement(sql);
            connStatements.put(sql, statement);
        }
        return statement;
    }

    /**
     * Returns the .geogig directory for the platform object.
     */
    static File geogigDir(Platform platform) {
        Optional<URL> url = new ResolveGeogigDir(platform).call();
        if (!url.isPresent()) {
            throw new RuntimeException("Unable to resolve .geogig directory");
        }
        try {
            return new File(url.get().toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException("Error resolving .geogig directory", e);
        }
    }

    /**
     * Logs a (prepared) sql statement.
     * 
     * @param sql Base sql to log.
     * @param log The logger object.
     * @param args Optional arguments to the statement.
     * 
     * @return The original statement.
     */
    static String log(String sql, Logger log, Object... args) {
        if (log.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder(sql);
            if (args.length > 0) {
                sb.append(";");
                for (int i = 0; i < args.length; i++) {
                    sb.append(i).append("=").append(args[i]).append(", ");
                }
                sb.setLength(sb.length() - 2);
            }
            log.debug(sb.toString());
        }
        return sql;
    }

    synchronized static DataSource newDataSource(File db) {
        String driverName;
        try {
            Class.forName("shaded.org.postgresql.Driver");
            driverName = "shaded.org.postgresql.Driver";
        } catch (ClassNotFoundException e) {
            try {
                Class.forName("org.postgresql.Driver");
                driverName = "org.postgresql.Driver";
            } catch (ClassNotFoundException e2) {
                throw Throwables.propagate(e);
            }
        }

        ForwardingDataSource ds = new ForwardingDataSource(driverName);
        ds.setServerName("localhost");
        ds.setDatabase("osm_shape");
        ds.setPortNumber(5432);
        ds.setUser("postgres");
        ds.setPassword("geo123");

        // call("setServerName", "localhost", dataSource);
        // call("setDatabaseName", "geogigconfig", dataSource);
        // call("setPortNumber", Integer.valueOf(5432), dataSource);
        // call("setUser", "postgres", dataSource);
        // call("setPassword", "geo123", dataSource);
        BoneCPConfig config = new BoneCPConfig();
        // config.setJdbcUrl("jdbc:postgresql://localhost:5432/geogigconfig");
        // config.setUsername("postgres");
        // config.setPassword("geo123");
        config.setMaxConnectionsPerPartition(20);
        config.setDatasourceBean(ds);

        BoneCPDataSource connPool = new BoneCPDataSource(config);
        connPool.setConnectionHook(STATEMENTS_CLEANER);
        return connPool;
    }

    private static void call(String setter, Object value, Object obj) {
        for (Method m : obj.getClass().getMethods()) {
            if (setter.equals(m.getName())) {
                try {
                    m.invoke(obj, value);
                } catch (IllegalAccessException | IllegalArgumentException
                        | InvocationTargetException e) {
                    throw Throwables.propagate(e);
                }
                break;
            }
        }
    }

    static void closeDataSource(DataSource ds) {
        ((BoneCPDataSource) ds).close();
    }

    static Connection newConnection(DataSource ds) {
        try {
            Connection connection = ds.getConnection();
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException("Unable to obatain connection", e);
        }
    }

    static SQLException rollbackAndRethrow(Connection c, SQLException e) throws SQLException {
        c.rollback();
        throw e;
    }
}
