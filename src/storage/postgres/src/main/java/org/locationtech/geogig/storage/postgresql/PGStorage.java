/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.postgresql;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
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
import org.locationtech.geogig.storage.ConfigDatabase;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.jolbox.bonecp.BoneCPDataSource;

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
    public static final String VERSION2 = "2";

    public static final String VERSION3 = "3";

    static Map<Connection, Map<String, PreparedStatement>> OPEN_STATEMENTS = new IdentityHashMap<>();

    private static final PGDataSourceManager DATASOURCE_POOL = new PGDataSourceManager();

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

    synchronized static DataSource newDataSource(ConfigDatabase configDb) {
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

        Optional<String> serverConf = configDb.get("postgres.server");
        Optional<Integer> portNumberConf = configDb.get("postgres.port", Integer.class);
        Optional<String> databaseNameConf = configDb.get("postgres.database");
        Optional<String> userConf = configDb.get("postgres.user");
        Optional<String> passwordConf = configDb.get("postgres.password");
        
        checkState(serverConf.isPresent(), "postgres.server config is not set");
        checkState(databaseNameConf.isPresent(), "postgres.database config is not set");
        checkState(userConf.isPresent(), "postgres.user config is not set");
        checkState(passwordConf.isPresent(), "postgres.password config is not set");

        String server = serverConf.get();
        int portNumber = portNumberConf.or(5432);
        String databaseName = databaseNameConf.get();
        String user = userConf.get();
        String password = passwordConf.get();
        Config config = new Config(driverName, server, portNumber, databaseName, user, password);
        BoneCPDataSource dataSource = DATASOURCE_POOL.acquire(config);
        return dataSource;
    }

    static void closeDataSource(DataSource ds) {
        DATASOURCE_POOL.release((BoneCPDataSource) ds);
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
