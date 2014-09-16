/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.postgresql;

import static java.lang.String.format;
import static org.locationtech.geogig.storage.postgresql.PGObjectDatabase.STAGE;
import static org.locationtech.geogig.storage.postgresql.PGStorage.FORMAT_NAME;
import static org.locationtech.geogig.storage.postgresql.PGStorage.VERSION;
import static org.locationtech.geogig.storage.postgresql.PGStorage.log;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.plumbing.merge.Conflict;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.storage.AbstractStagingDatabase;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

/**
 * Base class for SQLite based staging database.
 * 
 * @author Justin Deoliveira, Boundless
 * 
 */
public class PGStagingDatabase extends AbstractStagingDatabase {
    final static Logger LOG = LoggerFactory.getLogger(PGStagingDatabase.class);

    private static final String NULL_NAMESPACE = "nil";

    final static String CONFLICTS = "conflicts";

    final ConfigDatabase configdb;

    final Platform platform;

    private DataSource cx;

    @Inject
    public PGStagingDatabase(ObjectDatabase repoDb, ConfigDatabase configdb, Platform platform) {

        super(Suppliers.ofInstance(repoDb), Suppliers.ofInstance(new PGObjectDatabase(configdb,
                platform, STAGE)));

        this.configdb = configdb;
        this.platform = platform;
    }

    @Override
    public void open() {
        super.open();

        cx = ((PGObjectDatabase) stagingDb).dataSource;
        init(cx);
    }

    @Override
    public Optional<Conflict> getConflict(String namespace, String path) {
        List<Conflict> conflicts = getConflicts(namespace(namespace), path);
        if (conflicts.isEmpty()) {
            return Optional.absent();
        }
        return Optional.of(conflicts.get(0));
    }

    @Override
    public boolean hasConflicts(String namespace) {
        // int count = count(namespace(namespace), cx);
        // return count > 0;
        return false;
    }

    @Override
    public List<Conflict> getConflicts(@Nullable String namespace, String pathFilter) {
        // return Lists.newArrayList(Iterables.transform(get(namespace(namespace), pathFilter, cx),
        // StringToConflict.INSTANCE));
        return ImmutableList.of();
    }

    @Override
    public void addConflict(String namespace, Conflict conflict) {
        put(namespace(namespace), conflict.getPath(), conflict.toString(), cx);
    }

    private String namespace(String namespace) {
        return namespace == null ? NULL_NAMESPACE : namespace;
    }

    @Override
    public void removeConflict(String namespace, String path) {
        remove(namespace(namespace), path, cx);
    }

    @Override
    public void removeConflicts(String namespace) {
        namespace = namespace(namespace);
        for (Conflict c : Iterables.transform(get(namespace, null, cx), StringToConflict.INSTANCE)) {
            removeConflict(namespace, c.getPath());
        }
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.STAGING.configure(configdb, FORMAT_NAME, VERSION);
    }

    @Override
    public void checkConfig() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.STAGING.verify(configdb, FORMAT_NAME, VERSION);
    }

    /**
     * Creates the object table with the following schema:
     * 
     * <pre>
     * conflicts(namespace:varchar, path:varchar, conflict:varchar)
     * </pre>
     * 
     * Implementations of this method should be prepared to be called multiple times, so must check
     * if the table already exists.
     * 
     * @param cx The connection object.
     */
    protected void init(DataSource ds) {
        new DbOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws SQLException {
                try (ResultSet tables = cx.getMetaData().getTables(null, null, CONFLICTS, null)) {
                    if (tables.next()) {
                        return null;
                    }
                }
                String sql = format("CREATE TABLE IF NOT EXISTS %s (namespace VARCHAR, "
                        + "path VARCHAR, conflict VARCHAR, PRIMARY KEY(namespace,path))", CONFLICTS);

                LOG.debug(sql);
                try (Statement ps = cx.createStatement()) {
                    ps.execute(sql);
                }
                return null;
            }
        }.run(ds);
    }

    /**
     * Returns the number of conflicts matching the specified namespace filter.
     * 
     * @param namespace Namespace value, may be <code>null</code>.
     * 
     */
    protected int count(final String namespace, DataSource ds) {
        Integer count = new DbOp<Integer>() {
            @Override
            protected Integer doRun(Connection cx) throws IOException, SQLException {
                String sql = format("SELECT count(*) FROM %s WHERE namespace = ?", CONFLICTS);

                int count = 0;
                PreparedStatement ps = PGStorage.prepareStatement(cx, log(sql, LOG, namespace));
                ps.setString(1, namespace);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        count = rs.getInt(1);
                    }
                }

                return Integer.valueOf(count);
            }
        }.run(ds);

        return count.intValue();
    }

    /**
     * Returns all conflicts matching the specified namespace and pathFilter.
     * 
     * @param namespace Namespace value.
     * @param pathFilter Path filter, may be <code>null</code>.
     * 
     */
    protected List<String> get(final String namespace, @Nullable final String pathFilter,
            DataSource ds) {
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(ds);

        List<String> rs = new DbOp<List<String>>() {
            @Override
            protected List<String> doRun(Connection cx) throws IOException, SQLException {
                // final String sql;
                // if (pathFilter == null) {
                // sql = format("SELECT conflict FROM %s WHERE namespace = ? AND path IS NULL",
                // CONFLICTS);
                // } else {
                // sql = format("SELECT conflict FROM %s WHERE namespace = ? AND path LIKE '%%%s%'",
                // CONFLICTS, pathFilter);
                // }
                // List<String> conflicts = new ArrayList<>();
                // try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, namespace))) {
                // ps.setString(1, namespace);
                // try (ResultSet rs = ps.executeQuery()) {
                // while (rs.next()) {
                // conflicts.add(rs.getString(1));
                // }
                // }
                // }
                // return conflicts;
                return ImmutableList.of();
            }
        }.run(ds);
        return rs;
    }

    /**
     * Adds a conflict.
     * 
     * @param namespace The conflict namespace.
     * @param path The path of the conflict.
     * @param conflict The conflict value.
     */
    protected void put(final String namespace, final String path, final String conflict,
            DataSource ds) {
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(path);
        Preconditions.checkNotNull(conflict);
        new DbOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                // String sql = format("INSERT OR REPLACE INTO %s VALUES (?,?,?)", CONFLICTS);
                String sql = format("INSERT INTO %s VALUES (?,?,?)", CONFLICTS);

                log(sql, LOG, namespace, path, conflict);

                PreparedStatement ps = PGStorage.prepareStatement(cx, sql);
                ps.setString(1, namespace);
                ps.setString(2, path);
                ps.setString(3, conflict);

                ps.executeUpdate();

                return null;
            }
        }.run(ds);
    }

    /**
     * Removed a conflict.
     * 
     * @param namespace The conflict namespace.
     * @param path The path of the conflict.
     */
    protected void remove(final String namespace, @Nullable final String path, DataSource ds) {
        new DbOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                final String sql;
                if (path == null) {
                    sql = format("DELETE FROM %s WHERE namespace = ?", CONFLICTS);
                    log(sql, LOG, namespace);
                } else {
                    sql = format("DELETE FROM %s WHERE namespace = ? AND path = ?", CONFLICTS);
                    log(sql, LOG, namespace, path);
                }

                PreparedStatement ps = PGStorage.prepareStatement(cx, sql);
                ps.setString(1, namespace);
                if (path != null) {
                    ps.setString(2, path);
                }
                ps.executeUpdate();

                return null;
            }
        }.run(ds);
    }
}
