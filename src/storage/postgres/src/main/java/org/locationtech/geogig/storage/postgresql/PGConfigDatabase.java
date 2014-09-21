/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.postgresql;

import static com.google.common.base.Preconditions.checkArgument;
import static org.locationtech.geogig.storage.postgresql.PGStorage.closeDataSource;
import static org.locationtech.geogig.storage.postgresql.PGStorage.log;
import static org.locationtech.geogig.storage.postgresql.PGStorage.newDataSource;
import static org.locationtech.geogig.storage.postgresql.PGStorage.rollbackAndRethrow;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.geotools.util.Converters;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.plumbing.ResolveGeogigDir;
import org.locationtech.geogig.api.porcelain.ConfigException;
import org.locationtech.geogig.api.porcelain.ConfigException.StatusCode;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

/**
 * Base class for SQLite based config database.
 * 
 * @author Justin Deoliveira, Boundless
 * 
 */
public class PGConfigDatabase implements ConfigDatabase {

    static final Logger LOG = LoggerFactory.getLogger(PGConfigDatabase.class);

    final Platform platform;

    File lastWorkingDir;

    File lastUserDir;

    Config local;

    Config global;

    @Inject
    public PGConfigDatabase(Platform platform) {
        this.platform = platform;
    }

    @Override
    public Optional<String> get(String key) {
        try {
            return get(new Entry(key), String.class, local());
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new ConfigException(e, StatusCode.SECTION_OR_KEY_INVALID);
        } catch (IllegalArgumentException e) {
            throw new ConfigException(e, null);
        }
    }

    @Override
    public Optional<String> getGlobal(String key) {
        return get(new Entry(key), String.class, global());
    }

    @Override
    public <T> Optional<T> get(String key, Class<T> c) {
        return get(new Entry(key), c, local());
    }

    @Override
    public <T> Optional<T> getGlobal(String key, Class<T> c) {
        return get(new Entry(key), c, global());
    }

    @Override
    public Map<String, String> getAll() {
        return all(local());
    }

    @Override
    public Map<String, String> getAllGlobal() {
        return all(global());
    }

    @Override
    public Map<String, String> getAllSection(String section) {
        return all(section, local());
    }

    @Override
    public Map<String, String> getAllSectionGlobal(String section) {
        return all(section, global());
    }

    @Override
    public List<String> getAllSubsections(String section) {
        return list(section, local());
    }

    @Override
    public List<String> getAllSubsectionsGlobal(String section) {
        return list(section, global());
    }

    @Override
    public void put(String key, Object value) {
        put(new Entry(key), value, local());
    }

    @Override
    public void putGlobal(String key, Object value) {
        put(new Entry(key), value, global());
    }

    @Override
    public void remove(String key) {
        remove(new Entry(key), local());
    }

    @Override
    public void removeGlobal(String key) {
        remove(new Entry(key), global());
    }

    @Override
    public void removeSection(String key) {
        removeAll(key, local());
    }

    @Override
    public void removeSectionGlobal(String key) {
        removeAll(key, global());
    }

    <T> Optional<T> get(Entry entry, Class<T> clazz, Config config) {
        String raw = get(entry, config);
        if (raw != null) {
            return Optional.of(convert(raw, clazz));
        }
        return Optional.absent();
    }

    <T> T convert(String value, Class<T> clazz) {
        Object v = Converters.convert(value, clazz);
        checkArgument(v != null, "Can't convert %s to %s", value, clazz.getName());
        return clazz.cast(v);
    }

    void put(Entry entry, Object value, Config config) {
        put(entry, (String) (value != null ? value.toString() : null), config);
    }

    Config local() {
        if (local == null || !lastWorkingDir.equals(platform.pwd())) {
            final Optional<URL> url = new ResolveGeogigDir(platform).call();

            if (!url.isPresent()) {
                throw new ConfigException(StatusCode.INVALID_LOCATION);
            }

            URL u = url.get();
            File localFile;
            try {
                localFile = new File(new File(u.toURI()), "config.db");
            } catch (URISyntaxException e) {
                localFile = new File(u.getPath(), "config.db");
            }

            lastWorkingDir = platform.pwd();
            local = new Config(localFile);
        }
        return local;
    }

    Config global() {
        if (global == null || !lastUserDir.equals(platform.getUserHome())) {
            File home = platform.getUserHome();

            if (home == null) {
                throw new ConfigException(StatusCode.USERHOME_NOT_SET);
            }

            File globalDir = new File(home.getPath(), ".geogig");
            if (globalDir.exists() && !globalDir.isDirectory()) {
                throw new IllegalStateException(globalDir.getAbsolutePath()
                        + " exists but is not a directory");
            }

            if (!globalDir.exists() && !globalDir.mkdir()) {
                throw new ConfigException(StatusCode.CANNOT_WRITE);
            }

            lastUserDir = home;
            global = new Config(new File(globalDir, "config.db"));
        }
        return global;
    }

    protected static class Config {
        final File file;

        public Config(File file) {
            this.file = file;
        }
    }

    protected static class Entry {
        final String section;

        final String key;

        public Entry(String sectionAndKey) {
            checkArgument(sectionAndKey != null, "Config key may not be null.");

            String[] split = sectionAndKey.split("\\.");
            section = split[0];
            key = split[1];
        }
    }

    protected String get(final Entry entry, Config config) {
        checkArgument(!Strings.isNullOrEmpty(entry.section), "Section name required");
        checkArgument(!Strings.isNullOrEmpty(entry.key), "Key required");

        return new DbOp<String>() {
            @Override
            protected String doRun(Connection cx) throws IOException, SQLException {
                String sql = "SELECT value FROM config WHERE section = ? AND key = ?";

                String s = entry.section;
                String k = entry.key;

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, s, k))) {
                    ps.setString(1, s);
                    ps.setString(2, k);

                    try (ResultSet rs = ps.executeQuery()) {
                        return rs.next() ? rs.getString(1) : null;
                    }
                }
            }
        }.run(connect(config));
    }

    protected Map<String, String> all(Config config) {
        return new DbOp<Map<String, String>>() {
            @Override
            protected Map<String, String> doRun(Connection cx) throws IOException, SQLException {
                String sql = "SELECT section,key,value FROM config";
                try (ResultSet rs = cx.createStatement().executeQuery(log(sql, LOG))) {

                    Map<String, String> all = Maps.newLinkedHashMap();
                    while (rs.next()) {
                        String entry = String.format("%s.%s", rs.getString(1), rs.getString(2));
                        all.put(entry, rs.getString(3));
                    }

                    return all;
                }
            }
        }.run(connect(config));
    }

    protected Map<String, String> all(final String section, Config config) {
        return new DbOp<Map<String, String>>() {
            @Override
            protected Map<String, String> doRun(Connection cx) throws IOException, SQLException {
                String sql = "SELECT section,key,value FROM config WHERE section = ?";

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, section))) {
                    ps.setString(1, section);
                    Map<String, String> all = Maps.newLinkedHashMap();

                    try (ResultSet rs = ps.executeQuery()) {

                        while (rs.next()) {
                            String entry = String.format("%.%", rs.getString(1), rs.getString(2));
                            all.put(entry, rs.getString(3));
                        }
                    }

                    return all;
                }
            }
        }.run(connect(config));
    }

    protected List<String> list(final String section, Config config) {
        return new DbOp<List<String>>() {
            @Override
            protected List<String> doRun(Connection cx) throws IOException, SQLException {
                String sql = "SELECT key FROM config WHERE section = ?";

                List<String> all = new ArrayList<>(1);
                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, section))) {
                    ps.setString(1, section);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            all.add(rs.getString(1));
                        }
                    }
                }
                return all;
            }

        }.run(connect(config));

    }

    protected void put(final Entry entry, final String value, Config config) {
        new DbOp<Void>() {
            @Override
            protected boolean isAutoCommit() {
                return false;
            }

            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                doRemove(entry, cx);

                String sql = "with upsert as " //
                        + "(update config set value = '?' where section = '?' and key = '?' returning *)"//
                        + "insert into config (section, key, value)" //
                        + "select ?, ?, ? WHERE not exists (select 1 from upsert)";

                // String sql = "INSERT OR UPDATE INTO config (section,key,value) VALUES (?,?,?)";

                String s = entry.section;
                String k = entry.key;

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, value, s, k, s, k,
                        value))) {
                    ps.setString(1, s);
                    ps.setString(2, k);
                    ps.setString(3, value);

                    ps.executeUpdate();
                    cx.commit();
                } catch (SQLException e) {
                    rollbackAndRethrow(cx, e);
                }
                return null;
            }
        }.run(connect(config));
    }

    protected void remove(final Entry entry, Config config) {
        new DbOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                doRemove(entry, cx);
                return null;
            }
        }.run(connect(config));
    }

    private void doRemove(final Entry entry, Connection cx) throws SQLException {

        String sql = "DELETE FROM config WHERE section = ? AND key = ?";

        String s = entry.section;
        String k = entry.key;

        try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, s, k))) {
            ps.setString(1, s);
            ps.setString(2, k);

            ps.executeUpdate();
        }

    }

    protected void removeAll(final String section, Config config) {
        new DbOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                String sql = "DELETE FROM config WHERE section = ?";

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, section))) {
                    ps.setString(1, section);
                    ps.executeUpdate();
                }
                return null;
            }
        }.run(connect(config));
    }

    private DataSource dataSource;

    synchronized DataSource connect(Config config) {
        if (this.dataSource == null) {
            this.dataSource = null;// newDataSource(config.file);

            new DbOp<Void>() {
                @Override
                protected boolean isAutoCommit() {
                    return false;
                }

                @Override
                protected Void doRun(Connection cx) throws SQLException {
                    DatabaseMetaData md = cx.getMetaData();
                    try (ResultSet tables = md.getTables(null, null, "config", null)) {
                        if (tables.next()) {
                            return null;
                        }
                    }

                    try (Statement st = cx.createStatement()) {
                        String sql = "CREATE TABLE config (section VARCHAR, key VARCHAR, value VARCHAR,"
                                + " PRIMARY KEY (section,key))";
                        st.execute(log(sql, LOG));
                        sql = "CREATE INDEX config_section_idx ON config (section)";
                        st.execute(log(sql, LOG));
                        cx.commit();
                    } catch (SQLException e) {
                        rollbackAndRethrow(cx, e);
                    }

                    return null;
                }
            }.run(dataSource);
        }
        return dataSource;
    }

    public synchronized void close() {
        if (dataSource != null) {
            closeDataSource(dataSource);
            dataSource = null;
        }
    }
}
