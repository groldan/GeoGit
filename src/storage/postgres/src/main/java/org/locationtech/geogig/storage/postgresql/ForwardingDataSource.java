package org.locationtech.geogig.storage.postgresql;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

class ForwardingDataSource implements DataSource {

    private final Driver driver;

    private PrintWriter logger;

    private int loginTimeout;

    private final Config config;

    public ForwardingDataSource(final Config config) {
        this.config = config;
        try {
            Class<?> dclass = Class.forName(config.driverClassName);
            Preconditions.checkArgument(Driver.class.isAssignableFrom(dclass),
                    "Provided driver class name does not extend java.sql.Driver");
            this.driver = (Driver) dclass.newInstance();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

    }

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(config.user, config.password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Properties info = new Properties();
        if (username != null) {
            info.put("user", username);
        }
        if (password != null) {
            info.put("password", password);
        }
        return driver.connect(getUrl(), info);
    }

    public String getUrl() {
        StringBuilder sb = new StringBuilder("jdbc:postgresql://").append(config.server);
        if (config.portNumber != 0) {
            sb.append(':').append(config.portNumber);
        }
        sb.append('/').append(config.databaseName);
        sb.append("?loginTimeout=").append(loginTimeout);
        sb.append("&binaryTransfer=true");
        sb.append("&prepareThreshold=3");

        return sb.toString();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return logger;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.logger = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger() is not implemented");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException(getClass().getName() + " cannot be unwrapped as '" + iface.getName()
                + "'");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

}
