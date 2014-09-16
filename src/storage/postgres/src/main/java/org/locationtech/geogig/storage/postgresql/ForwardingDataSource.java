package org.locationtech.geogig.storage.postgresql;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.google.common.base.Throwables;

class ForwardingDataSource implements DataSource {

    private final Driver driver;

    private String user;

    private String password;

    private String databaseName;

    private int portNumber = 5432;

    private String server;

    private PrintWriter logger;

    private int loginTimeout;

    public ForwardingDataSource(final String driverClassName) {
        try {
            Class<?> dclass = Class.forName(driverClassName);
            driver = (Driver) dclass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setServerName(String server) {
        this.server = server;
    }

    public void setDatabase(String databaseName) {
        this.databaseName = databaseName;
    }

    public void setPortNumber(int portNumber) {
        this.portNumber = portNumber;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(user, password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Properties info = new Properties();
        if (user != null) {
            info.put("user", user);
        }
        if (password != null) {
            info.put("password", password);
        }
        return driver.connect(getUrl(), info);
    }

    private String getUrl() {
        StringBuilder sb = new StringBuilder("jdbc:postgresql://").append(server);
        if (portNumber != 0) {
            sb.append(':').append(portNumber);
        }
        sb.append('/').append(databaseName);
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
