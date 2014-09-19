package org.locationtech.geogig.storage.postgresql;

import static com.google.common.base.Objects.equal;

import com.google.common.base.Objects;

class Config {

    final String user;

    final String password;

    final String databaseName;

    final int portNumber;

    final String server;

    final String driverClassName;

    public Config(final String driverClassName, String server, int portNumber, String databaseName,
            String user, String password) {
        this.driverClassName = driverClassName;
        this.server = server;
        this.portNumber = portNumber;
        this.databaseName = databaseName;
        this.user = user;
        this.password = password;
    }

    /**
     * Equality comparison based on driver, server, port, user, and password, and database name.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Config)) {
            return false;
        }
        Config d = (Config) o;
        return equal(driverClassName, d.driverClassName) && equal(server, d.server)
                && equal(portNumber, d.portNumber) && equal(databaseName, d.databaseName)
                && equal(user, d.user) && equal(password, d.password);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(driverClassName, server, portNumber, databaseName, user, password);
    }

}
