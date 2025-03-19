package com.salesforce.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.DriverPropertyInfo;
import java.util.Properties;
import java.util.logging.Logger;

public class SalesforceDriver implements java.sql.Driver {
    private static final String URL_PREFIX = "jdbc:salesforce:";
    private static final int MAJOR_VERSION = 1;
    private static final int MINOR_VERSION = 0;

    static {
        try {
            DriverManager.registerDriver(new SalesforceDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register Salesforce JDBC driver", e);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public java.sql.Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        // Extract Salesforce connection details from URL and properties
        String instanceUrl = url.substring(URL_PREFIX.length());
        String username = info.getProperty("user");
        String password = info.getProperty("password");
        String securityToken = info.getProperty("securityToken");

        if (username == null || password == null) {
            throw new SQLException("Username and password are required");
        }

        return new SalesforceConnection(instanceUrl, username, password, securityToken);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[]{
            new DriverPropertyInfo("user", info.getProperty("user")),
            new DriverPropertyInfo("password", info.getProperty("password")),
            new DriverPropertyInfo("securityToken", info.getProperty("securityToken"))
        };
    }

    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false; // Not fully JDBC compliant as we don't support all JDBC features
    }

    @Override
    public Logger getParentLogger() {
        return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    }
} 