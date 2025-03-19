package com.salesforce.jdbc;

import com.salesforce.api.ForceApi;
import com.salesforce.api.ForceException;
import com.salesforce.api.ForceConnection;
import com.salesforce.api.ForceResult;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

public class SalesforceConnection implements Connection {
    private final ForceApi forceApi;
    private boolean closed = false;
    private boolean autoCommit = true;
    private int transactionIsolation = Connection.TRANSACTION_NONE;
    private final List<Statement> statements = new ArrayList<>();

    public SalesforceConnection(String instanceUrl, String username, String password, String securityToken) throws SQLException {
        try {
            ForceConnection forceConnection = new ForceConnection(instanceUrl, username, password + securityToken);
            this.forceApi = new ForceApi(forceConnection);
        } catch (ForceException e) {
            throw new SQLException("Failed to connect to Salesforce", e);
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        SalesforceStatement statement = new SalesforceStatement(this);
        statements.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        SalesforcePreparedStatement statement = new SalesforcePreparedStatement(this, sql);
        statements.add(statement);
        return statement;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement is not supported");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        // Convert SQL to SOQL
        return convertSQLToSOQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        // Salesforce REST API doesn't support transactions
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        // Salesforce REST API doesn't support transactions
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            for (Statement stmt : statements) {
                stmt.close();
            }
            statements.clear();
            closed = true;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new SalesforceDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        if (level != Connection.TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException("Only TRANSACTION_NONE is supported");
        }
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement is not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement is not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("CLOB is not supported");
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("BLOB is not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("NCLOB is not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("SQLXML is not supported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        try {
            forceApi.query("SELECT Id FROM User LIMIT 1");
            return true;
        } catch (ForceException e) {
            return false;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // Not supported
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // Not supported
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Array is not supported");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Struct is not supported");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        return 0;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }

    private String convertSQLToSOQL(String sql) {
        // Basic SQL to SOQL conversion
        // This is a simplified version and should be enhanced based on requirements
        return sql.replaceAll("(?i)SELECT\\s+\\*\\s+FROM", "SELECT Id FROM")
                  .replaceAll("(?i)WHERE\\s+", "WHERE ")
                  .replaceAll("(?i)AND\\s+", "AND ")
                  .replaceAll("(?i)OR\\s+", "OR ");
    }

    // Internal method to execute SOQL query
    ForceResult executeQuery(String soql) throws SQLException {
        try {
            return forceApi.query(soql);
        } catch (ForceException e) {
            throw new SQLException("Failed to execute query", e);
        }
    }
} 