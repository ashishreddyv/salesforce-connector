package com.salesforce.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class SalesforceStatement implements Statement {
    private final SalesforceConnection connection;
    private boolean closed = false;
    private int maxRows = 0;
    private int fetchSize = 0;
    private int queryTimeout = 0;
    private final List<ResultSet> resultSets = new ArrayList<>();

    public SalesforceStatement(SalesforceConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        String soql = connection.nativeSQL(sql);
        ForceResult result = connection.executeQuery(soql);
        SalesforceResultSet resultSet = new SalesforceResultSet(this, result);
        resultSets.add(resultSet);
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        // Convert SQL to Salesforce REST API call
        // This is a simplified version and should be enhanced based on requirements
        String soql = connection.nativeSQL(sql);
        ForceResult result = connection.executeQuery(soql);
        return result.getTotalSize();
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            for (ResultSet rs : resultSets) {
                rs.close();
            }
            resultSets.clear();
            closed = true;
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed();
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        // Not supported
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
    public void setCursorName(String name) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        // For now, we'll treat all statements as queries
        executeQuery(sql);
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return resultSets.isEmpty() ? null : resultSets.get(resultSets.size() - 1);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Batch operations are not supported");
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Batch operations are not supported");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Batch operations are not supported");
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkClosed();
        return false;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }
} 