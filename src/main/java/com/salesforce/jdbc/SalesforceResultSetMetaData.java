package com.salesforce.jdbc;

import java.sql.*;

public class SalesforceResultSetMetaData implements ResultSetMetaData {
    private final SalesforceResultSet resultSet;
    private final Map<String, Integer> columnMap;

    public SalesforceResultSetMetaData(SalesforceResultSet resultSet) {
        this.resultSet = resultSet;
        this.columnMap = resultSet.getColumnMap();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnMap.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0; // Not supported
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        for (Map.Entry<String, Integer> entry : columnMap.entrySet()) {
            if (entry.getValue() == column) {
                return entry.getKey();
            }
        }
        throw new SQLException("Invalid column index: " + column);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0; // Not supported
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0; // Not supported
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return Types.VARCHAR; // Default to VARCHAR for all types
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return "VARCHAR";
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return String.class.getName();
    }
} 