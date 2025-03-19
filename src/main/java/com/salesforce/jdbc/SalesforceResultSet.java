package com.salesforce.jdbc;

import com.salesforce.api.ForceResult;
import com.salesforce.api.ForceRecord;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SalesforceResultSet implements ResultSet {
    private final SalesforceStatement statement;
    private final ForceResult forceResult;
    private final List<ForceRecord> records;
    private int currentRow = -1;
    private boolean closed = false;
    private final Map<String, Integer> columnMap;

    public SalesforceResultSet(SalesforceStatement statement, ForceResult forceResult) {
        this.statement = statement;
        this.forceResult = forceResult;
        this.records = forceResult.getRecords();
        this.columnMap = createColumnMap();
    }

    private Map<String, Integer> createColumnMap() {
        if (records.isEmpty()) {
            return Map.of();
        }
        ForceRecord firstRecord = records.get(0);
        Map<String, Object> fields = firstRecord.getFields();
        Map<String, Integer> map = new java.util.HashMap<>();
        int columnIndex = 1;
        for (String fieldName : fields.keySet()) {
            map.put(fieldName, columnIndex++);
        }
        return map;
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (currentRow < records.size() - 1) {
            currentRow++;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return false; // Not supported
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getString(columnName);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getBoolean(columnName);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getByte(columnName);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getShort(columnName);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getInt(columnName);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getLong(columnName);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getFloat(columnName);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getDouble(columnName);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getBigDecimal(columnName, scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getBytes(columnName);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getDate(columnName);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getTime(columnName);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getTimestamp(columnName);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getAsciiStream(columnName);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getUnicodeStream(columnName);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getBinaryStream(columnName);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? value.toString() : null;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null && Boolean.parseBoolean(value.toString());
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? Byte.parseByte(value.toString()) : 0;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? Short.parseShort(value.toString()) : 0;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? Integer.parseInt(value.toString()) : 0;
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? Long.parseLong(value.toString()) : 0;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? Float.parseFloat(value.toString()) : 0;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? Double.parseDouble(value.toString()) : 0;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? new BigDecimal(value.toString()).setScale(scale) : null;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? value.toString().getBytes() : null;
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? Date.valueOf(value.toString()) : null;
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? Time.valueOf(value.toString()) : null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? Timestamp.valueOf(value.toString()) : null;
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.US_ASCII)) : null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.UTF_16)) : null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? new ByteArrayInputStream(value.toString().getBytes()) : null;
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
    public String getCursorName() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new SalesforceResultSetMetaData(this);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getObject(columnName);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        return getFieldValue(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        Integer columnIndex = columnMap.get(columnLabel);
        if (columnIndex == null) {
            throw new SQLException("Column not found: " + columnLabel);
        }
        return columnIndex;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getCharacterStream(columnName);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? new StringReader(value.toString()) : null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getBigDecimal(columnName);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? new BigDecimal(value.toString()) : null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return currentRow == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return currentRow >= records.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return currentRow == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return currentRow == records.size() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        currentRow = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        currentRow = records.size();
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        if (!records.isEmpty()) {
            currentRow = 0;
            return true;
        }
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        if (!records.isEmpty()) {
            currentRow = records.size() - 1;
            return true;
        }
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return currentRow + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();
        if (row < 1 || row > records.size()) {
            return false;
        }
        currentRow = row - 1;
        return true;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClosed();
        int newRow = currentRow + rows;
        if (newRow < -1 || newRow >= records.size()) {
            return false;
        }
        currentRow = newRow;
        return true;
    }

    @Override
    public boolean previous() throws SQLException {
        checkClosed();
        if (currentRow > 0) {
            currentRow--;
            return true;
        }
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
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void insertRow() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Inserts are not supported");
    }

    @Override
    public void updateRow() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void deleteRow() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Deletes are not supported");
    }

    @Override
    public void refreshRow() throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Inserts are not supported");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getObject(columnName, map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getRef(columnName);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getBlob(columnName);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getClob(columnName);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getArray(columnName);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        checkRow();
        return getFieldValue(columnLabel);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        throw new SQLFeatureNotSupportedException("REF type is not supported");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        throw new SQLFeatureNotSupportedException("BLOB type is not supported");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        throw new SQLFeatureNotSupportedException("CLOB type is not supported");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        throw new SQLFeatureNotSupportedException("Array type is not supported");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getDate(columnName, cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        checkClosed();
        checkRow();
        Date date = getDate(columnLabel);
        if (date == null) {
            return null;
        }
        Calendar resultCal = Calendar.getInstance();
        resultCal.setTime(date);
        return new Date(resultCal.getTimeInMillis());
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getTime(columnName, cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        checkClosed();
        checkRow();
        Time time = getTime(columnLabel);
        if (time == null) {
            return null;
        }
        Calendar resultCal = Calendar.getInstance();
        resultCal.setTime(time);
        return new Time(resultCal.getTimeInMillis());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getTimestamp(columnName, cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        checkClosed();
        checkRow();
        Timestamp timestamp = getTimestamp(columnLabel);
        if (timestamp == null) {
            return null;
        }
        Calendar resultCal = Calendar.getInstance();
        resultCal.setTime(timestamp);
        return new Timestamp(resultCal.getTimeInMillis());
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getURL(columnName);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        if (value == null) {
            return null;
        }
        try {
            return new URL(value.toString());
        } catch (java.net.MalformedURLException e) {
            throw new SQLException("Invalid URL format", e);
        }
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getRowId(columnName);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        Object value = getFieldValue(columnLabel);
        return value != null ? RowId.valueOf(value.toString()) : null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getNClob(columnName);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        throw new SQLFeatureNotSupportedException("NCLOB type is not supported");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getSQLXML(columnName);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        throw new SQLFeatureNotSupportedException("SQLXML type is not supported");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getNString(columnName);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        String columnName = getColumnName(columnIndex);
        return getNCharacterStream(columnName);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        checkClosed();
        checkRow();
        return getCharacterStream(columnLabel);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }

    private void checkRow() throws SQLException {
        if (currentRow < 0 || currentRow >= records.size()) {
            throw new SQLException("No current row");
        }
    }

    private String getColumnName(int columnIndex) throws SQLException {
        for (Map.Entry<String, Integer> entry : columnMap.entrySet()) {
            if (entry.getValue() == columnIndex) {
                return entry.getKey();
            }
        }
        throw new SQLException("Invalid column index: " + columnIndex);
    }

    private Object getFieldValue(String columnLabel) throws SQLException {
        checkRow();
        ForceRecord record = records.get(currentRow);
        return record.getField(columnLabel);
    }
} 