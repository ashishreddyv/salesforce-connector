package com.salesforce.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class SalesforceDriverTest {
    private static final String TEST_URL = "jdbc:salesforce://test.salesforce.com";
    private static final String TEST_USER = "test@example.com";
    private static final String TEST_PASSWORD = "test-password";
    private static final String TEST_TOKEN = "test-token";

    private SalesforceDriver driver;
    private Properties props;

    @BeforeEach
    void setUp() {
        driver = new SalesforceDriver();
        props = new Properties();
        props.setProperty("user", TEST_USER);
        props.setProperty("password", TEST_PASSWORD);
        props.setProperty("securityToken", TEST_TOKEN);
    }

    @Test
    void testAcceptsURL() throws SQLException {
        assertTrue(driver.acceptsURL(TEST_URL));
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306/test"));
    }

    @Test
    void testGetMajorVersion() {
        assertEquals(1, driver.getMajorVersion());
    }

    @Test
    void testGetMinorVersion() {
        assertEquals(0, driver.getMinorVersion());
    }

    @Test
    void testJdbcCompliant() {
        assertFalse(driver.jdbcCompliant());
    }

    @Test
    void testGetParentLogger() {
        assertNotNull(driver.getParentLogger());
    }

    @Test
    @Disabled("Requires actual Salesforce credentials")
    void testConnect() throws SQLException {
        Connection conn = driver.connect(TEST_URL, props);
        assertNotNull(conn);
        assertFalse(conn.isClosed());
        conn.close();
    }

    @Test
    void testConnectWithMissingCredentials() {
        Properties emptyProps = new Properties();
        assertThrows(SQLException.class, () -> driver.connect(TEST_URL, emptyProps));
    }

    @Test
    void testGetPropertyInfo() throws SQLException {
        DriverPropertyInfo[] info = driver.getPropertyInfo(TEST_URL, props);
        assertNotNull(info);
        assertTrue(info.length > 0);
        
        // Verify required properties
        boolean hasUser = false;
        boolean hasPassword = false;
        boolean hasToken = false;
        
        for (DriverPropertyInfo prop : info) {
            if ("user".equals(prop.name)) hasUser = true;
            if ("password".equals(prop.name)) hasPassword = true;
            if ("securityToken".equals(prop.name)) hasToken = true;
        }
        
        assertTrue(hasUser, "Missing user property");
        assertTrue(hasPassword, "Missing password property");
        assertTrue(hasToken, "Missing securityToken property");
    }

    @Test
    @Disabled("Requires actual Salesforce credentials")
    void testExecuteQuery() throws SQLException {
        Connection conn = driver.connect(TEST_URL, props);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT Id, Name FROM Account LIMIT 1");
        
        assertNotNull(rs);
        assertTrue(rs.next());
        assertNotNull(rs.getString("Id"));
        assertNotNull(rs.getString("Name"));
        
        rs.close();
        stmt.close();
        conn.close();
    }

    @Test
    @Disabled("Requires actual Salesforce credentials")
    void testSQLToSOQLConversion() throws SQLException {
        Connection conn = driver.connect(TEST_URL, props);
        Statement stmt = conn.createStatement();
        
        // Test simple SELECT
        String sql1 = "SELECT Id, Name FROM Account WHERE Industry = 'Technology'";
        String soql1 = conn.nativeSQL(sql1);
        assertEquals(sql1, soql1);
        
        // Test subquery
        String sql2 = "SELECT a.Name, o.Amount FROM Account a LEFT JOIN Opportunity o ON a.Id = o.AccountId";
        String soql2 = conn.nativeSQL(sql2);
        assertTrue(soql2.contains("(SELECT Amount FROM Opportunities"));
        
        stmt.close();
        conn.close();
    }
} 