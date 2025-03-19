# Salesforce JDBC Connector

A JDBC driver that allows you to connect to Salesforce using standard JDBC queries. This driver converts SQL queries into Salesforce SOQL queries and executes them using the Salesforce REST API.

## Features

- Connect to Salesforce using standard JDBC
- Convert SQL queries to SOQL
- Support for basic SQL operations (SELECT, WHERE, ORDER BY, GROUP BY)
- Support for subqueries
- Support for outer joins
- Read-only access to Salesforce data

## Limitations

- No transaction support
- No stored procedures
- No batch updates
- Limited SQL grammar support (mainly SOQL)
- Forward-only, read-only result sets
- No write operations (INSERT, UPDATE, DELETE)

## Requirements

- Java 11 or higher
- Maven 3.6 or higher
- Salesforce account with API access

## Dependencies

- Salesforce REST API Client
- JDBC API
- SLF4J for logging
- Jackson for JSON processing
- JUnit for testing

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/salesforce-connector.git
cd salesforce-connector
```

2. Build the project:
```bash
mvn clean install
```

3. Add the JAR to your project's classpath.

## Usage

1. Register the driver:
```java
Class.forName("com.salesforce.jdbc.SalesforceDriver");
```

2. Create a connection:
```java
String url = "jdbc:salesforce://login.salesforce.com";
Properties props = new Properties();
props.setProperty("user", "your-username");
props.setProperty("password", "your-password");
props.setProperty("securityToken", "your-security-token");

Connection conn = DriverManager.getConnection(url, props);
```

3. Execute queries:
```java
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT Id, Name FROM Account WHERE Industry = 'Technology'");
while (rs.next()) {
    System.out.println(rs.getString("Name"));
}
```

## SQL to SOQL Conversion

The driver converts SQL queries to SOQL. Here are some examples:

```sql
-- SQL
SELECT Id, Name FROM Account WHERE Industry = 'Technology' ORDER BY Name

-- Converted to SOQL
SELECT Id, Name FROM Account WHERE Industry = 'Technology' ORDER BY Name
```

```sql
-- SQL
SELECT a.Name, o.Amount 
FROM Account a 
LEFT JOIN Opportunity o ON a.Id = o.AccountId 
WHERE o.Amount > 10000

-- Converted to SOQL
SELECT Name, (SELECT Amount FROM Opportunities WHERE Amount > 10000) FROM Account
```

## Error Handling

The driver throws standard JDBC SQLExceptions with appropriate error codes and messages. Common errors include:

- Invalid credentials
- Network connectivity issues
- Invalid SQL syntax
- Unsupported SQL features

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 