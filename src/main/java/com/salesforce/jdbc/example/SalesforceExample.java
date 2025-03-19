package com.salesforce.jdbc.example;

import com.salesforce.jdbc.SalesforceDriver;

import java.sql.*;
import java.util.Properties;

public class SalesforceExample {
    public static void main(String[] args) {
        // Connection parameters
        String url = "jdbc:salesforce://login.salesforce.com";
        Properties props = new Properties();
        props.setProperty("user", "your-username");
        props.setProperty("password", "your-password");
        props.setProperty("securityToken", "your-security-token");

        try {
            // Register the driver
            Class.forName("com.salesforce.jdbc.SalesforceDriver");

            // Create a connection
            System.out.println("Connecting to Salesforce...");
            Connection conn = DriverManager.getConnection(url, props);
            System.out.println("Connected successfully!");

            // Create a statement
            Statement stmt = conn.createStatement();

            // Example 1: Simple SELECT query
            System.out.println("\nExecuting simple SELECT query...");
            ResultSet rs1 = stmt.executeQuery("SELECT Id, Name, Industry FROM Account LIMIT 5");
            System.out.println("Results:");
            while (rs1.next()) {
                System.out.printf("Account: %s (Industry: %s)%n", 
                    rs1.getString("Name"), 
                    rs1.getString("Industry"));
            }
            rs1.close();

            // Example 2: Query with WHERE clause
            System.out.println("\nExecuting query with WHERE clause...");
            ResultSet rs2 = stmt.executeQuery(
                "SELECT Id, Name, Amount FROM Opportunity WHERE Amount > 10000 LIMIT 5");
            System.out.println("Results:");
            while (rs2.next()) {
                System.out.printf("Opportunity: %s (Amount: %s)%n", 
                    rs2.getString("Name"), 
                    rs2.getString("Amount"));
            }
            rs2.close();

            // Example 3: Query with subquery (relationship query)
            System.out.println("\nExecuting query with subquery...");
            ResultSet rs3 = stmt.executeQuery(
                "SELECT a.Name, o.Amount FROM Account a " +
                "LEFT JOIN Opportunity o ON a.Id = o.AccountId " +
                "WHERE o.Amount > 10000 LIMIT 5");
            System.out.println("Results:");
            while (rs3.next()) {
                System.out.printf("Account: %s (Opportunity Amount: %s)%n", 
                    rs3.getString("Name"), 
                    rs3.getString("Amount"));
            }
            rs3.close();

            // Example 4: Query with ORDER BY
            System.out.println("\nExecuting query with ORDER BY...");
            ResultSet rs4 = stmt.executeQuery(
                "SELECT Id, Name, AnnualRevenue FROM Account " +
                "WHERE AnnualRevenue > 1000000 " +
                "ORDER BY AnnualRevenue DESC LIMIT 5");
            System.out.println("Results:");
            while (rs4.next()) {
                System.out.printf("Account: %s (Annual Revenue: %s)%n", 
                    rs4.getString("Name"), 
                    rs4.getString("AnnualRevenue"));
            }
            rs4.close();

            // Example 5: Query with aggregate functions
            System.out.println("\nExecuting query with aggregate functions...");
            ResultSet rs5 = stmt.executeQuery(
                "SELECT Industry, COUNT(Id) as Count " +
                "FROM Account " +
                "GROUP BY Industry " +
                "ORDER BY Count DESC LIMIT 5");
            System.out.println("Results:");
            while (rs5.next()) {
                System.out.printf("Industry: %s (Count: %s)%n", 
                    rs5.getString("Industry"), 
                    rs5.getString("Count"));
            }
            rs5.close();

            // Clean up
            stmt.close();
            conn.close();
            System.out.println("\nConnection closed.");

        } catch (ClassNotFoundException e) {
            System.err.println("Error: Salesforce JDBC Driver not found.");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Error: Database error occurred.");
            e.printStackTrace();
        }
    }
} 