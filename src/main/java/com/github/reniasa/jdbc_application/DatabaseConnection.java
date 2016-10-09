package com.github.reniasa.jdbc_application;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DatabaseConnection {
    private final String USERNAME = "SA";
    private final String PASSWORD = "";
    private final String URL      = "jdbc:hsqldb:hsql://127.0.0.1:9001/test-db";
    
    private Connection connection;
    private PreparedStatement preparedStatement = null;
    
    private static final Logger log = LogManager.getLogger();
    
    public DatabaseConnection() throws SQLException {
        connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
    }
    
    public void createTables() throws SQLException {
        connection.setAutoCommit(false);
        
        String createTableStudent = "CREATE TABLE IF NOT EXISTS Student("
                                  + "pkey INT PRIMARY KEY, "
                                  + "name VARCHAR(40) NOT NULL, "
                                  + "sex VARCHAR(10) NOT NULL, "
                                  + "age INT NOT NULL, "
                                  + "level INT NOT NULL "
                                  + ")";
        
        preparedStatement = connection.prepareStatement(createTableStudent);
        preparedStatement.executeUpdate();
        
        String createTableFaculty = "CREATE TABLE IF NOT EXISTS Faculty("
                                  + "pkey INT PRIMARY KEY, "
                                  + "name VARCHAR(40) NOT NULL "
                                  + ")";
        
        preparedStatement = connection.prepareStatement(createTableFaculty);
        preparedStatement.executeUpdate();
        
        String createTableClass = "CREATE TABLE IF NOT EXISTS Class("
                                + "pkey INT PRIMARY KEY, "
                                + "name VARCHAR(40) NOT NULL, "
                                + "fkey_faculty INT FOREIGN KEY REFERENCES Faculty(pkey)"
                                + ")";
        
        preparedStatement = connection.prepareStatement(createTableClass);
        preparedStatement.executeUpdate();
        
        String createTableEnrollment = "CREATE TABLE IF NOT EXISTS Enrollment("
                                     + "fkey_student INT FOREIGN KEY REFERENCES Student(pkey), "
                                     + "fkey_class INT FOREIGN KEY REFERENCES Class(pkey)"
                                     + ")";
        
        preparedStatement = connection.prepareStatement(createTableEnrollment);
        preparedStatement.executeUpdate();
        
        
        connection.commit();
        
        preparedStatement.close();
        connection.setAutoCommit(true);
    }
    
    public void insertStudent(int pkey, String name, String sex, int age, int level) throws SQLException {
        String insertStudentSql = "INSERT INTO Student (pkey, name, sex, age, level) "
                                + "VALUES (?, ?, ?, ?, ?)";
        
        preparedStatement = connection.prepareStatement(insertStudentSql);
        preparedStatement.setInt(1, pkey);
        preparedStatement.setString(2, name);
        preparedStatement.setString(3, sex);
        preparedStatement.setInt(4, age);
        preparedStatement.setInt(5, level);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }
    
    public void insertFaculty(int pkey, String name) throws SQLException {
        String insertFacultySql = "INSERT INTO Faculty (pkey, name) "
                                + "VALUES (?, ?)";
        
        preparedStatement = connection.prepareStatement(insertFacultySql);
        preparedStatement.setInt(1, pkey);
        preparedStatement.setString(2, name);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }
    
    public void insertClass(int pkey, String name, int fkey_faculty) throws SQLException {
        String insertClassSql = "INSERT INTO Class (pkey, name, fkey_faculty) "
                                + "VALUES (?, ?, ?)";
        
        preparedStatement = connection.prepareStatement(insertClassSql);
        preparedStatement.setInt(1, pkey);
        preparedStatement.setString(2, name);
        preparedStatement.setInt(3, fkey_faculty);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }
    
    public void insertEnrollment(int fkey_student, int fkey_class) throws SQLException {
        String insertClassSql = "INSERT INTO Enrollment (fkey_student, fkey_class) "
                                + "VALUES (?, ?)";
        
        preparedStatement = connection.prepareStatement(insertClassSql);
        preparedStatement.setInt(1, fkey_student);
        preparedStatement.setInt(2, fkey_class);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }
    
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }
    
    public ResultSet executeQuery(PreparedStatement preparedStatement) throws SQLException {
        return preparedStatement.executeQuery();
    }
    
    public void logResultOneColumn(ResultSet result, int queryIndex) throws SQLException {
        log.info("Wynik zapytania {}:", queryIndex);
        while(result.next()) {
            log.info("{}={}", result.getMetaData().getColumnName(1), result.getString(1));
        }
    }
    
    public void logResultTwoColumns(ResultSet result, int queryIndex) throws SQLException {
        log.info("Wynik zapytania {}:", queryIndex);
        while(result.next()) {
            log.info("{}={}, {}={}", result.getMetaData().getColumnName(1), result.getString(1), result.getMetaData().getColumnName(2), result.getString(2));
        }
    }
    
    public void closeConnection() throws SQLException {
        connection.close();
    }
}
