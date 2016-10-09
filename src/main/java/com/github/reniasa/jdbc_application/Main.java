package com.github.reniasa.jdbc_application;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
    
    private static final Logger log = LogManager.getLogger();
    
    public static void main(String[] args) {
        DatabaseConnection database;
        PreparedStatement preparedStatement;
        try {
            database = new DatabaseConnection();
            
            database.createTables();
            
            database.insertStudent(1, "John Smith", "male", 23, 2);
            database.insertStudent(2, "Rebecca Milson", "female", 27, 3);
            database.insertStudent(3, "George Heartbreaker", "male", 19, 1);
            database.insertStudent(4, "Deepika Chopra", "female", 25, 3);
            
            database.insertFaculty(100, "Engineering");
            database.insertFaculty(101, "Philosophy");
            database.insertFaculty(102, "Law and administration");
            database.insertFaculty(103, "Languages");
            
            database.insertClass(1000, "Introduction to labour law", 102);
            database.insertClass(1001, "Graph algorithms", 100);
            database.insertClass(1002, "Existentialism in 20th century", 101);
            database.insertClass(1003, "English grammar", 103);
            database.insertClass(1004, "From Plato to Kant", 101);
            
            database.insertEnrollment(1, 1000);
            database.insertEnrollment(1, 1002);
            database.insertEnrollment(1, 1003);
            database.insertEnrollment(1, 1004);
            database.insertEnrollment(2, 1002);
            database.insertEnrollment(2, 1003);
            database.insertEnrollment(4, 1000);
            database.insertEnrollment(4, 1002);
            database.insertEnrollment(4, 1003);
            
            String sql1 = "SELECT pkey, name FROM Student";
            database.logResultTwoColumns(database.executeQuery(database.prepareStatement(sql1)), 1);
            String sql2 = "SELECT pkey, name FROM Student LEFT JOIN Enrollment ON pkey=fkey_student WHERE fkey_student IS NULL";
            database.logResultTwoColumns(database.executeQuery(database.prepareStatement(sql2)), 2);
            String sql3 = "SELECT pkey, name FROM Student INNER JOIN Enrollment ON pkey=fkey_student WHERE fkey_class=? AND sex=?";
            preparedStatement = database.prepareStatement(sql3);
            preparedStatement.setInt(1, 1002);
            preparedStatement.setString(2, "female");
            database.logResultTwoColumns(database.executeQuery(preparedStatement), 3);
            String sql4 = "SELECT Faculty.pkey, Faculty.name FROM Faculty INNER JOIN Class ON Faculty.pkey=Class.fkey_faculty "
                        + "LEFT JOIN Enrollment ON Class.pkey=Enrollment.fkey_class WHERE fkey_class IS NULL";
            database.logResultTwoColumns(database.executeQuery(database.prepareStatement(sql4)), 4);
            String sql5 = "SELECT MAX(Student.age) AS max_age FROM Student INNER JOIN Enrollment ON pkey=fkey_student "
                        + "WHERE fkey_class=?";
            preparedStatement = database.prepareStatement(sql5);
            preparedStatement.setInt(1, 1000);
            database.logResultOneColumn(database.executeQuery(preparedStatement), 5);
            String sql6 = "SELECT name FROM Class INNER JOIN Enrollment ON pkey=fkey_class "
                    + "GROUP BY name HAVING COUNT(fkey_student)>=?";
            preparedStatement = database.prepareStatement(sql6);
            preparedStatement.setInt(1, 2);
            database.logResultOneColumn(database.executeQuery(preparedStatement), 6);
            String sql7 = "SELECT level, AVG(age) as avg_age FROM Student JOIN Enrollment ON (pkey = fkey_student) GROUP BY level";
            database.logResultTwoColumns(database.executeQuery(database.prepareStatement(sql7)), 7);
        } catch (SQLException e) {
            log.error(e.getMessage());
        }

    }

}
