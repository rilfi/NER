package com.stormadvance.storm_example;

import java.sql.Connection;
import java.sql.DriverManager;
/**
 * 
 * This class return the MySQL connection.
 */
public class MySQLConnection {

	private static Connection connect = null;


	public static Connection getMySQLConnection() {
		try {
			// this will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			// setup the connection with the DB.
			connect = DriverManager
					.getConnection("jdbc:mysql://inoovalab.com/inoovala_storm_test?user=inoovala_storm&password=Storm_Test");
			return connect;
		} catch (Exception e) {
			throw new RuntimeException("Error occure while get mysql connection : ");
		}
	}
}
