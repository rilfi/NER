package com.stormadvance.storm_example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 
 * This class return the MySQL connection.
 */
public class MySQLConnection {

	private static Connection connect = null;

	/**
	 * This method return the MySQL connection.
	 *
	 * @param ip
	 *            ip of MySQL server
	 * @param database
	 *            name of database
	 * @param user
	 *            name of user
	 * @param password
	 *            password of given user
	 * @return MySQL connection
	 */
	public static Connection getMySQLConnection(String ip, String database, String user, String password) {

			// this will load the MySQL driver, each DB has its own driver
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		final String DB_URL = "jdbc:mysql://"+ip+"/"+database;
		// setup the connection with the DB.
		try {
			connect =DriverManager.getConnection(DB_URL, user, password);;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connect;

	}
}
