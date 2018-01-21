package com.stormadvance.storm_example;

import java.sql.*;

import org.apache.storm.tuple.Tuple;
/**
 * This class contains logic to persist record into MySQL database.
 * 
 */
public class MySQLDump {
	/**
	 * Name of database you want to connect
	 */
	private String database;
	/**
	 * Name of MySQL user
	 */
	private String user;
	/**
	 * IP of MySQL server
	 */
	private String ip;
	/**
	 * Password of MySQL server
	 */
	private String password;
	Connection conn;
	private Statement stmt;
	
	public MySQLDump(String ip, String database, String user, String password) {
		final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

		this.ip = ip;
		this.database = database;
		this.user = user;
		this.password = password;
		final String DB_URL = "jdbc:mysql://inoovalab.com/inoovala_storm_test";
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {

			conn = conn = DriverManager.getConnection(DB_URL, user, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Get the MySQL connection
	 */

	private PreparedStatement preparedStatement = null;

	/**
	 * Persist input tuple.
	 * @param tuple
	 */
	public void persistRecord(Tuple tuple) {


			// preparedStatements can use variables and are more efficient

		//preparedStatement = connect.prepareStatement("insert into  apachelog values (?)");

			//preparedStatement.setString(1, tuple.getStringByField("site"));

			
			// Insert record
			String sql = "INSERT INTO Registration " +
					"VALUES (100, 'Zara', 'Ali', 18)";
		try {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}



	}
	
	public void close() {
		try {
		conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
}
