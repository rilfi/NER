package com.stormadvance.storm_example;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

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
	
	public MySQLDump(String ip, String database, String user, String password) {
		this.ip = ip;
		this.database = database;
		this.user = user;
		this.password = password;
	}
	
	/**
	 * Get the MySQL connection
	 */
	private Connection connect = MySQLConnection.getMySQLConnection(ip,database,user,password);

	private PreparedStatement preparedStatement = null;
	Statement stmt = null;
	
	/**
	 * Persist input tuple.
	 * @param tuple
	 */
	public void persistRecord(Tuple tuple) {


			// preparedStatements can use variables and are more efficient
		try {
			stmt = connect.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
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

		finally {
			// close prepared statement
			if (stmt != null) {

				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}

			}
		}

	}
	
	public void close() {
		try {
		connect.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
}
