package com.stormadvance.storm_example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

/**
 * This Bolt call the getConnectionn(....) method of MySQLDump class to persist
 * the record into MySQL database.
 * 
 * @author Admin
 * 
 */
public class PersistenceBolt implements IBasicBolt {

	private MySQLDump mySQLDump = null;
	private static final long serialVersionUID = 1L;
	/**
	 * Name of database you want to connect
	 */
	/*private String database;
	*//**
	 * Name of MySQL user
	 *//*
	private String user;
	*//**
	 * IP of MySQL server
	 *//*
	private String ip;
	*//**
	 * Password of MySQL server
	 *//*
	private String password;*/

	/*public PersistenceBolt() {
		this.ip = ip;
		this.database = database;
		this.user = user;
		this.password = password;
	}*/
	final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	final String DB_URL = "jdbc:mysql://inoovalab.com/inoovala_storm_test";

	//  Database credentials
	final String USER = "inoovala_storm";
	final String PASS = "Storm_Test";
	Connection conn = null;
	Statement stmt = null;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			try {
				conn = DriverManager.getConnection(DB_URL, USER, PASS);
				stmt = conn.createStatement();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		// create the instance of MySQLDump(....) class.

	}

	/**
	 * This method call the persistRecord(input) method of MySQLDump class to
	 * persist record into MySQL.
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
		//System.out.println("Input tuple : " + input);
		//mySQLDump.persistRecord(input);
		String sql = "INSERT INTO Registration VALUES (100, 'Zara', 'Ali', 18)";
		try {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void cleanup() {
		// Close the connection
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
