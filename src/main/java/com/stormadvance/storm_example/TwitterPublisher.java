package com.stormadvance.storm_example;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * This Bolt call the getConnectionn(....) method of MySQLDump class to persist
 * the record into MySQL database.
 * 
 * @author Admin
 * 
 */
public class TwitterPublisher implements IBasicBolt {

	private MySQLDump mySQLDump = null;
	private static final long serialVersionUID = 1L;
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

	public TwitterPublisher(String ip, String database, String user,
                            String password) {
		this.ip = ip;
		this.database = database;
		this.user = user;
		this.password = password;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {

		// create the instance of MySQLDump(....) class.
		mySQLDump = new MySQLDump(ip, database, user, password);
	}

	/**
	 * This method call the persistRecord(input) method of MySQLDump class to
	 * persist record into MySQL.
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("Input tuple : " + input);
		mySQLDump.persistRecord(input);
	}

	public void cleanup() {
		// Close the connection
		mySQLDump.close();
	}

}
