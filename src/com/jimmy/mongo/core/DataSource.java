package com.jimmy.mongo.core;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;

import com.mongodb.DB;
import com.mongodb.Mongo;

public class DataSource {

	public static DB db;

	public static String database = "da";

	public static String host = "localhost";

	public static int port = 27017;

	private static DataSource instance = null;

	private DataSource() {
		config();
	}

	private void config() {
		
		System.out.println("Datasource Config!");
		Properties prop = new Properties();

		try {
			// load a properties file
			prop.load(new FileInputStream("conf/mongo.properties"));
			System.out.println("load mongo.properties:"+prop.propertyNames());
			setHost(prop.getProperty("url"));
			setPort(Integer.parseInt(""+prop.getProperty("port")));
			setDatabase(prop.getProperty("dbName"));

		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}

	static public DataSource getInstance() {
		if (instance == null) {
			instance = new DataSource();
		}
		return instance;
	}

	public DB getConnect() throws UnknownHostException {
		Mongo m = new Mongo(getHost(), getPort());
		return m.getDB(getDatabase());
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

}
