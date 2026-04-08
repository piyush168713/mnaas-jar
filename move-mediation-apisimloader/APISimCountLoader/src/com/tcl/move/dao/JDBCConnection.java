package com.tcl.move.dao;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.log4j.Logger;

import com.tcl.move.exceptions.DatabaseException;

/**
 * Class to handle DB connections
 * 
 * @author Tata Consultancy Services
 * 
 */
public class JDBCConnection {

	private static Logger logger = Logger.getLogger(JDBCConnection.class);

	/**
	 * Creates a connection to Hadoop DB
	 * 
	 * @return The Oracle connection to Hadoop
	 * @throws DBConnectionException
	 *             In case of an error during connection
	 */
	public Connection getConnectionHive() throws DatabaseException {

		Connection connection = null;

		String serverName = System.getProperty("IMPALAD_HOST");
		String portNumber = System.getProperty("IMPALAD_JDBC_PORT");
		String driverName = "com.cloudera.impala.jdbc41.Driver";

		try {			
			Class.forName(driverName);
			connection = DriverManager.getConnection("jdbc:impala://"
					+ serverName + ":"
					+ portNumber + ";auth=noSasl;SocketTimeout=0");
			logger.info("Hive Database Connection established " + serverName);

		} catch (Exception ex) {
			logger.error("Exception Occurred while creating a connection " + getStackTrace(ex));
			throw new DatabaseException(ex.getMessage());
		}
		return connection;
	}
	
	/**
	 * Fetches and returns the stack trace associated with the exception
	 * 
	 * @param exception
	 *            The exception
	 * @return The stack trace
	 */
	private static String getStackTrace(Exception exception) {

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		exception.printStackTrace(pw);
		return sw.toString(); // stack trace as a string
	}
}