package com.tcl.move.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.tcl.move.dao.SubsCountDAO;

public class SubsCountLoader {

	private static Logger logger = null;

	SubsCountDAO dao = new SubsCountDAO();	
	
	public static void main(String[] args) {

		// Get the params from the command line - For production			
		String propertyFile = args[0]; // The path of the property files
		String logFile = args[1]; // The path of the log file

		// Param files hardcoded - For local		
		// String workSpacePath = "E:\\eclipse-workspace\\APISimCountLoader\\";
		// String propertyFile = workSpacePath + "MNAAS_Property_Files\\MNAAS_ShellScript.properties";
		// String logFile = workSpacePath + "MNAASCronLogs\\MoveSIMTable.log";

		System.setProperty("logfile.name", logFile);
		logger = Logger.getLogger(SubsCountLoader.class);
		
		logger.info("Batch started.");
				
		// Load the properties to System object for use in the application
		Properties properties = new Properties();
		InputStream inputStream;

		try {
			inputStream = new FileInputStream(propertyFile);
			properties.load(inputStream);
			inputStream.close();

			for (String name : properties.stringPropertyNames()) {
				String value = properties.getProperty(name);
				System.setProperty(name, value);
			}

			SubsCountLoader loader = new SubsCountLoader();
			loader.loadAPItables();	

			logger.info("Batch Completed at " + new Date());

		} catch (FileNotFoundException e) {
			logger.error("Property file path name is wrong: " + getStackTrace(e));
		} catch (IOException e) {
			logger.error("Property file could not be read: " + getStackTrace(e));
		}		
	}

	private void loadAPItables() {		

		try {
			String rawPartDate = dao.getLastPartFromRaw();
			String aggrPartDate = dao.getLastPartFromAggr();

			if (rawPartDate.equalsIgnoreCase(aggrPartDate)) {
				// All good. Aggr is loaded. Let's insert the other data.
				String monthPartDate = dao.getMonthPartDate();
				if (aggrPartDate.equalsIgnoreCase(monthPartDate)) {
					// All good. Its loaded and we have no issues
				} else {
					// Load the data
					dao.loadMonthTable();
					dao.loadYearTable();
				}
			} else {
				// Stop for now and try again later.
				logger.info("Latest Partition Date is not same in raw and aggr. Please load the aggr table");
			}
		} catch (Exception e) {
			logger.error("Exception while running batch - " + getStackTrace(e));	
		}
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
