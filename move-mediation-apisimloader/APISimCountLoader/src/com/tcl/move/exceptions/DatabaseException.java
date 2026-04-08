package com.tcl.move.exceptions;

/**
 * A custom exception class to handle the exceptions thrown from the database.
 * 
 * @author Tata Consultancy Services
 * 
 */
public class DatabaseException extends Exception {

	private static final long serialVersionUID = 6228590916239837910L;

	/**
	 * Constructor for the exception class
	 * 
	 * @param msg
	 *            The error message
	 */
	public DatabaseException(String msg) {
		super(msg);
	}
}
