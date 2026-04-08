package com.tcl.move.dao;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;

import com.tcl.move.exceptions.DatabaseException;
import com.tcl.move.utils.SubsCountUtils;


public class SubsCountDAO {

	private static final Logger logger = Logger.getLogger(SubsCountDAO.class);
	ResourceBundle daoBundle = ResourceBundle.getBundle("MOVEDAO");
	private JDBCConnection jdbcConn = new JDBCConnection();	
	
	public String getLastPartFromRaw() throws DatabaseException {

		String sql = daoBundle.getString("part.date.raw");
		// select max(partition_date) part_date from mnaas.move_sim_inventory_status
				
		String partDate = null;

		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			connection = jdbcConn.getConnectionHive();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery(sql);

			if (resultSet.next())
				partDate = resultSet.getString("part_date");

			logger.info("Partiton Date in raw table - " + partDate);
			
		} catch (Exception e) {
			logger.error("Exception in getLastPartFromRaw - " + getStackTrace(e));
			throw new DatabaseException(e.getMessage());

		} finally {
			try {
				if (null != statement) {
					statement.close();
				}
				if (null != connection) {
					connection.close();
				}
				if (null != resultSet) {
					resultSet.close();
				}

			} catch (Exception ex) {
				logger.error("Exception in getLastPartFromRaw - " + getStackTrace(ex));
				throw new DatabaseException(ex.getMessage());
			}
		}
		return partDate;
	}

	public String getLastPartFromAggr() throws DatabaseException {
		
		String sql = daoBundle.getString("part.date.aggr"); 
		// select max(partition_date) part_date from mnaas.move_sim_inventory_count
		
		String partDate = null;

		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			connection = jdbcConn.getConnectionHive();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery(sql);

			if (resultSet.next())
				partDate = resultSet.getString("part_date");
			
			logger.info("Partiton Date in raw table - " + partDate);
			
		} catch (Exception e) {
			logger.error("Exception in getLastPartFromAggr - " + getStackTrace(e));
			throw new DatabaseException(e.getMessage());

		} finally {
			try {
				if (null != statement) {
					statement.close();
				}
				if (null != connection) {
					connection.close();
				}
				if (null != resultSet) {
					resultSet.close();
				}

			} catch (Exception ex) {
				logger.error("Exception in getLastPartFromAggr - " + getStackTrace(ex));
				throw new DatabaseException(ex.getMessage());
			}
		}
		return partDate;
	}

	public String getMonthPartDate() throws DatabaseException {
				
		String sql = daoBundle.getString("part.date.month");
		// select max(status_date) status_date from mnaas.move_curr_month_count
		String partDate = null;

		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			connection = jdbcConn.getConnectionHive();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery(sql);

			if (resultSet.next())
				partDate = resultSet.getString("status_date");

			logger.info("Partiton Date in raw table - " + partDate);
			
		} catch (Exception e) {
			logger.error("Exception in getMonthPartDate - " + getStackTrace(e));
			throw new DatabaseException(e.getMessage());

		} finally {
			try {
				if (null != statement) {
					statement.close();
				}
				if (null != connection) {
					connection.close();
				}
				if (null != resultSet) {
					resultSet.close();
				}

			} catch (Exception ex) {
				logger.error("Exception in getMonthPartDate - " + getStackTrace(ex));
				throw new DatabaseException(ex.getMessage());
			}
		}
		return partDate;
	}

	public void loadMonthTable() throws DatabaseException {		
		
		String sql1 = daoBundle.getString("truncate.month");
		// truncate table mnaas.move_curr_month_count
		
		String sql2 = daoBundle.getString("insert.month.active");
		// insert into mnaas.move_curr_month_count partition (status_date) 
		// select buss_unit_id, prod_status, sum(count_sim), partition_date 
		// from mnaas.move_sim_inventory_count 
		// where partition_date between to_date(trunc(now(), 'month')) and to_date(date_sub(trunc(now(), 'dd'), 1))
		// and prod_status not in ('Preactive', 'SIMAVAILABLE')
		// group by prod_status, partition_date, buss_unit_id
		
		String sql3 = daoBundle.getString("insert.month.activity");
		// insert into mnaas.move_curr_month_count partition (status_date) 
		// select split_part(split_part(businessunittag, '|', (businessunitlevel*3)-1), '=', 2) buss_unit_id, 'Activity' prod_status, 
		// count(distinct sim), {0} 
		// from mnaas.traffic_details_raw_daily 
		// where partition_date between ? and ? 
		// group by buss_unit_id
		
		String sql4 = daoBundle.getString("insert.lastmonth.active");
		// insert into mnaas.move_curr_month_count partition (status_date) 
		// select buss_unit_id, prod_status, sum(count_sim), partition_date 
		// from mnaas.move_sim_inventory_count 
		// where partition_date between to_date(months_sub(trunc(now(), 'month'), 1)) and to_date(days_sub(trunc(now(), 'month'), 1))
		// and prod_status not in ('Preactive', 'SIMAVAILABLE')
		// group by prod_status, partition_date, buss_unit_id
		
		boolean first = SubsCountUtils.isFirstOfMonth();
		
		String monthStart = SubsCountUtils.getMonthStartDate();
		
		if (first)
			monthStart = SubsCountUtils.getPrevMonthStartDate();		
		
		List<String> monthDates = SubsCountUtils.getMonthDates(first);
		
		Connection connection = null;
		Statement statement = null;
		PreparedStatement pstatement = null;

		try {
			connection = jdbcConn.getConnectionHive();
			statement = connection.createStatement();
			statement.executeUpdate(sql1);
			logger.info("Month Table Truncated");
						
			if (first)
				statement.executeUpdate(sql4);
			else
				statement.executeUpdate(sql2);
			logger.info("Active, Suspended, Deactivated, etc SIM counts updated");
			
			for (String date : monthDates) {
				String fullQuery = MessageFormat.format(sql3, "'" + date + "'");
				logger.info("Executing " + fullQuery);
				pstatement = connection.prepareStatement(fullQuery);
				pstatement.setString(1, monthStart);
				pstatement.setString(2, date);
				pstatement.executeUpdate();
			}
			logger.info("Activity SIM counts updated");
			
		} catch (Exception e) {
			logger.error("Exception in loadMonthTable - " + getStackTrace(e));
			throw new DatabaseException(e.getMessage());

		} finally {
			try {
				if (null != statement) {
					statement.close();
				}
				if (null != connection) {
					connection.close();
				}				

			} catch (Exception ex) {
				logger.error("Exception in loadMonthTable - " + getStackTrace(ex));
				throw new DatabaseException(ex.getMessage());
			}
		}
	}	

	public void loadYearTable() throws DatabaseException {

		boolean first = SubsCountUtils.isFirstOfMonth();
		
		String monthStart = SubsCountUtils.getMonthStartDate();
		String statusDate = SubsCountUtils.getMonthCurrentDate();
		String month = SubsCountUtils.getMonthStartDate();
		if (first) {
			month = SubsCountUtils.getPrevMonthStartDate();
			monthStart = SubsCountUtils.getPrevMonthStartDate();
			statusDate = SubsCountUtils.getPrevMonthLastDate();	
		}
		month = month.substring(0, 7);
				
		
		String sql1 = daoBundle.getString("drop.part.year");
		// ALTER TABLE mnaas.move_curr_year_count DROP IF EXISTS PARTITION(status_month_year = {0})
				
		String sql2 = daoBundle.getString("insert.year.active");
		// insert into mnaas.move_curr_year_count partition (status_month_year)
		// select buss_unit_id, prod_status, sum(count_sim), {0} 
		// from mnaas.move_sim_inventory_count
		// where partition_date = ?
		// and prod_status not in ('Preactive', 'SIMAVAILABLE')
		// group by prod_status, buss_unit_id
		
		String sql3 = daoBundle.getString("insert.year.activity");
		// insert into mnaas.move_curr_year_count partition (status_month_year) 
		// select split_part(split_part(businessunittag, '|', (businessunitlevel*3)-1), '=', 2) buss_unit_id,
		// 'Activity' prod_status, count(distinct sim), {0} pdate
		// from mnaas.traffic_details_raw_daily
		// where partition_date between ? and ?
		// group by buss_unit_id
		
		String sql4 = daoBundle.getString("get.partiton.count");
		// select count(distinct status_month_year) count from mnaas.move_curr_year_count
		
		String sql5 = daoBundle.getString("get.min.part");
		// select min(status_month_year) status_date from mnaas.move_curr_year_count
		
		Connection connection = null;		
		PreparedStatement pstatement = null;
		ResultSet resultSet = null;
		// boolean retry = false;

		try {
			connection = jdbcConn.getConnectionHive();
			logger.info(MessageFormat.format(sql1, "'" + month + "'"));
			pstatement = connection.prepareStatement(MessageFormat.format(sql1, "'" + month + "'"));
			pstatement.executeUpdate();
			logger.info(month + " Year Table Partition dropped");			
			
			pstatement.close();
			logger.info(MessageFormat.format(sql2, "'" + month + "'"));
			pstatement = connection.prepareStatement(MessageFormat.format(sql2, "'" + month + "'"));
			pstatement.setString(1, statusDate);			
			pstatement.executeUpdate();
			logger.info("Active, Suspended, Deactivated, etc SIM counts updated - " + MessageFormat.format(sql2, "'" + month + "'") + " " + statusDate);
			
			pstatement.close();
			logger.info(MessageFormat.format(sql3, "'" + month + "'"));
			pstatement = connection.prepareStatement(MessageFormat.format(sql3, "'" + month + "'"));
			pstatement.setString(1, monthStart);
			pstatement.setString(2, statusDate);
			pstatement.executeUpdate();
			logger.info("Activity SIM counts updated - " + MessageFormat.format(sql3, "'" + month + "'") + " " + monthStart);			
			
			pstatement.close();			
			pstatement = connection.prepareStatement(sql4);			
			resultSet = pstatement.executeQuery();
			if (resultSet.next()) {
				int count = resultSet.getInt("count");
				if (count > 12) {
					// Remove the oldest poartition
					pstatement.close();
					resultSet.close();
					pstatement = connection.prepareStatement(sql5);	
					resultSet = pstatement.executeQuery();
					if (resultSet.next()) {
						String dropDate = resultSet.getString("status_date");
						pstatement.close();
						logger.info(MessageFormat.format(sql1, "'" + dropDate + "'"));
						pstatement = connection.prepareStatement(MessageFormat.format(sql1, "'" + dropDate + "'"));
						pstatement.executeUpdate();
						logger.info(dropDate + " Year Table Partition dropped");
					}
				}
			}			
			
		} catch (Exception e) {
			logger.error("Exception in loadYearTable - " + getStackTrace(e));
			throw new DatabaseException(e.getMessage());

		} finally {
			try {				
				if (null != pstatement) {
					pstatement.close();
				}
				if (null != connection) {
					connection.close();
				}	
				if (null != resultSet) {
					resultSet.close();
				}	

			} catch (Exception ex) {
				logger.error("Exception in loadYearTable - " + getStackTrace(ex));
				throw new DatabaseException(ex.getMessage());
			}
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
