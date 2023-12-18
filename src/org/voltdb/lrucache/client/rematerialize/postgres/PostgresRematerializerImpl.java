package org.voltdb.lrucache.client.rematerialize.postgres;


import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import java.util.Properties;

import org.voltdb.lrucache.client.rematerialize.AbstractSqlRematerializer;
import org.voltdb.lrucache.client.rematerialize.LRUCacheRematerializer;
import org.voltdb.seutils.log.LogInterface;
import org.voltdb.seutils.log.VoltLog;
import org.voltdb.seutils.utils.CSException;
import org.voltdb.seutils.wranglers.postgres.PostgresConnectionWrangler;

public class PostgresRematerializerImpl extends AbstractSqlRematerializer implements LRUCacheRematerializer {


	/**
	 * Used to manage connectivity to Oracle
	 */
	PostgresConnectionWrangler m_ocw = null;

	/**
	 * Set if something goes horribly wrong
	 */
	boolean m_isBroken = false;

	/**
	 * SQL statement we query database with
	 */
	String m_sqlString = null;

	/**
	 * Prepared statements holding our SQL
	 */
	PreparedStatement m_ps;

	/**
	 * Log Instance
	 */
	VoltLog m_Log = new VoltLog(logger);

	/**
	 * Non-argument constructor. Arguments are passed in via 'setConfig'
	 */
	public PostgresRematerializerImpl() {
	}

	/**
	 * @param schemaName
	 * @param tableName
	 * @param config
	 */
	public void setConfig(String schemaName, String tableName, Properties config) {

		super.setConfig(schemaName, tableName, config);

		String sid = config.getProperty(DATABASE_SID);

		try {
			
			m_ocw = new PostgresConnectionWrangler(m_otherDbHostnames, m_otherDbPort, sid, m_otherDbUsername,
					m_otherDbPassword, m_Log);

			m_ocw.confirmConnected();

			m_sqlString = getSelectSql(POSTGRES);

			m_isBroken = false;

		} catch (CSException e) {

			m_isBroken = true;
			logger.error(e.getMessage());

		}

	}

	/**
	 * Implementing classes use this method to find 'missing' data.
	 * 
	 * @param pkArgs
	 *            The Primary Key fields
	 * @param m_pkArgCount
	 *            How any of the PK fields are actually the PK
	 * @return Object[] of values in VoltDB column order, or null.
	 * @throws Exception
	 */
	@Override
	public synchronized Object[] fetch(Object[] pkArgs, int pkArgCount) throws Exception {

		final long start = System.currentTimeMillis();
		Object[] data = null;

		try {

			// make sure we are connected to Oracle; this
			// will throw an exception if we aren't
			m_ocw.confirmConnected();

			if (m_ps == null) {
				m_ps = m_ocw.m_connection.prepareStatement(m_sqlString);
			}

			for (int i = 0; i < pkArgs.length && i < pkArgCount; i++) {
				String pkColName = getPkCol((i + 1));
				SqlUtils.bindPostgresParamWithVoltDatatype(m_ps, (i + 1), pkArgs[i], getColDatatype(pkColName));
			}

			ResultSet rs = m_ps.executeQuery();
			m_statsInstance.reportLatency(ORACLE + "_query_ms", start, "", 100);

			data = mapOracleRowToVoltObjectArray(rs, m_Log);

			rs.close();

		} catch (Exception e) {

			m_isBroken = true;
			logStackTrace("PostgresRematerializerImpl.fetch", logger, e);			
			disconnect();
		}

		return data;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.voltdb.lrucache.client.rematerialize.AbstractSqlRematerializer#
	 * disconnect()
	 */
	@Override
	public void disconnect() {

		super.disconnect();

		if (m_ps != null) {
			try {
				m_ps.close();
				m_ps = null;
			} catch (SQLException e) {
				m_ps = null;
				m_Log.warning("Got this while trying to disconnect: " + e.getMessage());
			}
		}

		if (m_ocw.haveConnection()) {
			m_ocw.disconnect();
		}
	}

	/**
	 * Map an oracle row to a VoltDB Object[]
	 * 
	 * @param rs
	 *            An oracle Result Set
	 * @param theLog
	 * @return Object[] mapping of the row.
	 */
	private Object[] mapOracleRowToVoltObjectArray(ResultSet rs, LogInterface theLog) {

		Object[] data = null;

		ResultSetMetaData rsmd;
		try {

			rsmd = rs.getMetaData();

			if (rs.next()) {

				data = new Object[rsmd.getColumnCount()];

				for (int i = 0; i < data.length; i++) {

					data[i] = rs.getObject((i + 1));

					String voltDataType = getColDatatype(i + 1);

					if (data[i] instanceof oracle.sql.TIMESTAMP) {
						oracle.sql.TIMESTAMP ost = (oracle.sql.TIMESTAMP) data[i];
						data[i] = ost.timestampValue();
					} else if (data[i] instanceof BigDecimal) {

						BigDecimal bd = (BigDecimal) data[i];

						if (voltDataType.equals("TINYINT")) {
							data[i] = new Byte(bd.byteValue());
						} else if (voltDataType.equals("SMALLINT")) {
							data[i] = new Short(bd.shortValue());
						} else if (voltDataType.equals("INTEGER")) {
							data[i] = new Integer(bd.intValue());
						} else if (voltDataType.equals("BIGINT")) {
							data[i] = new Long(bd.longValue());
						} else if (voltDataType.equals("DECIMAL")) {
							// It's already a BigDecimal...
						}
					}
				}
			}

		} catch (SQLException e) {			
			
			logStackTrace("PostgresRematerializerImpl.mapOracleRowToVoltObjectArray", logger, e);			
			m_isBroken = true;
			disconnect();

			
		}

		return data;
	}

}
