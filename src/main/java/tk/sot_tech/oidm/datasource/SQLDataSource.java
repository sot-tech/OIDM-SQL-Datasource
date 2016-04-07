/*
 * Copyright (c) 2016, eramde
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package tk.sot_tech.oidm.datasource;

import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Logger;
import javax.xml.bind.DatatypeConverter;
import oracle.iam.platform.Platform;
import tk.sot_tech.oidm.sch.AbstractDataSource;
import static tk.sot_tech.oidm.utility.Misc.isNullOrEmpty;
import static tk.sot_tech.oidm.utility.Misc.ownStack;
import static tk.sot_tech.oidm.utility.Misc.registerJDBC;
import tk.sot_tech.oidm.utility.Pair;

/**
 * Example properties file:
 * request.select=select col0, col1, col2 from table where changed &gt; ?
 * <br>
 * request.select.key=col0
 * <br>
 * request.select.multival.0=select mulcol0, keycol from multable
 * <br>
 * request.select.multival.0.name=MULTIVAL0
 * <br>
 * request.select.multival.0.key=keycol
 * <br>
 * request.delete=delete from table where delcol0 = ? and delcol1 = ? and changed = ?
 * <br>
 * request.delete.0=col0:VARCHAR
 * <br>
 * request.delete.1=col1:VARCHAR
 * <br>
 * request.delete.2=changed:TIMESTAMP
 * <br>
 * <br>
 */
public class SQLDataSource extends AbstractDataSource {

	private static final ResourceBundle BUNDLE = ResourceBundle.getBundle("resources/sqlrecon");
	public static final String RES_DRIVER = BUNDLE.getString("itresource.driver"),
			RES_URL = BUNDLE.getString("itresource.url"),
			RES_USER = BUNDLE.getString("itresource.user"),
			RES_PASSWORD = BUNDLE.getString("itresource.password");

	private Connection source = null;
	private String reqQuery = "", reqQueryKey = "", delQuery = "";
	private final ArrayList<String> delQueryKeys = new ArrayList<>();
	private final HashMap<String, Pair<String, String>> multivalList = new HashMap<>();

	@Override
	public void close() throws Exception {
		if (source != null && !source.isClosed()) {
			source.close();
		}
	}

	@Override
	public ArrayList<HashMap<String, Object>> fetchData() throws SQLException {
		ArrayList<HashMap<String, Object>> values = new ArrayList<>();
		HashMap<Object, HashMap<String, ArrayList<HashMap<String, Object>>>> multivalCache;
		try {
			multivalCache = getMultivaluedData(source);
		}
		catch (tcAPIException | tcColumnNotFoundException | SQLException ex) {
			Logger.getLogger(SQLDataSource.class.getName()).severe(ownStack(ex));
			throw new SQLException(ex);
		}
		try(PreparedStatement sourceStatement = source.prepareStatement(reqQuery)) {
			if (parameters.getFromDate() != null && reqQuery.contains("?")) {
				sourceStatement.setTimestamp(1, new Timestamp(parameters.getFromDate().getTime()));
			}
			if (sourceStatement.execute()) {
				try (ResultSet resultSet = sourceStatement.getResultSet()) {
					if (resultSet != null) {
						ResultSetMetaData metaData = resultSet.getMetaData();
						while (resultSet.next()) {
							HashMap<String, Object> record = new HashMap<>();
							for (int i = 1; i <= metaData.getColumnCount(); ++i) {
								String name = metaData.getColumnName(i);
								Object data = resultSet.getObject(i);
								if (data != null && data instanceof byte[]) {
									data = DatatypeConverter.printBase64Binary((byte[]) data);
								}
								if (reqQueryKey.equalsIgnoreCase(name)) {
									if (multivalCache != null) {
										record.put(MULTIVALUED_KEY, multivalCache.get(data));
									}
								}
								record.put(name, data);
							}
							values.add(record);
						}
					}
				}
			}
			else {
				throw new IllegalStateException("Unable to execute query " + reqQuery);
			}
		}
		return values;
	}

	/*
	 * USER_ID 1..N	TABLENAME 1..N	FIELD 1..1	DATA
	 */
	private HashMap<Object, HashMap<String, ArrayList<HashMap<String, Object>>>> getMultivaluedData(Connection source) throws SQLException, tcAPIException, tcColumnNotFoundException {
		if (multivalList.isEmpty()) {
			return null;
		}
		HashMap<Object, HashMap<String, ArrayList<HashMap<String, Object>>>> fullData = new HashMap<>();
		//Multival TABLE NAMES
		for (String s : multivalList.keySet()) {
			//<select, selectKey>
			Pair<String, String> current = multivalList.get(s);
			ArrayList<HashMap<String, Object>> rows = new ArrayList<>();
			try(PreparedStatement statement = source.prepareCall(current.key)) {
				if (statement.execute()) {
					try (ResultSet resultSet = statement.getResultSet()) {
						if (resultSet != null) {
							ResultSetMetaData metaData = resultSet.getMetaData();
							while (resultSet.next()) {
								HashMap<String, Object> row = new HashMap<>();
								for (int i = 1; i <= metaData.getColumnCount(); ++i) {
									String name = metaData.getColumnName(i);
									Object data = resultSet.getObject(i);
									if (data != null && data instanceof byte[]) {
										data = DatatypeConverter.printBase64Binary((byte[]) data);
									}
									row.put(name, data);
								}
								rows.add(row);
							}
						}
					}
				}
				else {
					throw new IllegalStateException("Unable to execute query " + reqQuery);
				}
			}
			for (HashMap<String, Object> row : rows) {
				Object key = row.remove(current.value);
				HashMap<String, ArrayList<HashMap<String, Object>>> userTables = fullData.get(key);
				if (userTables == null) {
					userTables = new HashMap<>();
				}
				ArrayList<HashMap<String, Object>> userTableRows = userTables.get(s);
				if (userTableRows == null) {
					userTableRows = new ArrayList<>();
				}
				checkAndAppendITResourceKey(row);
				userTableRows.add(row);
				userTables.put(s, userTableRows);
				fullData.put(key, userTables);
			}
		}
		return fullData;
	}

	@Override
	public void clearData(ArrayList<HashMap<String, Object>> values) throws SQLException {
		if (isNullOrEmpty(delQuery)) {
			return;
		}
		try(PreparedStatement sourceStatement = source.prepareStatement(delQuery)) {
			for (HashMap<String, ? extends Object> value : values) {
				int i = 1;
				for (String key : delQueryKeys) {
					String[] keyType = key.split(":");
					if (keyType == null || keyType.length < 2) {
						throw new IllegalArgumentException("Invalid delete query configuration");
					}
					try {
						Field field = Types.class.getDeclaredField(keyType[1].toUpperCase());
						field.setAccessible(true);
						int sqlType = field.getInt(null);
						sourceStatement.setObject(i++, value.get(keyType[0]), sqlType);
					}
					catch (IllegalAccessException | IllegalArgumentException | 
							NoSuchFieldException | SecurityException | SQLException ex) {
						Logger.getLogger(SQLDataSource.class.getName()).severe(ownStack(ex));
					}
				}
				sourceStatement.execute();
				sourceStatement.clearParameters();
			}
		}
	}

	@Override
	protected AbstractDataSource initImpl() {
		String url, user, pwd, driver;
		if (parameters.getItParameters().isEmpty()) {
			try {
				source = Platform.getOperationalDS().getConnection();
			}
			catch (SQLException ex) {
				Logger.getLogger(SQLDataSource.class.getName()).severe(ownStack(ex));
			}
		}
		else {
			url = parameters.getItParameters().get(RES_URL);
			user = parameters.getItParameters().get(RES_USER);
			pwd = parameters.getItParameters().get(RES_PASSWORD);
			driver = parameters.getItParameters().get(RES_DRIVER);
			try {
				registerJDBC(driver);
				source = DriverManager.getConnection(url, user, pwd);
			}
			catch (ClassNotFoundException | IllegalAccessException | InstantiationException | SQLException ex) {
				Logger.getLogger(SQLDataSource.class.getName()).severe(ownStack(ex));
			}
		}
		reqQuery = parameters.getResourceBundle().getString(BUNDLE.getString("param.sql.selectparam"));
		String tmp;
		if (parameters.getResourceBundle().containsKey(tmp = BUNDLE.getString("param.sql.selectparam.key"))) {
			reqQueryKey = parameters.getResourceBundle().getString(tmp);
		}
		if (parameters.getResourceBundle().containsKey(tmp = BUNDLE.getString("param.sql.deleteparam"))) {
			delQuery = parameters.getResourceBundle().getString(tmp);
			int i = 0;
			while (parameters.getResourceBundle().containsKey(tmp = BUNDLE.getString("param.sql.deleteparam") + '.' + i)) {
				delQueryKeys.add(parameters.getResourceBundle().getString(tmp));
				++i;
			}
		}
		int i = 0;
		while (parameters.getResourceBundle().containsKey(tmp = BUNDLE.getString("param.sql.multivalparam") + '.' + i)) {
			multivalList.put(parameters.getResourceBundle().getString(tmp + '.' + "name"),
							 new Pair(parameters.getResourceBundle().getString(tmp).toUpperCase(),
									  parameters.getResourceBundle().getString(tmp + '.' + "key").toUpperCase()));
			++i;
		}
		return this;
	}

}
