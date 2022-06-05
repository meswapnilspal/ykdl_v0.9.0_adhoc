package com.kc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
//import java.sql.Statement;

import org.apache.hadoop.security.UserGroupInformation;

import java.util.ArrayList;
import java.util.List;

public class Hive_Utils {

	/**
	 * To get all Existing file names from the hive table
	 * 
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */

	public int[] GetIndexBoundaries(int FoundIndex, List<CategoryTable> recordList, long category_seq, int i, int j) {

		int lower = FoundIndex;
		int high = FoundIndex;

		int[] boundaries = new int[2];

		for (int k = FoundIndex; k >= 0; k--) {

			if (recordList.get(k).pseq == category_seq) {
				lower = k;
			} else
				break;

		}

		for (int k = FoundIndex; k <= recordList.size() - 1; k++) {

			if (recordList.get(k).pseq == category_seq) {
				high = k;
			} else
				break;

		}

		boundaries[0] = lower;
		boundaries[1] = high;
		return boundaries;

	}

	/**
	 * To get all Existing file names from the hive table
	 * 
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public static List<CategoryTable> getRecords(String hiveUrl, String user, String keytab, String hiveQuery)
			throws SQLException {

		List<CategoryTable> recordList = new ArrayList<CategoryTable>();

		try {
			System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
			System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
			conf.set("hadoop.security.authentication", "Kerberos");
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(user, keytab);

			String HIVE_CONNECTION_URL = hiveUrl;
			String STATEMENT = hiveQuery;

			Connection Hivecon = null;

			PreparedStatement ps;

			Class.forName("org.apache.hive.jdbc.HiveDriver");

			Hivecon = DriverManager.getConnection(HIVE_CONNECTION_URL);

			ps = Hivecon.prepareStatement(STATEMENT);

			// Execute Query
			ResultSet res = ps.executeQuery();

			while (res.next()) {

				recordList.add(new CategoryTable(res.getLong("p.pseq"), res.getString("p.id"),
						res.getLong("p.shelfon_category_seq"), res.getString("p.name")));

			}

			ps.close();
			Hivecon.close();

		} catch (SQLException s) {
			System.out.print("Inside Exception" + s);
			s.printStackTrace();

		}

		catch (Exception e) {

			e.printStackTrace();
		}
		return recordList;

	}
}
