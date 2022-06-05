package com.kc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Recursion {

	static long MiddleValue = 0;
	static int size = 0;
	static int middleIndex = 0;

	public static void main(String[] args) throws Exception {

		//String propertiesFile = args[0];
		String hiveurl = args[0];
		String hiveusername = args[1];
		String hivekeytab = args[2];
		//String hivequery = args[3];
		String hiveexportfileloc = args[3];
		//String logrowcount = args[5];
		
		String hivequery = "select p.pseq, p.id,p.shelfon_category_seq,p.name from shelfon_category_processed as p order by p.pseq ASC";
		
		
		FileOutputStream outputStream = null;

		try {

			//Properties prop = getPropertyvalues(propertiesFile);

			//String ExportedFileLoc = prop.getProperty("hiveexportfileloc") + "exploded.tsv";
			String ExportedFileLoc = hiveexportfileloc + "exploded.tsv";

			outputStream = new FileOutputStream(ExportedFileLoc);
			List<CategoryTable> recordList = new ArrayList<CategoryTable>();
			List<CategoryTable> recordList_data = new ArrayList<CategoryTable>();

			/*int rowcount = Integer.parseInt(prop.getProperty("logrowcount"));
			recordList = Hive_Utils.getRecords(prop.getProperty("hiveurl"), prop.getProperty("hiveusername"),
					prop.getProperty("hivekeytab"), prop.getProperty("hivequery"));*/
			int rowcount = 10;
			recordList = Hive_Utils.getRecords(hiveurl, hiveusername, hivekeytab, hivequery);

			for (int i = 0; i < recordList.size(); i++) {
				recordList_data.add(recordList.get(i));
			}

			if (recordList == null || recordList.size() == 0) {
				System.out.println("No Records to Export");
				System.exit(0);
			}

			System.out.println("Initial records to be Exported are identified");

			if (recordList_data == null || recordList_data.size() == 0) {
				System.out.println("No Records to compare");
				System.exit(0);
			}

			CategoryTable record;
			String sText;
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Calendar cal = Calendar.getInstance();

			size = recordList.size();

			if (size > 0) {

				middleIndex = size / 2;
				MiddleValue = recordList.get(middleIndex).pseq;
			} else {
				System.exit(0);
			}

			System.out.println(dateFormat.format(cal.getTime()));

			for (int i = 0; i < recordList.size(); i++) {

				if (i % rowcount == 0) {
					DateFormat dateFormat1 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
					Calendar cal1 = Calendar.getInstance();
					System.out.println("Iteration: " + i / rowcount + " " + dateFormat1.format(cal1.getTime())); // print
																													// every
																													// millionth
																													// iteration
				}

				record = recordList.get(i);

				if (record.pseq == 0) {
					sText = record.category_seq + "\t" + record.name + "\t" + record.pseq + "\t" + record.category_seq
							+ "\t" + record.name + "\n";
					outputStream.write(sText.getBytes());
				}

				finddetails(recordList, record.category_seq, record.pseq, 1, record.name, " > " + record.name,
						" > " + record.pseq, record.pseq, outputStream, recordList_data, record, " > " + record.name,
						" > " + record.pseq);

			}
			outputStream.close();

			System.out.println(recordList.size());

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		// TODO Auto-generated method stub
		finally {
			if (outputStream != null)
				outputStream.close();
		}

	}

	public static int finddetails(List<CategoryTable> recordList, long category_seq, long pseq, int level, String id,
			String Path, String EPath, long ParentID, FileOutputStream outputStream,
			List<CategoryTable> recordList_data, CategoryTable record, String sPrevPath, String sPrevEPath)
			throws Exception {

		CategoryTable record_temp = null;
		String output;

		CategoryTable searchkey = new CategoryTable(category_seq, id, pseq, record.name);
		int index = Collections.binarySearch(recordList_data, searchkey, new RecordComp());

		if (index > 0) {

			Hive_Utils util = new Hive_Utils();
			int[] boundaries = util.GetIndexBoundaries(index, recordList_data, category_seq, index, index);

			sPrevPath = Path;
			sPrevEPath = EPath;

			for (int i = boundaries[0]; i <= boundaries[1]; i++) {

				record_temp = recordList_data.get(i);

				if (pseq != record_temp.pseq) {
					level = level + 1;
					Path = Path + " > " + record_temp.name;
					EPath = EPath + " > " + record_temp.pseq; // mycode

				}

				else {
					Path = sPrevPath + " > " + record_temp.name;
					EPath = sPrevEPath + " > " + record_temp.pseq; // mycode

				}

				pseq = record_temp.pseq;

				String[] filter_Path = EPath.split(" > ");
				String EPath3 = EPath.substring(7) + " > " + record_temp.category_seq;

				String Path2 = Path.substring(3);
				output = record_temp.category_seq + "\t" + record_temp.name + "\t" + record_temp.pseq + "\t" + EPath3
						+ "\t" + Path2 + "\n";

				if (filter_Path[1].equals("0")) {
					outputStream.write(output.getBytes());
				}

				finddetails(recordList, record_temp.category_seq, record_temp.pseq, level, record_temp.id, Path, EPath,
						ParentID, outputStream, recordList_data, record, sPrevPath, sPrevEPath);
			}

		}
		return 0;
	}

	private static Properties getPropertyvalues(String propertiesFile) throws FileNotFoundException {
		// String file = "hive.properties";
		InputStream inStream = new FileInputStream(propertiesFile);
		Properties prop = new Properties();
		try {
			prop.load(inStream);
			return prop;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				inStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public static int ReturnIndexColl(List<CategoryTable> tblrecordlist, CategoryTable record, int size,
			long Middlevalue, int MiddleIndex) {

		int low = 0;
		int high = size - 1;

		long midVal;
		int cmp;
		while (low <= high) {
			int mid = (low + high) >>> 1;

			midVal = tblrecordlist.get(mid).pseq;

			if (record.pseq == midVal)
				cmp = 0;
			else if (record.pseq > midVal)
				cmp = -1;
			else
				cmp = 1;

			if (cmp < 0)
				low = mid + 1;
			else if (cmp > 0)
				high = mid - 1;
			else
				return mid;
		}
		return -(low + 1);

	}
}
