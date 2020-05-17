
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap.KeySetView;

public class DBApp {

	static ArrayList<Table> tables = new ArrayList<Table>();
	static int pageSize = 200;

	@SuppressWarnings("unchecked")
	public static void init() {
		try {

			FileInputStream fileIn = new FileInputStream("tables.bin");
			ObjectInputStream objectIn = new ObjectInputStream(fileIn);
			tables = (ArrayList<Table>) objectIn.readObject();
			objectIn.close();
			System.out.println("The tables array.bin  was succesfully read from the file in init method");

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	public static boolean isValidColType(String strTableName, Hashtable<String, Object> htblColNameValue) {
		try (BufferedReader br = new BufferedReader(new FileReader("metadata.csv"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				String tableName = values[0];
				String colName = values[1];
				String colType = values[2];
				if (tableName.equalsIgnoreCase(strTableName)) {
					if (htblColNameValue.containsKey(colName)) {
						Object colValue = htblColNameValue.get(colName);
						if (!(colValue.getClass().getName().equalsIgnoreCase(colType))) {
							// Class.forName(htblColNameType.get(colName)).getName()
							return false;
						}
					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	public static int getPageSize() {
		return pageSize;
	}

	public static void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException, ClassNotFoundException {
		Set<String> columnsKeySet = null;
		try (FileWriter writer = new FileWriter(new File("metadata.csv"), true)) {

			StringBuilder sb = new StringBuilder();
			columnsKeySet = htblColNameType.keySet();
			Iterator<String> iter = columnsKeySet.iterator();
			for (int i = 0; i < columnsKeySet.size(); i++) {
				String colName = iter.next();
				sb.append(strTableName + ",");
				sb.append(colName + ",");
				sb.append(Class.forName(htblColNameType.get(colName)).getName() + ",");
				if (colName.equals(strClusteringKeyColumn))
					sb.append(true + ",");

				else
					sb.append(false + ",");

				sb.append(true);
				sb.append('\n');
			}

			writer.write(sb.toString());

			System.out.println("done appending csv metadata file in create table method");

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		try {
			new File("Tables/" + strTableName).mkdirs();

		} catch (Exception e) {
			e.printStackTrace();
		}

		Table newTable = new Table(strTableName, strClusteringKeyColumn);
		newTable.setNumberOfColumns(columnsKeySet.size());

		newTable.setNameOfColumns(columnsKeySet.toArray());
		try {
			tables.add(newTable);
			File file = new File("tables.bin");
			file.createNewFile();
			FileOutputStream fileOut = new FileOutputStream("tables.bin");
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
			objectOut.writeObject(tables);
			objectOut.close();
			System.out.println(
					"The tables array   was succesfully written to the tables file in the create table method ");

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	public static void createBitmapIndex(String strTableName, String strColName) throws DBAppException {
		Table y = null;
		for (Table x : tables) {
			if (x.getName().equals(strTableName)) {
				y = x;
				break;
			}
		}
		if (y.getIndexedColumns().contains(strColName)) {
			System.out.println("An index already exists on this column");
			return;
		}
		y.createBitmap(strColName);
		System.out.println("index was created successfuly");
	}

	public static void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		Table y = null;
		for (Table x : tables) {
			if (x.getName().equals(strTableName)) {
				y = x;
				break;
			}
		}
		if (!isValidColType(strTableName, htblColNameValue))
			throw new DBAppException("Invalid type entered");
		Date date = new Date(); // this object contains the current date value
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		htblColNameValue.put("TouchDate", formatter.format(date));
		y.addTuple(htblColNameValue);
		System.out.println("the records are inserted in insertintoTable method");
	}

	public static void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		Table y = null;
		for (Table x : tables) {
			if (x.getName().equals(strTableName)) {
				y = x;
				break;
			}
		}

		if (!isValidColType(strTableName, htblColNameValue))
			throw new DBAppException("Invalid type entered");

		Date date = new Date(); // this object contains the current date value
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		htblColNameValue.put("TouchDate", formatter.format(date));
		y.updateTuple(strKey, htblColNameValue);
		System.out.println("the records are updated in updateTable method");
	}

	public static void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		Table y = null;
		for (Table x : tables) {
			if (x.getName().equals(strTableName)) {
				y = x;
				break;
			}
		}

		if (!isValidColType(strTableName, htblColNameValue))
			throw new DBAppException("Invalid type entered");

		y.deleteTuple(htblColNameValue);
		System.out.println("the records are deleted in deleteTable method");
	}

	public static Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		ArrayList<Vector<Hashtable<String, Object>>> resultSets = new ArrayList<Vector<Hashtable<String, Object>>>(
				arrSQLTerms.length);

		for (SQLTerm term : arrSQLTerms) {
			String tableName = term.get_strTableName();
			for (Table table : tables) {
				if (table.getName().equals(tableName)) {
					Vector<Hashtable<String, Object>> records = null;
					if (table.getIndexedColumns().contains(term.get_strColumnName())) {
						 records = table.getRecordsQueryIndexed(term);
					} else {
						records = table.getRecordsQuery(term);
					}

					resultSets.add(records);
					break;
				}
			}

		}
		Vector<Hashtable<String, Object>> resultSet=evaluateTermsWithOperators(resultSets, strarrOperators);

		return resultSet.iterator();
	}

	public static Vector<Hashtable<String, Object>> evaluateTermsWithOperators(
			ArrayList<Vector<Hashtable<String, Object>>> resultSets, String[] strarrOperators) {
		
		ArrayList<String> strarrOps=new ArrayList<String>();
		for(String operator:strarrOperators) {
			strarrOps.add(operator);
		}
		
		while(!strarrOps.isEmpty()) {
			while(true) {
				int index=strarrOps.indexOf("AND");
				if(!strarrOps.remove("AND"))
					break;
				Vector<Hashtable<String, Object>> set1=resultSets.remove(index);
				Vector<Hashtable<String, Object>> set2=resultSets.remove(index+1);
				resultSets.add(index, AND(set1, set2));
			}
			while(true) {
				int index=strarrOps.indexOf("XOR");
				if(!strarrOps.remove("XOR"))
					break;
				Vector<Hashtable<String, Object>> set1=resultSets.remove(index);
				Vector<Hashtable<String, Object>> set2=resultSets.remove(index+1);
				resultSets.add(index, XOR(set1, set2));
			}
			while(true) {
				int index=strarrOps.indexOf("OR");
				if(!strarrOps.remove("OR"))
					break;
				Vector<Hashtable<String, Object>> set1=resultSets.remove(index);
				Vector<Hashtable<String, Object>> set2=resultSets.remove(index+1);
				resultSets.add(index, OR(set1, set2));
			}
		}
		
		return resultSets.get(0);
	}

	public static Vector<Hashtable<String, Object>> AND(Vector<Hashtable<String, Object>> recordsSet1,
			Vector<Hashtable<String, Object>> recordsSet2) {
		Vector<Hashtable<String, Object>> resultSet = new Vector<Hashtable<String, Object>>();
		for (Hashtable<String, Object> ht : recordsSet1) {
			if (recordsSet2.contains(ht)) {
				resultSet.add(ht);
			}
		}
		return resultSet;
	}

	public static Vector<Hashtable<String, Object>> OR(Vector<Hashtable<String, Object>> recordsSet1,
			Vector<Hashtable<String, Object>> recordsSet2) {
		Vector<Hashtable<String, Object>> resultSet = new Vector<Hashtable<String, Object>>();
		for (Hashtable<String, Object> ht : recordsSet1) {
			resultSet.add(ht);
		}
		for (Hashtable<String, Object> ht : recordsSet2) {
			if (!resultSet.contains(ht)) {
				resultSet.add(ht);
			}
		}
		return resultSet;
	}

	public static Vector<Hashtable<String, Object>> XOR(Vector<Hashtable<String, Object>> recordsSet1,
			Vector<Hashtable<String, Object>> recordsSet2) {
		Vector<Hashtable<String, Object>> resultSet = new Vector<Hashtable<String, Object>>();
		for (Hashtable<String, Object> ht : recordsSet1) {
			if (!recordsSet2.contains(ht)) {
				resultSet.add(ht);
			}
		}
		for (Hashtable<String, Object> ht : recordsSet2) {
			if (!recordsSet1.contains(ht)) {
				resultSet.add(ht);
			}
		}
		return resultSet;
	}

	public static void main(String[] args) throws ClassNotFoundException, DBAppException, IOException {
		String strTableName = "Student";
		init();
//		 Hashtable<String,String> htblColNameType = new Hashtable<String,String>( );
//		 htblColNameType.put("id", "java.lang.Integer");
//		 htblColNameType.put("name", "java.lang.String");
//		 htblColNameType.put("gpa", "java.lang.Double");
//		 createTable( strTableName, "id", htblColNameType );
//

		// insertion
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		htblColNameValue.put("id", new Integer(232));
		htblColNameValue.put("name", new String("momo soso"));
		htblColNameValue.put("gpa", new Double(0.66));
		insertIntoTable(strTableName, htblColNameValue);

		for (Table t : tables) {
			if (t.getName().equalsIgnoreCase(strTableName)) {
				t.getRecords();

			}
		}

	}

}
