import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

public class Table implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1857740281668563826L;

	// private Hashtable<Integer,Page> pages=null;
	private String name;
	private int currentPage;
	private int numberOfRows;
	private int numberOfColumns;
	private String[] nameOfColumns;
	private String strClusteringKeyColumn;
	private ArrayList<String> indexedColumns;
	// private Hashtable<String, BitmapIndex> bitMap;

	public String[] getNameOfColumns() {
		return nameOfColumns;
	}

	public void setNameOfColumns(Object[] nameOfColumns) {
		String[] y = new String[nameOfColumns.length];
		for (int i = 0; i < nameOfColumns.length; i++) {
			y[i] = (String) nameOfColumns[i];
		}

		this.nameOfColumns = y;
	}

	public int getNumberOfColumns() {
		return numberOfColumns;
	}

	public void setNumberOfColumns(int numberOfColumns) {
		this.numberOfColumns = numberOfColumns;
	}

	public Table(String name, String strClusteringKeyColumn) {
		this.name = name;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		currentPage = 1;
		new Page(name, currentPage, strClusteringKeyColumn);
		numberOfRows = 0;
		// this.bitMap = new Hashtable<String, BitmapIndex>();
		this.indexedColumns = new ArrayList<String>();
	}

	public Vector getRecordsVector() {
		Vector returnMe = null;
		for (int i = 1; i <= currentPage; i++) {
			Page p = getPage(i);
			Vector page = p.getTuples();
			Iterator it = page.iterator();
			while (it.hasNext()) {
				Hashtable<String, Object> htblColNameValue = (Hashtable<String, Object>) it.next();
				returnMe.add(htblColNameValue);
			}
		}
		return returnMe;
	}

	public static String runLengthEncoding(String text) {
		String encodedString = "";
		for (int i = 0, count = 1; i < text.length(); i++) {
			if (i + 1 < text.length() && text.charAt(i) == text.charAt(i + 1))
				count++;
			else {
				encodedString = encodedString + (Integer.toString(count)) + (Character.toString(text.charAt(i))) + '-';
				count = 1;
			}
		}
		return encodedString;
	}

	public static String runLengthDecoding(String text) {
		String decodedString = "";
		String[] textArray = text.split("-");
		for (int i = 0; i < textArray.length; i++) {
			char value = textArray[i].charAt(textArray[i].length() - 1);
			int numberOfVal = Integer.parseInt(textArray[i].substring(0, textArray[i].length() - 1));
			for (int j = 0; j < numberOfVal; j++) {
				decodedString += value;

			}
		}

		return decodedString;
	}

	public void addIndexedColumn(String strColName) {
		indexedColumns.add(strColName);
	}

	public ArrayList<String> getIndexedColumns() {
		return indexedColumns;
	}

	public void setIndexedColumns(ArrayList<String> indexedColumns) {
		this.indexedColumns = indexedColumns;
	}

	public BitmapIndex getBitMap(String colName) {
		BitmapIndex bitMap = null;
		if (!indexedColumns.contains(colName))
			return null;
		try {

			FileInputStream fileIn = new FileInputStream("Tables\\" + name + "\\" + "index" + colName + ".bin");
			ObjectInputStream objectIn = new ObjectInputStream(fileIn);
			bitMap = (BitmapIndex) objectIn.readObject();
			objectIn.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			return bitMap;
		}
	}

	public void createBitmap(String colName) {
		BitmapIndex colBitMap = new BitmapIndex(name, colName);
		Vector records = getRecordsVector();
		Iterator it = records.iterator();
		ArrayList<Object> disValues = new ArrayList<Object>();
		while (it.hasNext()) {
			Hashtable<String, Object> htblColNameValue = (Hashtable<String, Object>) it.next();
			Object value = htblColNameValue.get(colName);
			if (!disValues.contains(value)) {
				disValues.add(value);
			}
		}

		for (Object disValue : disValues) {
			it = records.iterator();
			String myBitMap = "";
			while (it.hasNext()) {
				Hashtable<String, Object> htblColNameValue = (Hashtable<String, Object>) it.next();
				Object value = htblColNameValue.get(colName);
				if (value.equals(disValue))
					myBitMap += 1;
				else
					myBitMap += 0;
			}
			// myBitMap = runLengthEncoding(myBitMap);
			colBitMap.put(disValue, myBitMap);
		}
		// bitMap.put(colName, colBitMap);
		indexedColumns.add(colName);
	}

	public int getCurrentPage() {
		return currentPage;
	}

	public Page getPage(int pageNum) {
		Page page = null;
		try {

			FileInputStream fileIn = new FileInputStream("Tables\\" + name + "\\page" + pageNum + ".bin");
			ObjectInputStream objectIn = new ObjectInputStream(fileIn);
			page = (Page) objectIn.readObject();
			objectIn.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			return page;
		}
	}

	public boolean deletePage(int pageNum) {
		boolean deleted = false;
		try {

			File file = new File("Tables\\" + name + "\\page" + pageNum + ".bin");
			deleted = file.delete();
			System.out.println("delete page method ");

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			return deleted;
		}
	}

	public void setCurrentPage(int currentPage) {
		this.currentPage = currentPage;
	}

	public int getNumberOfRows() {
		return numberOfRows;
	}

	public void setNumberOfRows(int numberOfRows) {
		this.numberOfRows = numberOfRows;
	}

	public String getName() {
		return name;
	}

	public void getRecords() {
		for (int i = 1; i <= currentPage; i++) {
			Page p = getPage(i);
			Vector page = p.getTuples();
			Iterator it = page.iterator();

			while (it.hasNext()) {
				Hashtable<String, Object> htblColNameValue = (Hashtable<String, Object>) it.next();
				printRecord(htblColNameValue);
			}

		}
	}

	public static void printRecord(Hashtable<String, Object> htblColNameValue) {
		Set<String> colNames = htblColNameValue.keySet();
		for (String colName : colNames) {
			String column = colName;
			String value = htblColNameValue.get(colName).toString();
			System.out.print(column + ":" + " " + value + "  ,  ");

		}
		System.out.println("");
	}

	public void updateTuple(String strKey, Hashtable<String, Object> htblColNameValue) {
		// get page that contains the record using the key
		// br = new BufferedReader(new FileReader(FILENAME));

		// getting class type of clustering key
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("metadata.csv"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String clusterKeyType = "";
		String sCurrentLine;
		try {
			while ((sCurrentLine = br.readLine()) != null) {
				String[] words;
				words = sCurrentLine.split(",");
				if (words[0].equals(name) && words[1].equals(strClusteringKeyColumn)) {
					clusterKeyType = words[2];
					break;
				}
			}

		} catch (IOException e) {

			e.printStackTrace();

		}
		Page p = null;
		int insertedPage = 0;

		String strClusterKeyValOfInserted = "";
		int intClusterKeyValOfInserted = 0;
		double dblClusterKeyValOfInserted = 0;

		String strClusterKeyValOfCurrTuple = "";
		int intClusterKeyValOfCurrTuple = 0;
		double dblClusterKeyValOfCurrTuple = 0;

		boolean pageFound = false;
		// get number of correct page to insert in it.
		for (int i = 1; i <= currentPage; i++) {
			Page currPage = getPage(i);
			Hashtable<String, Object> currTuple = currPage.getLast();
			Object clusterKeyOfCurrTuple = (Object) currTuple.get(strClusteringKeyColumn);
			if (clusterKeyType.equals("java.lang.String")) {
				strClusterKeyValOfInserted = (String) strKey;
				strClusterKeyValOfCurrTuple = (String) clusterKeyOfCurrTuple;
				if (strClusterKeyValOfCurrTuple.compareToIgnoreCase(strClusterKeyValOfInserted) >= 0) {
					insertedPage = i;
					pageFound = true;
					break;
				}
			} else if (clusterKeyType.equals("java.lang.Integer")) {
				intClusterKeyValOfInserted = Integer.parseInt(strKey);
				intClusterKeyValOfCurrTuple = (int) clusterKeyOfCurrTuple;
				if (intClusterKeyValOfCurrTuple >= intClusterKeyValOfInserted) {
					insertedPage = i;
					pageFound = true;
					break;
				}
			} else if (clusterKeyType.equals("java.lang.Double")) {
				dblClusterKeyValOfInserted = Double.parseDouble(strKey);
				dblClusterKeyValOfCurrTuple = (double) clusterKeyOfCurrTuple;
				if (dblClusterKeyValOfCurrTuple >= dblClusterKeyValOfInserted) {
					insertedPage = i;
					pageFound = true;
					break;
				}
			}

		}
		if (!pageFound) {
			insertedPage = currentPage;
		}
		p = getPage(insertedPage);
		Hashtable<String, Object> targetTuple = p.deleteSingleTuple(strKey);
		// Hashtable<String, Integer> test= new Hashtable<String, Integer>();
		// Integer x= test.remove("k");
		int deletedIndex = (int) targetTuple.remove("deletedIndex");
		ArrayList<Integer> deletedIndexList = new ArrayList<Integer>();
		deletedIndexList.add(deletedIndex);
		deleteFromBitmap(targetTuple, deletedIndexList);
		Set<String> targetColNames = targetTuple.keySet();
		Set<String> newColNames = htblColNameValue.keySet();
		for (String newColName : newColNames) {
			for (String targetColName : targetColNames) {
				if (newColName.equals(targetColName)) {
					targetTuple.replace(targetColName, htblColNameValue.get(newColName));
				}
			}

		}
		addTuple(targetTuple);

	}

	public void deleteTuple(Hashtable<String, Object> htblColNameValue) {
		// stores the number of records in each page
		ArrayList<Integer> numOfRecordsDeleted = new ArrayList<Integer>(currentPage - 1);
		ArrayList<Integer> deletedIndeces = new ArrayList<Integer>();
		// initialize the array with zero
		for (int i = 1; i <= currentPage; i++) {
			numOfRecordsDeleted.add(0);
		}
		// add number of records deleted
		for (int i = 1; i <= currentPage; i++) {
			Page p = getPage(i);
			if (p != null) {
				ArrayList<Integer> tempDeletedIndeces = p.deleteTuple(htblColNameValue);
				numOfRecordsDeleted.add(i - 1, tempDeletedIndeces.size());
				for (Integer x : tempDeletedIndeces) {
					deletedIndeces.add(x);
				}
			}
		}
		deleteFromBitmap(htblColNameValue, deletedIndeces);
		// make arraylist cumulative
		ArrayList<Integer> cumulative_arr = new ArrayList<Integer>(numOfRecordsDeleted.size());
		cumulative_arr.add(0, numOfRecordsDeleted.get(0));
		for (int i = 1; i < cumulative_arr.size(); i++) {
			int sum = 0;
			for (int j = 0; j <= i; j++) {
				sum = sum + numOfRecordsDeleted.get(j);
			}
			cumulative_arr.add(i, sum);
		}

		// delete empty pages from bottom up before shifting
		Page p = getPage(currentPage);
		while (p.isEmpty()) {
			deletePage(currentPage--);
			p = getPage(currentPage);
		}
		// shifting up the records to fill the spaces in the page
		for (int i = 1; i < currentPage; i++) {
			if (cumulative_arr.get(i - 1) > 0) {
				p = getPage(i + 1);
				Hashtable<String, Object>[] shiftedRecords = p.shiftUp(cumulative_arr.get(i - 1));
				Page currPage = getPage(i);
				for (int j = 0; j < shiftedRecords.length; j++) {
					currPage.push(shiftedRecords[j]);
				}
			} else
				break;
		}
		// delete empty pages from bottom up after shifting
		p = getPage(currentPage);
		while (p.isEmpty()) {
			deletePage(currentPage--);
			p = getPage(currentPage);
		}

	}

	private void deleteFromBitmap(Hashtable<String, Object> htblColNameValue, ArrayList<Integer> indices) {
		Set<String> keySet = htblColNameValue.keySet();
		;
		for (String tupleKey : keySet) {
			for (String bitMapColName : indexedColumns) {
				if (tupleKey.equals(bitMapColName)) {
					getBitMap(bitMapColName).deleteTuple(indices);
				}
			}
		}
	}

	public void addTuple(Hashtable<String, Object> htblColNameValue) {

		Page p = null;
		int insertedPage = 0;
		Object clusterKey = (Object) htblColNameValue.get(strClusteringKeyColumn);
		String strClusterKeyValOfInserted = "";
		int intClusterKeyValOfInserted = 0;
		double dblClusterKeyValOfInserted = 0;

		String strClusterKeyValOfCurrTuple = "";
		int intClusterKeyValOfCurrTuple = 0;
		double dblClusterKeyValOfCurrTuple = 0;
		int insertIndex;

		boolean pageFound = false;
		// get number of correct page to insert in it.
		for (int i = 1; i <= currentPage; i++) {
			Page currPage = getPage(i);
			Hashtable<String, Object> currTuple = currPage.getLast();
			if (currTuple == null) {
				insertedPage = 1;
				pageFound = true;
				break;
			}
			Object clusterKeyOfCurrTuple = (Object) currTuple.get(strClusteringKeyColumn);
			if (clusterKey instanceof String) {
				strClusterKeyValOfInserted = (String) clusterKey;
				strClusterKeyValOfCurrTuple = (String) clusterKeyOfCurrTuple;
				if (strClusterKeyValOfCurrTuple.compareToIgnoreCase(strClusterKeyValOfInserted) >= 0) {
					insertedPage = i;
					pageFound = true;
					break;
				}
			} else if (clusterKey instanceof Integer) {
				intClusterKeyValOfInserted = (int) clusterKey;
				intClusterKeyValOfCurrTuple = (int) clusterKeyOfCurrTuple;
				if (intClusterKeyValOfCurrTuple >= intClusterKeyValOfInserted) {
					insertedPage = i;
					pageFound = true;
					break;
				}
			} else if (clusterKey instanceof Double) {
				dblClusterKeyValOfInserted = (double) clusterKey;
				dblClusterKeyValOfCurrTuple = (double) clusterKeyOfCurrTuple;
				if (dblClusterKeyValOfCurrTuple >= dblClusterKeyValOfInserted) {
					insertedPage = i;
					pageFound = true;
					break;
				}
			}

		}
		if (!pageFound) {
			insertedPage = currentPage;
		}

		// check if full and shiftDown
		for (int i = insertedPage; i <= currentPage; i++) {
			if (getPage(i).isFull()) {
				if (insertedPage == currentPage) {
					Page page = new Page(name, ++currentPage, strClusteringKeyColumn);

					page.push(p.shiftDown());
				} else {
					getPage(i + 1).push(p.shiftDown());
				}
				insertIndex = getPage(i).insertTuple(htblColNameValue);
				numberOfRows++;
				this.insertToBitMap(htblColNameValue, insertIndex);
			} else {
				numberOfRows++;
				insertIndex = getPage(i).insertTuple(htblColNameValue);
				this.insertToBitMap(htblColNameValue, insertIndex);
			}
		}
	}

	public Vector<Hashtable<String, Object>> getRecordsQuery(SQLTerm term) {
		Vector<Hashtable<String, Object>> resultRecords = new Vector<Hashtable<String, Object>>();
		for (int i = 1; i <= currentPage; i++) {
			Page currPage = getPage(i);
			Vector tuplesOfPage = currPage.getTuples();
			Iterator it = tuplesOfPage.iterator();
			while (it.hasNext()) {
				Hashtable<String, Object> htblColNameValue = (Hashtable<String, Object>) it.next();
				Object value = term.get_objValue();
				if (term.get_strOperator().equals("=")) {
					if (value instanceof String) {
						if (((String) (htblColNameValue.get(term.get_strColumnName())))
								.equals((String) term.get_objValue())) {
							resultRecords.add(htblColNameValue);
						}
					} else if (value instanceof Integer) {
						if (((Integer) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((Integer) term.get_objValue())) == 0) {
							resultRecords.add(htblColNameValue);
						}
					} else if (value instanceof Double) {
						if (((Double) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((Double) term.get_objValue())) == 0) {
							resultRecords.add(htblColNameValue);
						}
					}
				} else if (term.get_strOperator().equals(">")) {
					if (value instanceof String) {
						if (((String) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((String) term.get_objValue())) > 0) {
							resultRecords.add(htblColNameValue);
						}
					} else if (value instanceof Integer) {
						if (((Integer) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((Integer) term.get_objValue())) > 0) {
							resultRecords.add(htblColNameValue);
						}
					} else if (value instanceof Double) {
						if (((Double) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((Double) term.get_objValue())) > 0) {
							resultRecords.add(htblColNameValue);
						}
					}

				} else if (term.get_strOperator().equals(">=")) {
					if (value instanceof String) {
						if (((String) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((String) term.get_objValue())) >= 0) {
							resultRecords.add(htblColNameValue);
						}
					} else if (value instanceof Integer) {
						if (((Integer) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((Integer) term.get_objValue())) >= 0) {
							resultRecords.add(htblColNameValue);
						}
					} else if (value instanceof Double) {
						if (((Double) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((Double) term.get_objValue())) >= 0) {
							resultRecords.add(htblColNameValue);
						}
					}

				} else if (term.get_strOperator().equals("<")) {
					if (value instanceof String) {
						if (((String) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((String) term.get_objValue())) < 0) {
							resultRecords.add(htblColNameValue);
						}
					} else if (value instanceof Integer) {
						if (((Integer) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((Integer) term.get_objValue())) < 0) {
							resultRecords.add(htblColNameValue);
						}
					} else if (value instanceof Double) {
						if (((Double) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((Double) term.get_objValue())) < 0) {
							resultRecords.add(htblColNameValue);
						}
					}
				} else if (term.get_strOperator().equals("<=")) {
					if (value instanceof String) {
						if (((String) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((String) term.get_objValue())) <= 0) {
							resultRecords.add(htblColNameValue);
						}
					} else if (value instanceof Integer) {
						if (((Integer) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((Integer) term.get_objValue())) <= 0) {
							resultRecords.add(htblColNameValue);
						}
					} else if (value instanceof Double) {
						if (((Double) (htblColNameValue.get(term.get_strColumnName())))
								.compareTo(((Double) term.get_objValue())) <= 0) {
							resultRecords.add(htblColNameValue);
						}
					}
				} else if (term.get_strOperator().equals("!=")) {
					if (value instanceof String) {
						if (!(((String) (htblColNameValue.get(term.get_strColumnName())))
								.equals((((String) term.get_objValue()))))) {
							resultRecords.add(htblColNameValue);
						}
					} else if (value instanceof Integer) {
						if (((Integer) (htblColNameValue.get(term.get_strColumnName()))) != ((Integer) term
								.get_objValue())) {
							resultRecords.add(htblColNameValue);
						}
					} else if (value instanceof Double) {
						if (((Double) (htblColNameValue.get(term.get_strColumnName()))) != (((Double) term
								.get_objValue()))) {
							resultRecords.add(htblColNameValue);
						}
					}
				}
			}

		}

		return resultRecords;
	}

	@SuppressWarnings("unchecked")
	public Hashtable<String, Object> getRecordByIndex(int index) {
		return (Hashtable<String, Object>) getPage((int) (index / DBApp.getPageSize()) + 1).getTuples()
				.get(index % DBApp.getPageSize());
	}

	public Vector<Hashtable<String, Object>> getRecordsByBitMapIndex(String BitMapString) {
		Vector<Hashtable<String, Object>> resultSet = new Vector<Hashtable<String, Object>>();
		for (int i = 0; i < BitMapString.length(); i++) {
			if (BitMapString.charAt(i) == '1')
				resultSet.add(getRecordByIndex(i));
		}
		return resultSet;
	}

	private void insertToBitMap(Hashtable<String, Object> htblColNameValue, int index) {
		Set<String> keySet = htblColNameValue.keySet();
		for (String tupleKey : keySet) {
			for (String bitMapColName : indexedColumns) {
				if (tupleKey.equals(bitMapColName)) {
					getBitMap(bitMapColName).insertTuple(htblColNameValue, index);
				}
			}
		}
	}

	public Vector<Hashtable<String, Object>> getRecordsQueryIndexed(SQLTerm term) {
		Vector<Hashtable<String, Object>> resultRecords = new Vector<Hashtable<String, Object>>();

		Object value = term.get_objValue();
		BitmapIndex bmi = getBitMap(term.get_strColumnName());
		if (term.get_strOperator().equals("=")) {
			resultRecords.addAll((getRecordsByBitMapIndex(bmi.getBitMap().get(value))));
		} else if (term.get_strOperator().equals(">")) {
			Set<Object> keySet = bmi.getBitMap().keySet();
			//make sure of the and method in bitmapindex class because the anding of big integeres gives string of a big integer
			//declare x and  intiate it a string of  0s
			if (value instanceof String) {
				for (Object o : keySet) {
					if   (  (  (String)  o).compareTo((String) value ) >0)   {
						//ORed toghther
					}
				}
				//resultRecords.addAll((getRecordsByBitMapIndex(x)));
			} else if (value instanceof Integer) {
				//repeat as in string with correct type casting
			} else if (value instanceof Double) {

			}

			// resultRecords.addAll((getRecordsByBitMapIndex(BitMapString)));

		} else if (term.get_strOperator().equals(">=")) {
			if (value instanceof String) {

			} else if (value instanceof Integer) {

			} else if (value instanceof Double) {

			}

		} else if (term.get_strOperator().equals("<")) {
			if (value instanceof String) {

			} else if (value instanceof Integer) {

			} else if (value instanceof Double) {

			}
		} else if (term.get_strOperator().equals("<=")) {
			if (value instanceof String) {

			} else if (value instanceof Integer) {

			} else if (value instanceof Double) {

			}
		} else if (term.get_strOperator().equals("!=")) {
			if (value instanceof String) {

			} else if (value instanceof Integer) {

			} else if (value instanceof Double) {

			}
		}

		return resultRecords;
	}
}
