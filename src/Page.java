
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

public class Page implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int pageNumber;
	private String tableName;
	private int maxNumberOfTuples;
	private String ClusteringKey;
	private Vector<Hashtable<String, Object>> tuples = null;

	@SuppressWarnings("rawtypes")
	public Vector getTuples() {
		return tuples;
	}

	@SuppressWarnings("rawtypes")
	public Page(String tName, int pNumber, String ClusteringKey) {
		this.pageNumber = pNumber;
		this.tableName = tName;
		this.maxNumberOfTuples = DBApp.getPageSize();
		this.ClusteringKey = ClusteringKey;
		this.tuples = new Vector(maxNumberOfTuples);
		// create object
		// create .bin file in the right directory
		try {

			File file = new File("Tables\\" + tName + "\\" + "page" + pNumber + ".bin");
			file.createNewFile();

			FileOutputStream fileOut = new FileOutputStream("Tables\\" + tName + "\\" + "page" + pNumber + ".bin");
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
			objectOut.writeObject(this);

			objectOut.close();
			fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public boolean isFull() {
		if (tuples.size() == maxNumberOfTuples) {
			return true;
		} else {
			return false;
		}
	}

	public boolean isEmpty() {
		if (tuples.size() == 0) {
			return true;
		} else {
			return false;
		}
	}

	public void push(Hashtable<String, Object> htblColNameValue) {
		tuples.add(htblColNameValue);
		saveToDisk();
	}

	@SuppressWarnings("unchecked")
	public int insertTuple(Hashtable<String, Object> htblColNameValue) {

		Set<String> colNames = htblColNameValue.keySet();
		Object keyValue = null;
		for (String colName : colNames) {
			if (colName.equals(ClusteringKey)) {

				keyValue = (htblColNameValue.get(colName));
			}
		}
		int insertIndex = 0;
		if (keyValue instanceof Integer) {
			insertIndex = sortInt((int) keyValue);
		} else {
			if (keyValue instanceof Double) {
				insertIndex = sortDouble((double) keyValue);
			} else {
				if (keyValue instanceof String) {
					insertIndex = sortString((String) keyValue);
				}
			}
		}

		tuples.add(insertIndex, htblColNameValue);	
		saveToDisk();
		return (pageNumber-1)*maxNumberOfTuples + insertIndex;
	}

	private int sortInt(int keyValue) {
		Iterator iter = tuples.iterator();
		int i = 0;
		while (iter.hasNext()) {
			Hashtable currentTable = (Hashtable) iter.next();
			if (((int) (currentTable.get(ClusteringKey)) >= (keyValue))) {
				return i;
			}
			i++;
		}
		return i;
	}

	private int sortDouble(double keyValue) {
		Iterator iter = tuples.iterator();
		int i = 0;
		while (iter.hasNext()) {
			Hashtable currentTable = (Hashtable) iter.next();
			if (((double) (currentTable.get(ClusteringKey)) >= (keyValue))) {
				return i;
			}
			i++;
		}
		return i;
	}

	private int sortString(String keyValue) {
		Iterator iter = tuples.iterator();
		int i = 0;
		while (iter.hasNext()) {
			Hashtable currentTable = (Hashtable) iter.next();
			if (((((String) currentTable.get(ClusteringKey)).compareTo(keyValue))) >= 0) {
				return i;
			}
			i++;
		}
		return i;
	}

	@SuppressWarnings("rawtypes")
	public ArrayList<Integer> deleteTuple(Hashtable<String, Object> htblColNameValue) {
		int tempSize = tuples.size();
		Set<String> colNames = htblColNameValue.keySet();
		Iterator iter = tuples.iterator();
		int counter = (pageNumber-1)*maxNumberOfTuples;
		ArrayList<Integer> returnList = new ArrayList<Integer>();
		while (iter.hasNext()) {
			Hashtable currentTable = (Hashtable) iter.next();
			boolean deleteMe = false;
			for (String colName : colNames) {
				// checl if colName exists in hashtable beta3et el iterator
				if (currentTable.containsKey(colName)) {
					if (currentTable.containsValue(htblColNameValue.get(colName))) {
						deleteMe = true;
					} else {
						deleteMe = false;
					}
				} else {
					deleteMe = false;
				}
			}
			if (deleteMe) {
				tuples.remove(currentTable);
				returnList.add(counter);
			}
			counter ++;
		}
		tuples.remove(htblColNameValue);
		tempSize -= tuples.size();
		saveToDisk();
		return returnList;
	}

	public int getPageNumber() {
		return pageNumber;
	}

	@SuppressWarnings("unchecked")
	public Hashtable<String, Object>[] shiftUp(int s) {
		Hashtable<String, Object>[] returnMe = (Hashtable<String, Object>[]) new Object[s];
		for (int i = 0; i < s; i++) {
			returnMe[i] = (Hashtable<String, Object>) tuples.remove(0);
		}
		saveToDisk();
		return returnMe;
	}

	@SuppressWarnings("unchecked")
	public Hashtable<String, Object> shiftDown() {
		Hashtable<String, Object> returnMe = (Hashtable<String, Object>) tuples.remove(tuples.size() - 1);
		saveToDisk();
		return returnMe;
	}

	@SuppressWarnings("unchecked")
	public Hashtable<String, Object> getFirst() {
		Hashtable<String, Object> returnMe = new Hashtable<String, Object>();
		returnMe = (Hashtable<String, Object>) tuples.get(0);
		return returnMe;
	}

	@SuppressWarnings("unchecked")
	public Hashtable<String, Object> getLast() {
		if(tuples.isEmpty()) {
			return null;
		}
		Hashtable<String, Object> returnMe = new Hashtable<String, Object>();
		returnMe = (Hashtable<String, Object>) tuples.get(tuples.size() - 1);
		return returnMe;
	}

	public Hashtable<String, Object> deleteSingleTuple(String strKey) {
		Iterator iter = tuples.iterator();
		Hashtable currentTuple=null;
		boolean found = false;
		int counter=maxNumberOfTuples*(pageNumber-1);
		while (iter.hasNext()) {
			currentTuple = (Hashtable) iter.next();
			Set<String> colNames = currentTuple.keySet();
			boolean deleteMe = false;
			for (String colName : colNames) {
				// checl if colName exists in hashtable beta3et el iterator
				if (colName.equalsIgnoreCase(ClusteringKey)) {
					if (((String) currentTuple.get(colName)).equals(strKey)) {

						found = true;
						break;
					}
				}

			}
			counter ++;
		}
		if(!found)
			return null;
		tuples.remove(currentTuple);
		saveToDisk();
		currentTuple.put("deletedIndex", counter);
		return currentTuple;
		// tuples.remove(htblColNameValue);
	}
	public void saveToDisk() {
		try {
			FileOutputStream fileOut = new FileOutputStream("Tables\\" + tableName + "\\" + "page" + pageNumber + ".bin");
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
			objectOut.writeObject(this);

			objectOut.close();
			fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		
		}
	}
}
