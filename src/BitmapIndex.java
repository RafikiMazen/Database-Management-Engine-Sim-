import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

public class BitmapIndex implements Serializable {
	String colName;
	String tableName;
	Hashtable<Object, String> bitMap;

	public Hashtable<Object, String> getBitMap() {
		return bitMap;
	}

	public void setBitMap(Hashtable<Object, String> bitMap) {
		this.bitMap = bitMap;
		this.saveToDisk();
	}

	public BitmapIndex(String tableName, String colName) {
		this.colName = colName;
		this.tableName = tableName;
		this.bitMap = new Hashtable<Object, String>();
		
		
		
		
		try {
			File file = new File("Tables\\" + tableName + "\\" + "index" + colName+ ".bin");
			file.createNewFile();

			FileOutputStream fileOut = new FileOutputStream("Tables\\" + tableName + "\\" + "index" + colName+ ".bin");
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
			objectOut.writeObject(this);

			objectOut.close();
			fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		StringBuilder sb = new StringBuilder();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("metadata.csv"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String sCurrentLine;
		try {
			while ((sCurrentLine = br.readLine()) != null) {
				String[] words;
				words = sCurrentLine.split(",");
				if (words[0].equals(tableName) && words[1].equals(colName)) {
					words[4]="true";
					sb.append(words[0]+",");
					sb.append(words[1]+",");
					sb.append(words[2]+",");
					sb.append(words[3]+",");
					sb.append(words[4]);
					sb.append('\n');
				}else {
					sb.append(sCurrentLine);
					sb.append('\n');
				}
			}

		} catch (IOException e) {

			e.printStackTrace();

		}
		try (FileWriter writer = new FileWriter(new File("metadata.csv"))) {
			writer.write(sb.toString());
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void deleteTuple(ArrayList<Integer> indices) {
		Set<Object> bitMapKeys = bitMap.keySet();
		int counter = 0;
		for (Integer x : indices) {
			for (Object bitMapKey : bitMapKeys) {
				bitMap.replace(bitMapKey, removeChar(bitMap.get(bitMapKey), x - counter));
			}
			counter++;
		}
		this.saveToDisk();
	}

	private static String removeChar(String str, int index) {
		str = runLengthDecoding(str);
		StringBuilder sb = new StringBuilder(str);
		return runLengthEncoding(sb.deleteCharAt(index).toString());
	}

	public void insertTuple(Hashtable<String, Object> tuple, int index) {
		Set<String> colNames = tuple.keySet();
		for (String currColName : colNames) {
			if (currColName.equals(this.colName)) {
				Set<Object> bitMapKeys = bitMap.keySet();
				Boolean isNewValue = true;
				Object temp = null;
				for (Object bitMapKey : bitMapKeys) {
					temp = bitMapKey;
					if (tuple.get(currColName).equals(bitMapKey)) {
						isNewValue = false;
						bitMap.replace(bitMapKey, insertOne(bitMap.get(bitMapKey), index));
					} else {
						bitMap.replace(bitMapKey, insertZero(bitMap.get(bitMapKey), index));
					}
				}
				if (isNewValue) {
					String sequence = "";
					for (int i = 0; i < index; i++) {
						sequence += 0;
					}
					sequence += 1;
					// insert zeros for the remaining tuples
					for (int i = index; i <= bitMap.get(temp).length(); i++) {
						sequence += 0;
					}

					put(currColName, sequence);

				}

			}
		}
		this.saveToDisk();
	}

	private static String insertOne(String initBits, int index) {
		initBits = runLengthDecoding(initBits);
		StringBuilder sb = new StringBuilder(initBits);
		sb.insert(index, '1');
		return runLengthEncoding(sb.toString());
	}

	private static String insertZero(String initBits, int index) {
		initBits = runLengthDecoding(initBits);
		StringBuilder sb = new StringBuilder(initBits);
		sb.insert(index, '0');
		return runLengthEncoding(sb.toString());
	}

	public void put(Object disValue, String myBitMap) {
		bitMap.put(disValue, myBitMap);
		this.saveToDisk();
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

	public String getBitSequenceValue(Object value) {
		return bitMap.get(value);
	}

	private static String and(String s1, String s2) {
		s1 = runLengthDecoding(s1);
		s2 = runLengthDecoding(s2);
		BigInteger b1 = new BigInteger(s1, 2);
		BigInteger b2 = new BigInteger(s2, 2);
		String returnMe = b1.and(b2).toString();
		return returnMe;
	}

	private static String or(String s1, String s2) {
		s1 = runLengthDecoding(s1);
		s2 = runLengthDecoding(s2);
		BigInteger b1 = new BigInteger(s1, 2);
		BigInteger b2 = new BigInteger(s2, 2);
		String returnMe = b1.or(b2).toString();
		return returnMe;
	}

	private static String xor(String s1, String s2) {
		s1 = runLengthDecoding(s1);
		s2 = runLengthDecoding(s2);
		BigInteger b1 = new BigInteger(s1, 2);
		BigInteger b2 = new BigInteger(s2, 2);
		String returnMe = b1.xor(b2).toString();
		return returnMe;
	}

	public void saveToDisk() {
		try {
			FileOutputStream fileOut = new FileOutputStream("Tables\\" + tableName + "\\" + "index" + colName + ".bin");
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
			objectOut.writeObject(this);

			objectOut.close();
			fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
	}

	public Boolean containsValue(Object value) {
		Set<Object> values = bitMap.keySet();
		return values.contains(value);
	}
}