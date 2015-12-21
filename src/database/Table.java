package database;

import java.io.File;
import java.util.HashMap;

/**
 * A Table class maintains information about a table in a schema file. 
 * It includes a table's name, a file object of the table, and columns.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class Table {
	
	private class HiLoPair {
		private int hi;
		private int lo;
		
		public HiLoPair(int h, int l) { hi = h; l = lo; }
		public int hi() { return hi; }
		public int lo() { return lo; }
	}
	
	private String name;
	private File tableFile;
	private String[] columns;
	private HashMap<String, HiLoPair> attributeRanges;
	private int numTuples;
	
	/**
	 * Constructs a table object from a description in a schema file
	 * and the input directory of the database
	 * @param desc
	 * 			String array where the 0th element is the name of the table
	 * 			and all following indices are columns in the table.
	 * @param baseDir
	 * 			Absolute path of the input directory
	 */
	public Table(String[] desc, String baseDir) {
		name = desc[0];
		tableFile = new File(baseDir, "db/data/" + name);
		columns = new String[desc.length-1];
		for(int index = 0; index < desc.length-1; index++) {
			columns[index] = desc[index + 1];
		}
		attributeRanges = new HashMap<String, HiLoPair>();
	}
	
	/**
	 * Gets the File that contains data for this table.
	 * 
	 * @return
	 * 			File object that contains the data for this table.
	 */
	public File getDataFile() {
		return tableFile;
	}
	
	/**
	 * Gets the Table's name.
	 * 
	 * @return Table's name
	 * 					A String representing the table's name.
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * List of the names of columns in this table
	 * 
	 * @return schema
	 * 				List of Strings representing the names of columns in this table
	 */
	public String[] getSchema() {
		return columns;
	}
	
	/**
	 * Returns a list of the names of columns in this table in the form
	 * [Table Name].[Column Name]
	 * 
	 * @return A list of String representations of the columns in this table, prefixed
	 * by the name of the table. 
	 */
	public String[] getQualifiedSchema(String alias) {
		if(alias == null) {
			alias = name;
		}
		String[] qualColumns = new String[columns.length];
		int index = 0;
		for(String c : columns) {
			qualColumns[index] = String.format("%s.%s", alias, c);
			index++;
		}
		return qualColumns;
	}
	
	/**
	 * Method to get the String representation of a Table
	 * 
	 * @return String
	 * 			A list of String representing the name, path, and attribute
	 * 			names of a Table.
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append(String.format(" (%s)", getDataFile().getPath()));
		sb.append(": ");
		for (String col : columns) {
			sb.append(col);
			sb.append(", ");
		}
		return sb.substring(0, sb.length()-2);
	}
	
	public void setHighAndLow(String att, int hi, int lo) {
		HiLoPair hl = new HiLoPair(hi, lo);
		attributeRanges.put(att, hl);
	}
	
	public void setNumTuples(int n) {
		numTuples = n;
	}
	
	public int getHigh(String att) {
		return attributeRanges.get(att).hi();
	}
	
	public int getLow(String att) {
		return attributeRanges.get(att).lo();
	}
	
	public int getNumTuples() {
		return numTuples;
	}

}
