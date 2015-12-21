package database;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Tuple class maintains the information of a tuple.
 * A tuple has names of attributes, values of attributes, and a map of
 * attribute names and corresponding values.
 *
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */

public class Tuple {
	private Map<String, Integer> header;
	private String[] fieldList;
	private final int[] attributes;
	private TupleIdentifier rid;
	private boolean fastTuple;
	
	/**
	 * Constructor of a tuple.
	 * @param atts
	 * 				An array of String representing the attribute values.
	 * @param cols
	 * 				An array of String representing the attribute names.
	 */
	public Tuple(String[] atts, String[] cols){
		attributes = new int[atts.length];
		header = new HashMap<String,Integer>();
		fieldList = cols;
		int index = 0;
		for(String s: atts) {
			attributes[index] = Integer.parseInt(s);
			header.put(cols[index], index);
			index++;
		}
		fastTuple = false;
	}
	
	/**
	 * Constructor of a tuple.
	 * @param atts
	 * 				An array of Integer representing the attribute values.
	 * @param cols
	 * 				An array of String representing the attribute names.
	 */
	public Tuple(int[] atts, String[] cols) {
		attributes = atts;
		header = new HashMap<String, Integer>();
		fieldList = cols;
		for(int i = 0; i < cols.length; i++) {
			header.put(cols[i], i);
		}
		fastTuple = false;
	}
	
	/**
	 * Constructor that takes an optional "fast" argument -- with fast = true,
	 * the constructor does not initialize the mapping from column names to column
	 * indexes. This means that any call to getValueAtField() will fail. However,
	 * getValueAtIndex() will still work. This is useful when we are constructing lots
	 * of intermediate Tuples, and are not getting the attribute values of those tuples 
	 * as often.
	 * @param atts
	 * @param cols
	 * @param fast
	 */
	public Tuple(int[] atts, String[] cols, boolean fast) {
		fastTuple = fast;
		if(fastTuple) {
			attributes = atts;
			fieldList = cols;
		} else {
			attributes = atts;
			header = new HashMap<String, Integer>();
			fieldList = cols;
			for(int i = 0; i < cols.length; i++) {
				header.put(cols[i], i);
			}
		}
	}
	
	/**
	 * Set the RID of this Tuple to the page number
	 * it was read from, and the tuple number on that page.
	 * @param id Record id representing its location
	 * 				in page number, tuple number in
	 * 				binary format
	 */
	public void setRID(TupleIdentifier id) {
		rid = id;
	}
	
	/**
	 * Get the RID of this tuple (page number, tuple number)
	 * This will only be relevant if the tuple has been read
	 * from a binary file; otherwise there are no guarantees
	 * on the output of this function. 
	 * @return The TupleIdentifier representing the page number,
	 * 			and tuple number location from which this tuple
	 * 			was read.
	 */
	public TupleIdentifier getRID() {
		return rid;
	}
	
	/**
	 * Method to get all the attribute values.
	 * 
	 * @return Integer array
	 * 				An array of Integer representing the attribute values.
	 */
	public int[] getValues() {
		return attributes;
	}
	
	/**
	 * Method to get all the attribute names.
	 * 
	 * @return String array
	 * 				An array of String representing the attribute values.
	 */
	public String[] getFields() {
		return fieldList;
	}
	
	/**
	 * Method to get the attribute value at a particular position.
	 * 
	 * @param index
	 * 				An Integer indicating the position.
	 * @return value
	 * 				An Integer representing the desired value.
	 */
	public int getValueAtIndex(int index) {
		return attributes[index];
	}
	
	/**
	 * Method to get the attribute value of a particular field/attribute name
	 * If the tuple is a quickly constructed tuple (fastTuple) this will take
	 * O(n) time where n is the number of fields. If the tuple is a normal
	 * tuple (!fastTuple), this will take O(1) time.
	 * @param field
	 * 				A String representing the desired field name.
	 * @return value
	 * 				An Integer representing the desired value.
	 */
	public Integer getValueAtField(String field) {
		Integer index = null;
		if(fastTuple) {
			for(int i = 0; i < fieldList.length; i++) {
				if(field.equals(fieldList[i])) {
					index = i;
				}
			}
		} else {
			index = header.get(field);
		}
		if(index == null) {
			return null;
		}
		return getValueAtIndex(index);
	}
	
	public int getIndexOfField(String field) {
		for(int i = 0; i < fieldList.length; i++) {
			if(field.equals(fieldList[i])) {
				return i;
			}
		}
		return -1;
	}
	
	/**
	 * Method to get the String representation of a tuple's value.
	 * 
	 * @return String
	 * 				A String representing the value of the tuple
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int att : attributes) {
			if(sb.length() != 0) {
				sb.append(",");
			}
			sb.append(att);
		}
		return sb.toString();
	}
	
	/**
	 * Method to know whether two tuples have the same attribute values
	 * @param Object that this tuple compares to
	 * @return Boolean
	 * 			True if two tuples have the same attribute values, false otherwise
	 */
	@Override
	public boolean equals(Object o) {
		if(!(o instanceof Tuple)) {
			return false;
		}
		Tuple other = (Tuple)o;
		return Arrays.equals(getValues(), other.getValues()) && Arrays.equals(getFields(), other.getFields());
	}

}
