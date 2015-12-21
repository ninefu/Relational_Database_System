package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import database.Table;
import database.Tuple;
import database.TupleIdentifier;

/**
 * BinaryTupleReader reads a binary file one page at a time (page-at-a-time I/O). 
 * A page has Constants.pageSize bytes and is stored in a ByteBuffer. Each call of readNextTuple()
 * reads the next tuple in the page and constructs a Tuple object from the data.
 * Calling the reset will reset to the beginning of the binary file.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class BinaryTupleReader implements TupleReader {
	private FileInputStream fin;
	private FileChannel fc;
	private ByteBuffer buffer;
	private int numAttr;
	private int numTuple; //on a buffer page
	private int position;
	private int currentPageNumber;
	private int currentTupleNumber;
	private String[] colnames;
	
	/**
	 * Convenience constructor: create a BinaryTupleReader to read
	 * a table with no alias.
	 * 
	 * @param table
	 * 			Table to read tuples from
	 */
	public BinaryTupleReader(Table table){
		this(table, null);
	}
	
	/**
	 * Create a reader to read tuples from a table that has
	 * been referenced with an alias
	 * @param table
	 * 			Table to read tuples from
	 * @param alias
	 * 			String alias the table is referenced by
	 */
	public BinaryTupleReader(Table table, String alias) {
		this(table.getDataFile(), table.getSchema());
		String ref = alias == null ? table.getName() : alias;
		for(int i = 0; i < colnames.length; i++) {
			colnames[i] = String.format("%s.%s", ref, colnames[i]);
		}
	}
	
	/**
	 * Create a reader to read tuples from a certain file
	 * assuming those tuples have the schema provided
	 * @param f
	 * 			File to read tuples from
	 * @param schema
	 * 			Schema in the form of an array of strings, where
	 * 			each string is the qualified name of the column
	 * 			referenced
	 */
	public BinaryTupleReader(File f, String[] schema) {
		try {
			currentPageNumber = 0;
			currentTupleNumber = 0;
			colnames = new String[schema.length];
			for(int i = 0; i < colnames.length; i++) {
				colnames[i] = schema[i];
			}
			fin = new FileInputStream(f);
			fc = fin.getChannel();
			buffer = ByteBuffer.allocate(Constants.pageSize);
			int length = fc.read(buffer);
			if (length != -1){
				numAttr = buffer.getInt(0);
				numTuple = buffer.getInt(4);
				position = 8;
			} else {
				throw new FileNotFoundException();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void readNewPage(int page) {
		int pos = Constants.pageSize * page;
		buffer.clear();
		try {
			fc.position(pos);
			fc.read(buffer);
			currentPageNumber = page;
			currentTupleNumber = 0;
			numAttr = buffer.getInt(0);
			numTuple = buffer.getInt(4);
			position = 8;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Warning: this method will disrupt the state of getNextTuple()
	 * It is only to be used when you need to read a specific tuple
	 * from a specific page.
	 * @param page
	 * @param tup
	 * @return The specified tuple
	 */
	public Tuple readAt(TupleIdentifier rid) {
		readNewPage(rid.getPageNumber());
		position += rid.getTupleNumber() * (numAttr*4);
		int[] attr = new int[numAttr];
		for (int i = 0; i < attr.length; i++){
			attr[i] = buffer.getInt(position + i * 4);
		}
		Tuple tuple = new Tuple(attr, colnames);
		tuple.setRID(new TupleIdentifier(currentPageNumber, currentTupleNumber));
		position += numAttr * 4;
		numTuple--;
		currentTupleNumber++;
		return tuple;
	}
	
	/**
	 * Method to read the next tuple from the buffer page.
	 * 
	 * @return The next available tuple or null if reaches the end of the binary file.
	 */
	@Override
	public Tuple readNextTuple() {
		// if reaches the end of this buffer page, clear the buffer page,
		// read the next available page and update number of attributes & number of tuples
		// accordingly.
		// return null if no more page to read
		if (numTuple == 0){
			buffer.clear();
			try {
				int length = fc.read(buffer);
				if (length == -1){
					return null;
				}else{
					currentPageNumber++;
					currentTupleNumber = 0;
					numAttr = buffer.getInt(0);
					numTuple = buffer.getInt(4);
					position = 8;
				}
			} catch (IOException e) {
				System.err.println("Error occured while reading buffer");
				e.printStackTrace();
			}
		}
		int[] attr = new int[numAttr];
		for (int i = 0; i < attr.length; i++){
			attr[i] = buffer.getInt(position + i * 4);
		}
		Tuple tuple = new Tuple(attr, colnames);
		tuple.setRID(new TupleIdentifier(currentPageNumber, currentTupleNumber));
		position += numAttr * 4;
		numTuple--;
		currentTupleNumber++;
		return tuple;
	}

	/** 
	 * Method to reset the tuple reader to the beginning of the binary file.
	 */
	@Override
	public void reset() {
		try {
			currentPageNumber = 0;
			currentTupleNumber = 0;
			fc = fc.position(0);
			buffer.clear();
			if (fc.read(buffer) != -1){
				numAttr = buffer.getInt(0);
				numTuple = buffer.getInt(4);
				position = 8;
			}else{
				throw new FileNotFoundException();
			}
		} catch (IOException e) {
			System.err.println("Error occured while reading buffer");
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to reset the tuple reader to a particular tuple specified by a tuple index
	 * @param index
	 * 			An integer representing the ith tuple in the binary file. Index starts from 0.
	 * 
	 */
	public void reset(int index){
		try{
			fc = fc.position(0);
			buffer.clear();
			if (fc.read(buffer) != -1){
				numTuple = buffer.getInt(4);
				buffer.clear();
				int fcPosition = index/numTuple * Constants.pageSize;
				fc = fc.position(fcPosition);
				if (fc.read(buffer) != -1){
					currentPageNumber = index/numTuple;
					currentTupleNumber = index%numTuple;
					numAttr = buffer.getInt(0);
					position = (index%numTuple) * numAttr * 4 + 8;
					numTuple = buffer.getInt(4) - index%(numTuple);
				}else{
					throw new IndexOutOfBoundsException();
				}
			}else{
				throw new FileNotFoundException();
			}
		} catch (IOException e) {
			System.err.println("Error occured while reading buffer");
			e.printStackTrace();
		}
	}

	/**
	 * Method to close the binary file.
	 */
	@Override
	public void close() {
		try {
			fin.close();
		} catch (IOException e) {
			System.err.println("Error occured while closing the file.");
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to get the number of attributes for a tuple
	 * @return int
	 * 			number of attributes
	 */
	public int getAttributeNumber(){
		return numAttr;
	}
	
	/**
	 * Method to get the number of tuples stored in this page
	 * @return int
	 * 			number of tuples
	 */
	public int getTupleNumber(){
		return numTuple;
	}
	
	/**
	 * Get the current page number the tuple belongs to
	 * @return int representing the number of the page
	 */
	public int getCurrentPageNumber(){
		return currentPageNumber;
	}
	
	/**
	 * Get the current tuple number on this page;
	 * @return int representing the number of tuple
	 */
	public int getCurrentTupleNumber(){
		return currentTupleNumber;
	}
	
}
