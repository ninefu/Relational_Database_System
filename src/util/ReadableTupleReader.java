package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import database.Table;
import database.Tuple;

/**
 * ReadableTupleReader reads a tuple from a human readable file. Calling
 * reset() will reset to the beginning of the file.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class ReadableTupleReader implements TupleReader {

	private FileInputStream baseFileStream;
	private BufferedReader reader;
	private String[] schema;
	
	public ReadableTupleReader(Table table,String alias){
		this(table.getDataFile(), table.getQualifiedSchema(alias));
	}
	
	public ReadableTupleReader(File f, String[] qualifiedSchema){
		try {
			baseFileStream = new FileInputStream(f);
			reader = new BufferedReader(new InputStreamReader(baseFileStream));
			schema = qualifiedSchema;
		} catch (FileNotFoundException e) {
			// If we hit this exception, it is programmer error, so throw
			// a runtime exception. 
			throw new RuntimeException(e);
		}
	}
	
	
	/** Method to read the next line of the file and get the next tuple repeatedly.
	 * 
	 * @return Tuple
	 * 				the next available tuple output
	 */
	@Override
	public Tuple readNextTuple() {
		try {
			String line = reader.readLine();
			if(line != null) {
				return new Tuple(line.trim().split(","), schema);
			}
			return null;
		} catch (IOException e) {
			System.out.println("Error occurred while getting next tuple from ReadableTupleReader.");
			e.printStackTrace();
			return null;
		}
	}

	/** 
	 * Method to reset the tuple reader to the beginning of the file.
	 */
	@Override
	public void reset() {
		try {
			baseFileStream.getChannel().position(0);
			reader = new BufferedReader(new InputStreamReader(baseFileStream));
		} catch (IOException e) {
			System.out.println("Error occurred while resetting ReadableTupleReader.");
			e.printStackTrace();
		}
	}

	/**
	 * Method to close the file.
	 */
	@Override
	public void close() {
		try {
			baseFileStream.close();
		} catch (IOException e) {
			System.err.println("Error occured while closing the file.");
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to reset the file to a specific position
	 * 
	 * @param index
	 * 			Integer representing the target position in the file
	 */
	public void reset(int index){
//		try {
//			baseFileStream.getChannel().position((index - 1) * 8);
//			reader = new BufferedReader(new InputStreamReader(baseFileStream));
//			System.out.println(reader.readLine());
//		} catch (IOException e) {
//			System.out.println("Error occurred while resetting ReadableTupleReader.");
//			e.printStackTrace();
//		}
		
		try {
			baseFileStream.getChannel().position(0);
			reader = new BufferedReader(new InputStreamReader(baseFileStream));
			int count = 0;
			Tuple tr = null;
			while (count < index){
				tr = readNextTuple();
				count++;
			}
			if (index != 0 && tr == null){
				throw new IndexOutOfBoundsException();
			}
		} catch (IOException e) {
			System.out.println("Error occurred while resetting ReadableTupleReader.");
			e.printStackTrace();
		}
	}
}
