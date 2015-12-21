package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import database.Tuple;

/**
 * ReadableTupleWriter writes a tuple to the file in a human readable format.
 * Calling reset() will reset to the beginning of the file and overwrite
 * existing content.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class ReadableTupleWriter implements TupleWriter {
	private FileOutputStream fou;
	private BufferedWriter bw;
	
	public ReadableTupleWriter(File file){
		try {
			fou = new FileOutputStream(file);
			bw = new BufferedWriter(new OutputStreamWriter(fou));
		} catch (FileNotFoundException e) {
			System.out.println("Error occurred while opening the file.");
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to write a tuple to a file in a human readable way.
	 */
	@Override
	public void writeTuple(Tuple tup) {
		String values = tup.toString();
		try {
			bw.write(values);
			bw.newLine();
		} catch (IOException e) {
			System.out.println("Error occurred while writing a tuple to the file.");
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to reset the tupleWriter to the beginning of the file. It will
	 * overwrite existing content;
	 */
	@Override
	public void reset() {
		try {
			fou.getChannel().position(0);
			bw = new BufferedWriter(new OutputStreamWriter(fou));
		} catch (IOException e) {
			System.out.println("Error occurred while resetting ReadableTupleWriter.");
			e.printStackTrace();
		}
	}

	/**
	 * Method to close the file
	 */
	@Override
	public void close() {
		try {
			bw.close();
			fou.close();
		} catch (IOException e) {
			System.out.println("Error occurred while closing ReadableTupleWriter.");
			e.printStackTrace();
		}
	}

}
