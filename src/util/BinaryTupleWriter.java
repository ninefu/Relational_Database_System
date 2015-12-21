package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import database.Tuple;

/**
 * BinaryTupleWriter writes a tuple to a buffer page with a size of 4096 bytes. If
 * the page is full or does not have space for one more tuple, the writer will flush the
 * buffer to the file to be written. Calling the reset will reset to the beginning of the file.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class BinaryTupleWriter implements TupleWriter {
	private FileOutputStream fou;
	private FileChannel fc;
	private ByteBuffer buffer;
	private int numAttr;
	private int tupleLimit;
	private int position;
	private int curTuple; // how many tuples has been written in the buffer page
	
	public BinaryTupleWriter(File file,int attr){
		try {
			fou = new FileOutputStream(file);
			fc = fou.getChannel();
			buffer = ByteBuffer.allocate(4096);
			numAttr = attr;
			curTuple = 0;
			tupleLimit = 0;
			if(numAttr > 0) {
				tupleLimit = (4096-8)/(4*numAttr);
				buffer.putInt(0, numAttr);
				position = 8;
			}
		} catch (FileNotFoundException e) {
			System.err.println("Error occured while opening the file");
			e.printStackTrace();
		}
	}

	/**
	 * Method to write a tuple to a buffer page. If the page is full or doesn't have space
	 * for one more tuple, the buffer page will be flushed to the disk and be cleared for future
	 * tuples.
	 * @param tup
	 * 			A Tuple object to be written to the file.
	 */
	@Override
	public void writeTuple(Tuple tup) {
		assert(tup.getValues().length == numAttr);
		// if the buffer page is full, write to disk
		if (curTuple == tupleLimit){
			buffer.putInt(4, curTuple);
			try {
				fc.write(buffer);
				buffer.clear();
				position = 8;
				curTuple = 0;
				buffer.putInt(0,numAttr);
			} catch (IOException e) {
				System.err.println("Error occured while flushing the buffer");
				e.printStackTrace();
			}
		}
		int[] attr = tup.getValues();
		for (int i = 0; i < attr.length; i++){
			buffer.putInt(position + i * 4, attr[i]);
		}
		position += numAttr * 4;
		curTuple++;
	}

	/**
	 * Method to reset the tupleWriter to the beginning of the file.It will
	 * overwrite existing content;
	 */
	@Override
	public void reset() {
		try {
			fc = fc.position(0);
			buffer.clear();
			buffer.putInt(0,numAttr);
			position = 8;
			curTuple = 0;
		} catch (IOException e) {
			System.err.println("Error occured while reading buffer");
			e.printStackTrace();
		}
	}

	/**
	 * Method to close the file
	 */
	@Override
	public void close() {
		// if there are tuples left on the buffer page
		// flush those tuples to disk before close
		if (curTuple != 0){
			buffer.putInt(4, curTuple);
			try {
				fc.write(buffer);
			} catch (IOException e) {
				System.err.println("Error occured while flushing the buffer");
			}
		}
		try {
			fc.close();
			fou.close();
		} catch (IOException e) {
			System.err.println("Error occured while closing the file");
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to get the maximum number of tuples the buffer can hold given 
	 * the number of attributes
	 * @return int
	 * 			representing the maximum value
	 */
	public int getTupleLimit(){
		return tupleLimit;
	}

}
