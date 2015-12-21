/**
 * 
 */
package util;

import java.io.File;

import database.Table;
import database.Tuple;

/**
 * A class to convert between binary format and human-readable format
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class Converter {
	
	private void readAndWrite(TupleReader tr, TupleWriter tw) {
		Tuple tup;
		while((tup = tr.readNextTuple()) != null) {
			tw.writeTuple(tup);
		}
		tr.close();
		tw.close();	
	}
	
	public void BinaryToReadable(Table table, File writeTo) {
		TupleReader tr = new BinaryTupleReader(table);
		TupleWriter tw = new ReadableTupleWriter(writeTo);
		readAndWrite(tr, tw);
	}
	
	public void BinaryToReadable(File readFrom, String[] schema, File writeTo) {
		TupleReader tr = new BinaryTupleReader(readFrom, schema);
		TupleWriter tw = new ReadableTupleWriter(writeTo);
		readAndWrite(tr, tw);
	}
	
	public void ReadableToBinary(Table table, File writeTo) {
		TupleReader tr = new ReadableTupleReader(table, null);
		TupleWriter tw = new BinaryTupleWriter(writeTo, table.getSchema().length);
		readAndWrite(tr, tw);
	}

}
