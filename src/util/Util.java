package util;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import database.DatabaseCatalog;
import database.Table;
import database.Tuple;

/**
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 *
 */
public class Util {
	
	private class TupleComparator implements Comparator<Tuple> {
		
		@Override
		public int compare(Tuple o1, Tuple o2) {
			String[] atts = o1.getFields(); // Should be the same for both
			for(String att : atts) {
				int v1 = o1.getValueAtField(att);
				int v2 = o2.getValueAtField(att);
				int ret = v1 - v2;
				if(ret != 0) {
					return ret;
				}
			}
			return 0;
		}		
	}
	
	public void sortReadableFile(File src, File dest, String[] schema) {
		sortFile(true, src, dest, schema);
	}
	
	
	public void sortBinaryFile(File src, File dest, String[] schema) {
		sortFile(false, src, dest, schema);
	}
	
	/**
	 * Take in a file, sort the output by lexigraphic order of column names, 
	 * and write it to a new file. Uses in-memory sort, so this is primarily
	 * a testing utility.
	 * 
	 * @param schema
	 * 			Schema of tuples that you are sorting, for proper construction of
	 * 			Tuple objects and proper sorting.
	 * @param humanReadable
	 * 			Whether the file is in human-readable format (alternatively: binary format)
	 * @param src
	 * 			The file from which to read the results from
	 * @param dest
	 * 			The file to which the results should be written out to.
	 */
	public void sortFile(boolean humanReadable, File src, File dest, String[] schema) {
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		TupleReader tr;
		if(humanReadable) {
			tr = new ReadableTupleReader(src, schema);
		} else {
			tr = new BinaryTupleReader(src, schema);
		}
		Tuple tup;
		while((tup = tr.readNextTuple()) != null) {
			tuples.add(tup);
		}
		tr.close();
		Collections.sort(tuples, new TupleComparator());
		TupleWriter tw;
		if(humanReadable) {
			tw = new ReadableTupleWriter(dest);
		} else {
			tw = new BinaryTupleWriter(dest, schema.length);
		}
		for(Tuple t : tuples) {
			tw.writeTuple(t);
		}
		tw.close();
	}
	
	public void sortFile(TupleReader tr, TupleWriter tw) {
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		Tuple tup;
		while((tup = tr.readNextTuple()) != null) {
			tuples.add(tup);
		}
		Collections.sort(tuples, new TupleComparator());
		for(Tuple t : tuples) {
			tw.writeTuple(t);
		}
	}
	
	public static void main(String[] args) {
		RandomGenerator rg = new RandomGenerator(10, 100, 3, 100);
		DatabaseCatalog db = rg.getDatabase(true);
		Table t = db.getTable("Table0");
		Util util = new Util();
		util.sortReadableFile(t.getDataFile(), new File("testing_sortutil"), t.getQualifiedSchema(null));
	}

}
