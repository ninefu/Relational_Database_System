package testing;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import database.Table;
import database.Tuple;
import util.BinaryTupleReader;
import util.BinaryTupleWriter;

public class BinaryTupleWriterTester {
	File file = new File("samples/input/db/data","BinaryWriterTestBoats");
	String[] cols = new String[]{"D","E","F"};
	
	String[] des = new String[]{"BinaryWriterTestBoats","D","E","F"};
	Table table = new Table(des,"samples/input/");
	
	@Test
	public void testWriteTuple() {
		BinaryTupleWriter tw = new BinaryTupleWriter(file, cols.length);
		int[] attr;
		Tuple tuple;
		
		attr = new int[]{88,98,67};
		tuple = new Tuple(attr,cols);
		tw.writeTuple(tuple);
		
		attr = new int[]{40,32,19};
		tuple = new Tuple(attr,cols);
		tw.writeTuple(tuple);
		
		attr = new int[]{12,92,54};
		tuple = new Tuple(attr,cols);
		tw.writeTuple(tuple);
		
		tw.close();
		
		BinaryTupleReader tr = new BinaryTupleReader(table);
		assertEquals(3,tr.getTupleNumber());
		assertEquals(3,tr.getAttributeNumber());
		Tuple tup; // for reader
		tup = tr.readNextTuple();
		assertEquals("88,98,67",tup.toString());
		tup = tr.readNextTuple();
		assertEquals("40,32,19",tup.toString());
		tup = tr.readNextTuple();
		assertEquals("12,92,54",tup.toString());
		tup = tr.readNextTuple();
		assertEquals(null,tup);
		
		tr.close();
	}

	@Test
	public void testReset(){
		BinaryTupleWriter tw = new BinaryTupleWriter(file, cols.length);
		int[] attr;
		Tuple tuple;
		
		attr = new int[]{88,98,67};
		tuple = new Tuple(attr,cols);
		tw.writeTuple(tuple);
		
		attr = new int[]{40,32,19};
		tuple = new Tuple(attr,cols);
		tw.writeTuple(tuple);
		
		tw.reset();
		attr = new int[]{16,44,68};
		tuple = new Tuple(attr,cols);
		tw.writeTuple(tuple);
		tw.close();
		
		BinaryTupleReader tr = new BinaryTupleReader(table);
		assertEquals(1,tr.getTupleNumber());
		assertEquals(3,tr.getAttributeNumber());
		Tuple tup; // for reader
		tup = tr.readNextTuple();
		assertEquals("16,44,68",tup.toString());
		tup = tr.readNextTuple();
		assertEquals(null,tup);
		
		tr.close();
		
	}
	
	// Seems no need to test close, otherwise, a buffer page with tuples less than 
	// its tuple limit will not be flushed to disk.
//	@Test
//	public void testClose(){
//		
//	}
//	
	
	@Test
	public void testMultiPageWrite(){
		BinaryTupleWriter tw = new BinaryTupleWriter(file, cols.length);
		int[] attr;
		Tuple tuple;
		
		attr = new int[]{88,98,67};
		tuple = new Tuple(attr,cols);
		int limit = tw.getTupleLimit();
		for (int i = 0; i < limit; i++){
			tw.writeTuple(tuple);
		}
		
		
		attr = new int[]{40,32,19};
		tuple = new Tuple(attr,cols);
		for (int i = 0; i < limit; i++){
			tw.writeTuple(tuple);
		}
		tw.close();
		
		BinaryTupleReader tr = new BinaryTupleReader(table);
		int number = 1;
		Tuple tup = tr.readNextTuple();
		assertEquals("88,98,67",tup.toString());
		while (tr.readNextTuple() != null){
			number++;
		}
		assertEquals(limit*2,number);
		tr.close();
		
	}

}
