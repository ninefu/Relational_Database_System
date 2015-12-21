package testing;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import database.Table;
import database.Tuple;
import util.ReadableTupleReader;
import util.ReadableTupleWriter;

public class ReadableTupleWriterTester {
	File file = new File("samples/input/db/data","ReadableWriterTestBoats");
	String[] cols = new String[]{"D","E","F"};
	
	String[] des = new String[]{"ReadableWriterTestBoats","D","E","F"};
	Table table = new Table(des,"samples/input/");

	@Test
	public void testWriteTuple() {
		ReadableTupleWriter tw = new ReadableTupleWriter(file);
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
		
		ReadableTupleReader tr = new ReadableTupleReader(table,null);
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
		ReadableTupleWriter tw = new ReadableTupleWriter(file);
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
		
		ReadableTupleReader tr = new ReadableTupleReader(table,null);
		Tuple tup; // for reader
		tup = tr.readNextTuple();
		assertEquals("16,44,68",tup.toString());
		tup = tr.readNextTuple();
		assertEquals(null,tup);
		
		tr.close();
		
	}

}
