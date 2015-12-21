package testing;

import static org.junit.Assert.*;

import org.junit.Test;

import database.Table;
import database.Tuple;
import util.ReadableTupleReader;

public class ReadableTupleReaderTester {
	String[] des = new String[]{"Boats_humanreadable","D","E","F"};
	Table table = new Table(des,"samples/input/");
	
	@Test
	public void testGetNextTuple() {
		ReadableTupleReader tr = new ReadableTupleReader(table,null);
		Tuple tuple;
		
		tuple = tr.readNextTuple();
		assertEquals("7811,4505,5766",tuple.toString());
		
		tuple = tr.readNextTuple();
		assertEquals("3014,9170,4417",tuple.toString());
		
		tuple = tr.readNextTuple();
		assertEquals("9536,1220,6424",tuple.toString());
		
		tuple = tr.readNextTuple();
		assertEquals("4534,7369,4434",tuple.toString());
		
		tr.close();
	}
	
	@Test
	public void testReset(){
		ReadableTupleReader tr = new ReadableTupleReader(table,null);
		Tuple tuple;
		
		tuple = tr.readNextTuple();
		assertEquals("7811,4505,5766",tuple.toString());
		
		tuple = tr.readNextTuple();
		assertEquals("3014,9170,4417",tuple.toString());
		
		tuple = tr.readNextTuple();
		assertEquals("9536,1220,6424",tuple.toString());
		
		tr.reset();
		tuple = tr.readNextTuple();
		assertEquals("7811,4505,5766",tuple.toString());
		tr.close();
	}
	
	// need to work on this
//	@Test
//	public void testClose(){
//		ReadableTupleReader tr = new ReadableTupleReader(table,null);
//		tr.close();
//		Tuple tuple = tr.readNextTuple();
//		assertEquals(null,tuple);
//	}

	@Test
	public void testResetIndex(){
		ReadableTupleReader tr = new ReadableTupleReader(table,null);
		Tuple tuple;
		
		tuple = tr.readNextTuple();
		assertEquals("7811,4505,5766",tuple.toString());
		
		tuple = tr.readNextTuple();
		assertEquals("3014,9170,4417",tuple.toString());
		
		tuple = tr.readNextTuple();
		assertEquals("9536,1220,6424",tuple.toString());
		
		tr.reset(2);
		tuple = tr.readNextTuple();
		assertEquals("9536,1220,6424",tuple.toString());
		
		tr.reset(326);
		tuple = tr.readNextTuple();
		assertEquals("7720,558,3276",tuple.toString());
		
		tr.reset(9999);
		tuple = tr.readNextTuple();
		assertEquals("6954,1383,8384",tuple.toString());
		
		tr.reset(10000);
		tuple = tr.readNextTuple();
		assertEquals(null,tuple);
		tr.close();
	}
}
