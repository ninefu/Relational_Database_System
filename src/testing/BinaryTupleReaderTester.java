package testing;



import static org.junit.Assert.*;

import org.junit.Test;

import database.Table;
import database.Tuple;
import database.TupleIdentifier;
import util.BinaryTupleReader;

public class BinaryTupleReaderTester {
	String[] des = new String[]{"Boats","D","E","F"};
	Table table = new Table(des,"samples/input/");
	
	@Test
	public void testGetAttrNumber() {
		BinaryTupleReader tr = new BinaryTupleReader(table);
		int attr = tr.getAttributeNumber();
		assertEquals(3,attr);
		tr.close();
	}
	
	@Test
	public void testGetTotalTupleNumber(){
		BinaryTupleReader tr = new BinaryTupleReader(table);
		int number = 0;
		while (tr.readNextTuple() != null){
			number++;
		}
		assertEquals(10000,number);
		tr.close();
	}
	
	@Test
	public void testReadNextTupel(){
		BinaryTupleReader tr = new BinaryTupleReader(table);
		Tuple tuple;
		
		assertEquals(0,tr.getCurrentPageNumber());
		assertEquals(0,tr.getCurrentTupleNumber());
		tuple = tr.readNextTuple();
		assertEquals("7811,4505,5766",tuple.toString());
		
		assertEquals(0,tr.getCurrentPageNumber());
		assertEquals(1,tr.getCurrentTupleNumber());
		tuple = tr.readNextTuple();
		assertEquals("3014,9170,4417",tuple.toString());
		
		assertEquals(0,tr.getCurrentPageNumber());
		assertEquals(2,tr.getCurrentTupleNumber());
		tuple = tr.readNextTuple();
		assertEquals("9536,1220,6424",tuple.toString());
		
		assertEquals(0,tr.getCurrentPageNumber());
		assertEquals(3,tr.getCurrentTupleNumber());
		tuple = tr.readNextTuple();
		assertEquals("4534,7369,4434",tuple.toString());
		
		tr.close();
	}
	
	@Test
	public void testReset(){
		BinaryTupleReader tr = new BinaryTupleReader(table);
		Tuple tuple;
		
		tuple = tr.readNextTuple();
		assertEquals("7811,4505,5766",tuple.toString());
		
		tuple = tr.readNextTuple();
		assertEquals("3014,9170,4417",tuple.toString());
		
		tuple = tr.readNextTuple();
		assertEquals("9536,1220,6424",tuple.toString());
		
		tuple = tr.readNextTuple();
		assertEquals("4534,7369,4434",tuple.toString());
		
		tr.reset();
		assertEquals(0,tr.getCurrentPageNumber());
		assertEquals(0,tr.getCurrentTupleNumber());
		tuple = tr.readNextTuple();
		assertEquals("7811,4505,5766",tuple.toString());
		tr.close();
	}
	
	@Test
	public void testResetIndex(){
		BinaryTupleReader tr = new BinaryTupleReader(table);
		Tuple tuple;
		
		tuple = tr.readNextTuple();
		assertEquals("7811,4505,5766",tuple.toString());
		
		tuple = tr.readNextTuple();
		assertEquals("3014,9170,4417",tuple.toString());
		
		tuple = tr.readNextTuple();
		assertEquals("9536,1220,6424",tuple.toString());
		
		tr.reset(2);
		assertEquals(0,tr.getCurrentPageNumber());
		assertEquals(2,tr.getCurrentTupleNumber());
		tuple = tr.readNextTuple();
		assertEquals("9536,1220,6424",tuple.toString());
		
		tr.reset(326);
		assertEquals(0,tr.getCurrentPageNumber());
		assertEquals(326,tr.getCurrentTupleNumber());
		tuple = tr.readNextTuple();
		assertEquals("7720,558,3276",tuple.toString());
		
		tr.reset(400);
		assertEquals(1,tr.getCurrentPageNumber());
		assertEquals(60,tr.getCurrentTupleNumber());
		tuple = tr.readNextTuple();
		assertEquals("4240,9074,2363",tuple.toString());
		
		tr.reset(9999);
		assertEquals(29,tr.getCurrentPageNumber());
		assertEquals(139,tr.getCurrentTupleNumber());
		tuple = tr.readNextTuple();
		assertEquals("6954,1383,8384",tuple.toString());
		
		tr.reset(10000);
		assertEquals(29,tr.getCurrentPageNumber());
		assertEquals(140,tr.getCurrentTupleNumber());
		try {
			tuple = tr.readNextTuple();
		}catch (NullPointerException e){
			System.out.println("Reaches the end of the file");
		}

		tr.close();
	}
	
	@Test
	public void testReadAt(){
		BinaryTupleReader tr = new BinaryTupleReader(table);
		Tuple tuple;
		
		TupleIdentifier ti = new TupleIdentifier(0,0);
		tuple = tr.readAt(ti);
		assertEquals("7811,4505,5766",tuple.toString());
		
		assertEquals(0,tr.getCurrentPageNumber());
		assertEquals(1,tr.getCurrentTupleNumber());
		tuple = tr.readNextTuple();
		assertEquals("3014,9170,4417",tuple.toString());

		ti = new TupleIdentifier(1,0);
		tuple = tr.readAt(ti);
		assertEquals("7250,3182,3224",tuple.toString());
		assertEquals(1,tr.getCurrentPageNumber());
		assertEquals(1,tr.getCurrentTupleNumber());
		
		tuple = tr.readNextTuple();
		assertEquals("2997,5211,1492",tuple.toString());
	}
}
