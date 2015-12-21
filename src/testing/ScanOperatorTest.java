/**
 * 
 */
package testing;

import static org.junit.Assert.*;

import org.junit.Test;

import database.Table;
import database.Tuple;
import physicalOperator.ScanOperator;
import physicalOperator.SequentialScanOperator;

/**
 * @author yihuifu
 *
 */
public class ScanOperatorTest {

	@Test
	public void testGetNextTuple() {
		Table table = new Table(new String[]{"Sailors", "A", "B", "C"}, "samples/input/");
		String[] header = new String[]{"Sailors.A", "Sailors.B", "Sailors.C"};
		ScanOperator so = new SequentialScanOperator(table, null);
		Tuple current = so.getNextTuple();
		Tuple target = new Tuple(new String[]{"2228","3083","8277"}, header);
		assertEquals(target.toString(),current.toString());
		
		current = so.getNextTuple();
		target = new Tuple(new String[]{"5354","743","6031"}, header);
		assertEquals(target.toString(),current.toString());
		
		current = so.getNextTuple();
		target = new Tuple(new String[]{"7495","298","9221"}, header);
		assertEquals(target.toString(),current.toString());
		
		current = so.getNextTuple();
		target = new Tuple(new String[]{"1884","337","4632"}, header);
		assertEquals(target.toString(),current.toString());
		
		current = so.getNextTuple();
		target = new Tuple(new String[]{"7494","8450","5172"}, header);
		assertEquals(target.toString(),current.toString());
		
		current = so.getNextTuple();
		target = new Tuple(new String[]{"2540","5492","4298"}, header);
		assertEquals(target.toString(),current.toString());
	}
	
	@Test
	public void testReset(){
		Table table = new Table(new String[]{"Sailors", "A", "B", "C"}, "samples/input/");
		String[] header = new String[]{"Sailors.A", "Sailors.B", "Sailors.C"};
		ScanOperator so = new SequentialScanOperator(table, null);
		Tuple current = so.getNextTuple();
		Tuple target = new Tuple(new String[]{"2228","3083","8277"}, header);
		assertEquals(target.toString(),current.toString());
		
		current = so.getNextTuple();
		target = new Tuple(new String[]{"5354","743","6031"}, header);
		assertEquals(target.toString(),current.toString());
		
		current = so.getNextTuple();
		target = new Tuple(new String[]{"7495","298","9221"}, header);
		assertEquals(target.toString(),current.toString());
		
		so.reset();
		current = so.getNextTuple();
		target = new Tuple(new String[]{"2228","3083","8277"}, header);
		assertEquals(target.toString(),current.toString());	
	}
	
	@Test
	public void testTuplesHaveCorrectHeaders() {
		Table table = new Table(new String[]{"Sailors", "A", "B", "C"}, "samples/input/");
		String alias = "SailorsAlias";
		String[] header = new String[]{"SailorsAlias.A", "SailorsAlias.B", "SailorsAlias.C"};
		ScanOperator so = new SequentialScanOperator(table, alias);
		Tuple current = so.getNextTuple();
		Tuple target = new Tuple(new String[]{"2228","3083","8277"}, header);
		assertTrue(current.equals(target));
	}
	
	

}
