package testing;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.junit.Test;

import database.Tuple;
import index.BTreeIndex;
import index.IndexConfig;
import physicalOperator.IndexScanOperator;
import physicalOperator.PhysicalOperator;
import physicalOperator.PhysicalPlanConfig;
import physicalOperator.PhysicalPlanConfig.JoinMethod;
import physicalOperator.PhysicalPlanConfig.SortMethod;
import util.BinaryTupleReader;
import util.RandomGenerator;

public class BTreeTester extends BaseDatabaseTester {
	
	public void setupDatabase(boolean humanReadable, int numTables, int numRelations, int numAttributes, double percentMatches) {
		rg = new RandomGenerator(numTables, numRelations, numAttributes, (int)(100/percentMatches));
		db = rg.getDatabase(humanReadable);	
		t1 = getTableAt(0);
	}
	
	class TupleComparator implements Comparator<Tuple> {
		String[] columnOrder;
		
		public TupleComparator(String[] order) {
			columnOrder = order;
		}

		@Override
		public int compare(Tuple o1, Tuple o2) {
			for(String o : columnOrder) {
				int v1 = o1.getValueAtField(o);
				int v2 = o2.getValueAtField(o);
				int ret = v1-v2;
				if(ret != 0) {
					return ret;
				}
			}
			return 0;
		}		
	}
	
	@Test
	public void testBasicUnclustered() {
		int order = 3;
		int clustered = 0;
		setupDatabase(false, 1, 20, 3, 5);
		
		String col = String.format("%s.%s", t1.getName(), t1.getSchema()[1]);
		IndexConfig ic = new IndexConfig(db, t1, col, clustered, order);
		BTreeIndex index = ic.getIndex(t1.getName()).get(t1.getSchema()[1]);
		index.serialize();
		String before = index.toString();
		index.deserialize();
		String after = index.toString();
		System.out.println(before);
		System.out.println(after);
		assertEquals(before, after);
	}
	
	@Test
	public void testBasicClustered() {
		int order = 3;
		int clustered = 1;
		setupDatabase(false, 1, 20, 3, 5);
		
		String col = String.format("%s.%s", t1.getName(), t1.getSchema()[1]);
		IndexConfig ic = new IndexConfig(db, t1, col, clustered, order);
		BTreeIndex index = ic.getIndex(t1.getName()).get(t1.getSchema()[1]);
		index.serialize();
		String before = index.toString();
		index.deserialize();
		String after = index.toString();
		System.out.println(before);
		System.out.println(after);
		assertEquals(before, after);
		
		// Make sure that the original data file got sorted 
		BinaryTupleReader tr = new BinaryTupleReader(t1);
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		Tuple tup;
		while((tup = tr.readNextTuple()) != null) {
			tuples.add(tup);
		}
		Collections.sort(tuples, new TupleComparator(new String[] {col}));
		tr.reset();
		int ind = 0;
		while((tup = tr.readNextTuple()) != null) {
			assertEquals(tup, tuples.get(ind));
			ind++;
		}
	}
	
	@Test
	public void testSeveralLayers() {
		int order = 2;
		int clustered = 0;
		setupDatabase(false, 1, 100, 2, 1);
		
		String col = String.format("%s.%s", t1.getName(), t1.getSchema()[1]);
		IndexConfig ic = new IndexConfig(db, t1, col, clustered, order);
		BTreeIndex index = ic.getIndex(t1.getName()).get(t1.getSchema()[1]);
		index.serialize();
		String before = index.toString();
		index.deserialize();
		String after = index.toString();
		System.out.println(before);
		System.out.println(after);
		assertEquals(before, after);
	}
	
	@Test
	public void testVeryUnderfull() {
		int order = 5;
		int clustered = 0;
		setupDatabase(false, 1, 20, 2, 10);
		
		String col = String.format("%s.%s", t1.getName(), t1.getSchema()[1]);
		IndexConfig ic = new IndexConfig(db, t1, col, clustered, order);
		BTreeIndex index = ic.getIndex(t1.getName()).get(t1.getSchema()[1]);
		index.serialize();
		String before = index.toString();
		String[] lines = before.split("\n");
		assertTrue(lines.length == 2); // The header line and the line for the leaves
		index.deserialize();
		String after = index.toString();
		System.out.println(before);
		System.out.println(after);
		assertEquals(before, after);
	}

}
