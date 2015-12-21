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
import util.RandomGenerator;

public class IndexScanOperatorTester extends BaseDatabaseTester {

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
			for (String i : o1.getFields()){
				System.out.println("Fields " + i);
			}
			for(String o : columnOrder) {
				System.out.println(o);
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
	public void testIndexScanner() {
		int order = 2;
		int clustered = 0;
		setupDatabase(false, 1, 100, 2, 10);
		int low = 6;
		int high = 9;
		
		String col = String.format("%s.%s", t1.getName(), t1.getSchema()[1]);
		IndexConfig ic = new IndexConfig(db, t1, col, clustered, order);
		BTreeIndex index = ic.getIndex(t1.getName()).get(t1.getSchema()[1]);
		index.serialize();
		
		PhysicalPlanConfig conf = new PhysicalPlanConfig(JoinMethod.BNLJ, SortMethod.MEMORY, false, false);
		String query = String.format("SELECT * from %s WHERE %s >= %d AND %s <= %d", 
										t1.getName(), col, low, col, high);
		System.out.println(query);
		PhysicalOperator scanner = getPhysicalPlan(query, conf,null);	
		PhysicalOperator indexScanner = new IndexScanOperator(t1, null, low, high, ic);
		ArrayList<Tuple> scannedTuples = new ArrayList<Tuple>();
		ArrayList<Tuple> indexScannedTuples = new ArrayList<Tuple>();
		Tuple tup;
		while((tup = indexScanner.getNextTuple()) != null) {
			indexScannedTuples.add(tup);
		}
		while((tup = scanner.getNextTuple()) != null) {
			scannedTuples.add(tup);
		}
		Collections.sort(scannedTuples, new TupleComparator(new String[] {col}));
		int ind = 0;
		for(Tuple t : scannedTuples) {
			assertEquals(t, indexScannedTuples.get(ind));
			ind++;
		}
	}
	
	
	@Test
	public void testIndexScannerLT() {
		int order = 2;
		int clustered = 0;
		setupDatabase(false, 1, 100, 2, 10);
		Integer low = 6;
		Integer high = null;;
		
		String col = String.format("%s.%s", t1.getName(), t1.getSchema()[1]);
		IndexConfig ic = new IndexConfig(db, t1, col, clustered, order);
		BTreeIndex index = ic.getIndex(t1.getName()).get(t1.getSchema()[1]);
		index.serialize();
		
		PhysicalPlanConfig conf = new PhysicalPlanConfig(JoinMethod.BNLJ, SortMethod.MEMORY, false, false);
		String query = String.format("SELECT * from %s WHERE %s >= %d", 
										t1.getName(), col, low);
		System.out.println(query);
		PhysicalOperator scanner = getPhysicalPlan(query, conf,null);	
		PhysicalOperator indexScanner = new IndexScanOperator(t1, null, low, high, ic);
		ArrayList<Tuple> scannedTuples = new ArrayList<Tuple>();
		ArrayList<Tuple> indexScannedTuples = new ArrayList<Tuple>();
		Tuple tup;
		while((tup = indexScanner.getNextTuple()) != null) {
			indexScannedTuples.add(tup);
		}
		while((tup = scanner.getNextTuple()) != null) {
			scannedTuples.add(tup);
		}
		Collections.sort(scannedTuples, new TupleComparator(new String[] {col}));
		int ind = 0;
		for(Tuple t : scannedTuples) {
			assertEquals(t, indexScannedTuples.get(ind));
			ind++;
		}
	}
	
	@Test
	public void testIndexScannerGT() {
		int order = 2;
		int clustered = 0;
		setupDatabase(false, 1, 100, 2, 10);
		Integer low = null;
		Integer high = 9;
		
		String col = String.format("%s.%s", t1.getName(), t1.getSchema()[1]);
		IndexConfig ic = new IndexConfig(db, t1, col, clustered, order);
		BTreeIndex index = ic.getIndex(t1.getName()).get(t1.getSchema()[1]);
		index.serialize();
		
		PhysicalPlanConfig conf = new PhysicalPlanConfig(JoinMethod.BNLJ, SortMethod.MEMORY, false, false);
		String query = String.format("SELECT * from %s WHERE %s <= %d", 
										t1.getName(), col, high);
		System.out.println(query);
		PhysicalOperator scanner = getPhysicalPlan(query, conf,null);	
		PhysicalOperator indexScanner = new IndexScanOperator(t1, null, low, high, ic);
		ArrayList<Tuple> scannedTuples = new ArrayList<Tuple>();
		ArrayList<Tuple> indexScannedTuples = new ArrayList<Tuple>();
		Tuple tup;
		while((tup = indexScanner.getNextTuple()) != null) {
			indexScannedTuples.add(tup);
		}
		while((tup = scanner.getNextTuple()) != null) {
			scannedTuples.add(tup);
		}
		Collections.sort(scannedTuples, new TupleComparator(new String[] {col}));
		int ind = 0;
		for(Tuple t : scannedTuples) {
			assertEquals(t, indexScannedTuples.get(ind));
			ind++;
		}
	}
	
//	@Test
//	public void testIndexScannerGTAlias() {
//		int order = 2;
//		int clustered = 0;
//		setupDatabase(false, 1, 100, 2, 10);
//		Integer low = null;
//		Integer high = 9;
//		
//		String col_full = String.format("%s.%s", t1.getName(), t1.getSchema()[1]);
//		String col = t1.getSchema()[1];
//		System.out.println(col);
//		IndexConfig ic = new IndexConfig(db, t1, col_full, clustered, order);
//		BTreeIndex index = ic.getIndex(t1.getName());
//		index.serialize();
//		
//		PhysicalPlanConfig conf = new PhysicalPlanConfig(JoinMethod.BNLJ, SortMethod.MEMORY, false, false);
//		String query = String.format("SELECT * from %s AS S WHERE S.%s <= %d", 
//										t1.getName(), col, high);
//		System.out.println(query);
//		PhysicalOperator scanner = getPhysicalPlan(query, conf,null);	
//		PhysicalOperator indexScanner = new IndexScanOperator(t1, "S", low, high, ic);
//		ArrayList<Tuple> scannedTuples = new ArrayList<Tuple>();
//		ArrayList<Tuple> indexScannedTuples = new ArrayList<Tuple>();
//		Tuple tup;
//		while((tup = indexScanner.getNextTuple()) != null) {
//			indexScannedTuples.add(tup);
//		}
//		while((tup = scanner.getNextTuple()) != null) {
//			scannedTuples.add(tup);
//		}
//		Collections.sort(scannedTuples, new TupleComparator(new String[] {col}));
//		int ind = 0;
//		for(Tuple t : scannedTuples) {
//			assertEquals(t.toString(), indexScannedTuples.get(ind).toString());
//			ind++;
//		}
//	}
}
