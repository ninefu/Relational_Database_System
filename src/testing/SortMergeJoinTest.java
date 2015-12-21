package testing;

import org.junit.Test;

import physicalOperator.PhysicalOperator;
import physicalOperator.PhysicalPlanConfig;
import physicalOperator.PhysicalPlanConfig.JoinMethod;
import physicalOperator.PhysicalPlanConfig.SortMethod;

public class SortMergeJoinTest extends BaseDatabaseTester{
	public static boolean HR = true; // This flag determines whether files will generated/output in human-readable format or in binary.
	public static boolean IN = false;// This flag determines whether use index or not
	
	private void compareToTNLJ(String query, int pages) {
		PhysicalPlanConfig smjConf = new PhysicalPlanConfig(JoinMethod.SMJ, SortMethod.MEMORY, HR,IN); // Create the configuration file so that we will use BNLJ as the join method.
		PhysicalPlanConfig tnljConf = new PhysicalPlanConfig(JoinMethod.TNLJ, SortMethod.MEMORY, HR,IN); // Compare it to our implementation of join using TNLJ as the join method.
		
		PhysicalOperator smjOp = getPhysicalPlan(query, smjConf,null);
		PhysicalOperator tnljOp = getPhysicalPlan(query, tnljConf,null);
		dumpSortCompare(HR, tnljOp, smjOp);
	}

	@Test
	public void testAgainstTupleNestLoopJoinBasic() {
		setupDatabase(HR, 10); // Sets up a database in HR format with tuples that have a 1% chance of matching.
		String col1 = t1.getSchema()[0];
		String col2 = t2.getSchema()[0];
		// Something like "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G"
		String query = String.format("SELECT * FROM %s, %s WHERE %s.%s = %s.%s", 
										t1.getName(), t2.getName(), t1.getName(), col1, t2.getName(), col2);
		compareToTNLJ(query, 1);
	}
	
	@Test
	public void test3BufferPages() {
		setupDatabase(HR, 10);
		String col1 = t1.getSchema()[0];
		String col2 = t2.getSchema()[0];
		// Something like "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G"
		String query = String.format("SELECT * FROM %s, %s WHERE %s.%s = %s.%s", 
										t1.getName(), t2.getName(), t1.getName(), col1, t2.getName(), col2);
		compareToTNLJ(query, 3);
	}
	
	@Test
	public void testAlias() {
		setupDatabase(HR, 10);
		String col1 = t1.getSchema()[0];
		String col2 = t2.getSchema()[0];
		// Something like "SELECT * FROM Sailors S, Reserves R WHERE S.A = R.G"
		String query = String.format("SELECT * FROM %s S, %s R WHERE S.%s = R.%s", 
										t1.getName(), t2.getName(), col1, col2);
		System.out.println(query);
		compareToTNLJ(query, 1);	
	}
	
	@Test
	public void testSelfJoinAlias() {
		setupDatabase(HR, 10);
		String col1 = t1.getSchema()[0];
		String col2 = t1.getSchema()[1];
		// Something like "SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A = S2.A"
		String query = String.format("SELECT * FROM %s S1, %s S2 WHERE S1.%s = S2.%s", 
										t1.getName(), t1.getName(), col1, col2);
		System.out.println(query);
		compareToTNLJ(query, 1);
	}
	
	@Test
	public void testAliasJoinSelect() {
		setupDatabase(HR, 10);
		String col1 = t1.getSchema()[0];
		String col2 = t1.getSchema()[1];
		String col3 = t2.getSchema()[0];
		// Something like "SELECT * FROM Reserves AS R, Sailors AS S WHERE R.G < R.H AND S.A = 1 AND R.G = S.A"
		String query = String.format("SELECT * FROM %s AS R, %s AS S WHERE R.%s < R.%s AND S.%s = 1 AND R.%s = S.%s", 
										t1.getName(), t2.getName(), col1, col2, col3, col1, col3);
		System.out.println(query);
		compareToTNLJ(query, 1);
	}
}
