package testing;

import org.junit.Ignore;
import org.junit.Test;

import database.DatabaseCatalog;
import database.Table;
import index.IndexConfig;
import physicalOperator.PhysicalOperator;
import physicalOperator.PhysicalPlanConfig;
import physicalOperator.PhysicalPlanConfig.JoinMethod;
import physicalOperator.PhysicalPlanConfig.SortMethod;
import util.RandomGenerator;

public class PerformanceTester extends BaseDatabaseTester {
	public static boolean HR = false;
	
	//numTables = 2
	public void setupDatabase(boolean humanReadable, int numTables, int numTuples, int numAttributes, double percentMatches) {
		rg = new RandomGenerator(numTables, numTuples, numAttributes, (int)(100/percentMatches));
		db = rg.getDatabase(humanReadable);	
		t1 = getTableAt(0);
		t2 = getTableAt(1);
	}
	
	//for Project 4
	private void timeFullScan(String query) {
		PhysicalPlanConfig full_conf = new PhysicalPlanConfig(JoinMethod.TNLJ,SortMethod.MEMORY, HR,false);
		PhysicalOperator full = getPhysicalPlan(query, full_conf,null);
		System.out.println(String.format("Time for full scan on query %s: %d%n", query, full.dump()));
	}
	
	private void timeUnclustered(String query,int indexCount) {
		int order = 3;
		int clustered = 0;
		IndexConfig ic;
		//indexCount equals to 1 or 2
		if (indexCount == 1){
			String col = String.format("%s.%s", t1.getName(), t1.getSchema()[1]);
			ic = new IndexConfig(db,t1,col,clustered,order);
		}else{
			String col1 = String.format("%s.%s", t1.getName(), t1.getSchema()[1]);
			String col2 = String.format("%s.%s", t2.getName(), t2.getSchema()[0]);
			ic = new IndexConfig(db,t1,t2,col1,col2,clustered,clustered,order,order);
		}
				
		PhysicalPlanConfig un_conf = new PhysicalPlanConfig(JoinMethod.TNLJ,SortMethod.MEMORY, HR,true);
		PhysicalOperator full = getPhysicalPlan(query, un_conf,ic);
		System.out.println(String.format("Time for unclustered index on query %s: %d%n", query, full.dump()));
	}
	
	private void timeClustered(String query,int indexCount) {
		int order = 3;
		int clustered = 1;
		IndexConfig ic;
		if (indexCount == 1){
			String col = String.format("%s.%s", t1.getName(), t1.getSchema()[1]);
			ic = new IndexConfig(db,t1,col,clustered,order);
		}else{
			String col1 = String.format("%s.%s", t1.getName(), t1.getSchema()[1]);
			String col2 = String.format("%s.%s", t2.getName(), t2.getSchema()[0]);
			ic = new IndexConfig(db,t1,t2,col1,col2,clustered,clustered,order,order);
		}
		
		PhysicalPlanConfig clus_conf = new PhysicalPlanConfig(JoinMethod.TNLJ,SortMethod.MEMORY, HR,true);
		PhysicalOperator full = getPhysicalPlan(query, clus_conf,ic);
		System.out.println(String.format("Time for clustered index on query %s: %d%n", query, full.dump()));
	}
	
	@Test
	public void query1() {
		setupDatabase(HR, 2, 5000, 3, 0.1);
		String col1 = t1.getSchema()[1];
		// Something like "SELECT * FROM Sailors WHERE Sailors.A < 5"
		String query = String.format("SELECT * FROM %s WHERE %s.%s < 5",t1.getName(),t1.getName(),col1);
		timeFullScan(query);
		timeUnclustered(query,1);
		timeClustered(query,1);
	}
	
	
	@Test
	public void query2() {
		setupDatabase(HR, 2, 5000, 3, 0.1);
		String col1 = t1.getSchema()[1]; //the one with an index on col1
		String col2 = t2.getSchema()[0];
//		 Something like "SELECT * FROM Reserves AS R, Sailors AS S WHERE S.A = 1 AND R.G < 5"
//		String query = String.format("SELECT * FROM %s AS R, %s AS S WHERE R.%s = 1 AND S.%s < 5", 
//										t2.getName(), t1.getName(), col3, col1); 
		String query = String.format("SELECT * FROM %s AS S, %s AS R WHERE S.%s < 5 AND R.%s = 1", t1.getName(),t2.getName(),col1,col2);
		timeFullScan(query);
		timeUnclustered(query,1);
		timeClustered(query,1);
	}
	
	@Test
	public void query3() {
		setupDatabase(HR, 2, 5000, 3, 0.1);
		String col1 = t1.getSchema()[1];
		String col2 = t2.getSchema()[0];
		// Something like "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G ADN Sailors.A > 42"
//		String query = String.format("SELECT * FROM %s, %s WHERE %s.%s = %s.%s ADN %s.%s > 42", 
//										t1.getName(), t2.getName(), t1.getName(), col1, t2.getName(), col2, t1.getName(),col1);
		String query = String.format("SELECT * FROM %s AS S, %s AS R WHERE S.%s < 5 AND R.%s = 1", t1.getName(),t2.getName(),col1,col2);
		timeFullScan(query);
		timeUnclustered(query,2);
		timeClustered(query,2);
	}
	
	@Test
	public void query4() {
		setupDatabase(HR, 2, 5000, 3, 0.1);
		String col = t1.getSchema()[1];
		String query = String.format("SELECT * FROM %s AS S, %s AS R WHERE S.%s < 5 AND S.%s > 1",t1.getName(),t2.getName(),col,col);
		timeFullScan(query);
		timeUnclustered(query,1);
		timeClustered(query,1);
	}
	
	@Test
	public void query5(){
		setupDatabase(HR, 2, 5000, 3, 0.1);
		String col1 = t1.getSchema()[1];
		String col2 = t2.getSchema()[0];
//		String col3 = t1.getSchema()[1];
		// Something like "SELECT * FROM Reserves AS R, Sailors AS S WHERE S.A > 7 AND R.G = S.A"
		String query = String.format("SELECT * FROM %s AS S, %s AS R WHERE S.%s <= 7 AND R.%s < 5", 
										t1.getName(), t2.getName(), col1, col2);
		timeFullScan(query);
		timeUnclustered(query,1);
		timeClustered(query,1);
	}
	
	@Test
	public void query6(){
		setupDatabase(HR, 2, 5000, 3, 0.1);
		String col1 = t1.getSchema()[1];
		String col2 = t2.getSchema()[0];
//		String col3 = t1.getSchema()[1];
		// Something like "SELECT * FROM Reserves AS R, Sailors AS S WHERE S.A > 7 AND R.G = S.A"
		String query = String.format("SELECT * FROM %s AS S, %s AS R WHERE S.%s <= 7 AND R.%s < 5", 
										t1.getName(), t2.getName(), col1, col2);
		timeFullScan(query);
		timeUnclustered(query,2);
		timeClustered(query,2);
	}
	
}
