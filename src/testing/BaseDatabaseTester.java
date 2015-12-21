package testing;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.BeforeClass;
import static org.junit.Assert.*;

import database.DatabaseCatalog;
import database.Table;
import database.Tuple;
import index.IndexConfig;
import logicalOperator.LogicalOperator;
import logicalOperator.LogicalPlanBuilder;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import physicalOperator.PhysicalOperator;
import physicalOperator.PhysicalPlanBuilder;
import physicalOperator.PhysicalPlanConfig;
import util.BinaryTupleReader;
import util.BinaryTupleWriter;
import util.Converter;
import util.RandomGenerator;
import util.ReadableTupleReader;
import util.ReadableTupleWriter;
import util.TupleReader;
import util.TupleWriter;
import util.Util;

public abstract class BaseDatabaseTester {
	
	protected RandomGenerator rg;
	protected static Util util;
	protected DatabaseCatalog db;
	protected Table t1;
	protected Table t2;
	
	@BeforeClass
	public static void setUp() {
		util = new Util();
	}
	
	/**
	 * A way to get different tables for generating queries.
	 * @param index The nth table you would like to get.
	 * @return The table at that index, order decided by
	 * the toArray() method (should not matter unless the
	 * contents of the database has changed, which it shouldn't)
	 */
	protected Table getTableAt(int index) {
		Map<String, Table> tables = db.getAllTable();
		String[] names = new String[tables.keySet().size()];
		tables.keySet().toArray(names);
		return tables.get(names[index]);
	}
	
	/**
	 * Parses the given query and returns the resulting logical plan.
	 * @param query Query to parse
	 * @return LogicalOperator that is the root node of the logical plan.
	 */
	protected LogicalOperator getLogicalPlan(String query) {
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8)));
		try {
			PlainSelect ps = parser.PlainSelect();
			LogicalPlanBuilder lpb = new LogicalPlanBuilder(db, ps);
			return lpb.getFinalOperator();
		} catch (ParseException e) {
			e.printStackTrace();
			throw new RuntimeException("Error parsing query.");
		}
	}

	/**
	 * Parses the given query and returns the resulting physical plan,
	 * using the given physical plan config object.
	 * @param query Query to parse
	 * @param conf Configuration object that details the methods to use for
	 * 				sorting and joining
	 * @return PhysicalOperator that is the root node of the physical plan.
	 */
	protected PhysicalOperator getPhysicalPlan(String query, PhysicalPlanConfig conf, IndexConfig inConf) {
		LogicalOperator lplan = getLogicalPlan(query);
		PhysicalPlanBuilder ppb = new PhysicalPlanBuilder(conf,inConf);
		lplan.accept(ppb);
		return ppb.getPlan();
	}
	
	/**
	 * Given the appropriate readers, read tuples one by one
	 * and compare them. 
	 * @param tr1 Reader for expected set of tuples
	 * @param tr2 Reader for set of tuples we are testing
	 */
	protected void compareFiles(TupleReader tr1, TupleReader tr2) {
		Tuple t1 = tr1.readNextTuple();
		Tuple t2 = tr2.readNextTuple();
		int line = 0;
		while(t1 != null) {
			assertEquals(String.format("Disagreed at tuple %d", line), t1, t2);
			t1 = tr1.readNextTuple();
			t2 = tr2.readNextTuple();	
			line++;
		}
		assertNull(tr2.readNextTuple());
	}
	
	/**
	 * Given two physical plans, call getNextTuple() in parallel and compare
	 * each tuple.
	 * @param humanReadable Whether the files should be written out in human
	 * 						readable format or not/
	 * @param expected The query plan to test AGAINST
	 * @param totest The query plan we are testing
	 */
	protected void dumpCompare(boolean humanReadable, PhysicalOperator expected, PhysicalOperator totest) {
		Tuple e;
		Tuple t;
		int line = 0;
		while((e = expected.getNextTuple()) != null) {
			t = totest.getNextTuple();
			assertNotNull("Did not generate the number of tuples expected.", t);
			assertEquals(String.format("Disagreed at tuple %d", line), e, t);
			line++;
		}
		assertNull("Test operator had more tuples than expected.",totest.getNextTuple());
	}
	
	/**
	 * Given two physical plans, write the output of both to a file,
	 * sort those two files, and then compare the resulting tuples
	 * in the two files.
	 * @param humanReadable Whether the files should be written out in human
	 * 						readable format or not/
	 * @param expected The query plan to test AGAINST
	 * @param totest The query plan we are testing
	 */
	protected void dumpSortCompare(boolean humanReadable, PhysicalOperator expected, PhysicalOperator totest) {
		Tuple t = expected.getNextTuple();
		String[] schema = t.getFields();
		expected.reset();
		File srcFile1 = new File("expected_output");
		File srcFile2 = new File("totest_output");
		TupleWriter tw1;
		TupleWriter tw2;
		if(humanReadable) {
			tw1 = new ReadableTupleWriter(srcFile1);
			tw2 = new ReadableTupleWriter(srcFile2);
		} else {
			tw1 = new BinaryTupleWriter(srcFile1, schema.length);
			tw2 = new BinaryTupleWriter(srcFile2, schema.length);
		}
		expected.dump(tw1);
		totest.dump(tw2);
		tw1.close();
		tw2.close();
		File dstFile1 = new File("expected_output_sorted");
		File dstFile2 = new File("totest_output_sorted");
		util.sortFile(humanReadable, srcFile1, dstFile1, schema);
		util.sortFile(humanReadable, srcFile2, dstFile2, schema);
		TupleReader tr1;
		TupleReader tr2;
		if(humanReadable) {
			tr1 = new ReadableTupleReader(dstFile1, schema);
			tr2 = new ReadableTupleReader(dstFile2, schema);
		} else {
			tr1 = new BinaryTupleReader(dstFile1, schema);
			tr2 = new BinaryTupleReader(dstFile2, schema);
		}
		compareFiles(tr1, tr2);	
		tr1.close();
		tr2.close();
	}
	
//	public void testAgainstCases(String testCaseDir, String testCase, boolean order) {
//		testAgainstCases(testCaseDir, testCase, -1, order);
//	}
	
//	public void testAgainstCases(String testCaseDir, String testCase, int targetQuery, boolean orderDependent) {
//		String inputDir = String.format("%s/%s/input", testCaseDir, testCase);
//		String outputDir = String.format("%s/%s/output", testCaseDir, testCase);
//		String tempDir = String.format("%s/%s/temp", testCaseDir, testCase);
//		DatabaseCatalog db = new DatabaseCatalog(inputDir, outputDir, tempDir); 
//		//read the query
//		String queriesFile = db.getQueryDir();
//		File expectedDir = new File(testCaseDir, "../expected");
//
//		try {
//			//parse it using JSqlParser
//			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
//			Statement statement;
//			int queryCount = 0;
//			while ((statement = parser.Statement()) != null) {
//				String q = statement.toString();
//				try {
//					queryCount++;
//					if(targetQuery == -1 || targetQuery == queryCount) {
//						System.out.println(String.format("query%d: %s", queryCount, q));
//						// Write out expected in human readable so we can see what went wrong
//						File hrFile = new File(expectedDir, String.format("query%d_humanreadable",queryCount));
//						File sortedFile = new File(expectedDir, String.format("query%d_sorted",queryCount));
//						File bnFile = new File(expectedDir, String.format("query%d",queryCount));
//	
//						Select select = (Select) statement;
//						PlainSelect ps = (PlainSelect) select.getSelectBody();
//						
//						LogicalPlanBuilder logicalBuilder = new LogicalPlanBuilder(db, ps);
//						PhysicalPlanConfig config = new PhysicalPlanConfig(new File(inputDir, "plan_builder_config.txt"));
//						
//						config.setTempDir(db.getTempDir());
//	
//						PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder(config);
//						LogicalOperator logicalPlan = logicalBuilder.getFinalOperator();
//						logicalPlan.accept(physicalBuilder);
//						PhysicalOperator physicalPlan = physicalBuilder.getPlan();
//						Tuple firstTuple = physicalPlan.getNextTuple();
//						int columns = 0;
//						String[] header = new String[0];
//						if(firstTuple != null) {
//							columns = firstTuple.getValues().length;
//							header = firstTuple.getFields();
//						}
//						Converter.BinaryToReadable(bnFile, header, hrFile);
//						physicalPlan.reset();
//						
//						TupleWriter writer = new BinaryTupleWriter(db.getOutputFile(queryCount), columns);
//						double secs = (double)physicalPlan.dump(writer)/1000000000.0;
//						writer.close();
//						
//						// Write out in readable format for kicks
//						File readableOutfile = new File(String.format("%s_humanreadable", db.getOutputFile(queryCount).getPath()));
//						File sortedOutfile = new File(String.format("%s_sorted", db.getOutputFile(queryCount).getPath()));
//						Converter.BinaryToReadable(db.getOutputFile(queryCount), header, readableOutfile);
//
//						
//						System.out.println(String.format("query%d completed in %f seconds", queryCount, secs));
//						
//						TupleReader rExpected = new ReadableTupleReader(hrFile, header);
//						TupleReader rTest = new ReadableTupleReader(readableOutfile, header);
//						if(!orderDependent) {
//							TupleWriter wExpected = new ReadableTupleWriter(sortedFile);
//							TupleWriter wTest = new ReadableTupleWriter(sortedOutfile);
//							util.sortFile(rExpected, wExpected);
//							util.sortFile(rTest, wTest);
//							wExpected.close();
//							wTest.close();
//							rExpected.close();
//							rTest.close();
//							rExpected = new ReadableTupleReader(sortedFile, header);
//							rTest = new ReadableTupleReader(sortedOutfile, header);
//						}
//						compareFiles(rExpected, rTest);
//						rExpected.close();
//						rTest.close();
//					}
//				} catch (Exception e){
//					e.printStackTrace();
//					fail(String.format("Exception occured on query%d: %s ", queryCount, statement));
//				}
//			}
//		} catch (Exception e) {
//			System.err.println("Exception occurred during parsing");
//			e.printStackTrace();
//		}
//	}
	
	/**
	 * Sets up a database with two tables, each containing 640 tuples, each table with 4 attributes.
	 * 
	 * @param humanReadable True if the tables should be in human readable format, false if binary.
	 * @param percentMatches The percentage of matches for equality on different tuples (i.e. the percentage
	 * 							chance that Table1.A == Table2.E) For example, if you give percentMatches = 50.0,
	 * 							attributes will be generated for the table in the range [0,2) so that the
	 * 							chance that two attributes will match is 50%. 
	 */
	public void setupDatabase(boolean humanReadable, double percentMatches) {
		rg = new RandomGenerator((int)(100/percentMatches));
		db = rg.getDatabase(humanReadable);	
		t1 = getTableAt(0);
		t2 = getTableAt(1);
	}
	
}
