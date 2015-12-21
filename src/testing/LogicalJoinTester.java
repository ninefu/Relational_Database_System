package testing;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.junit.Ignore;
import org.junit.Test;

import database.DatabaseCatalog;
import logicalOperator.LogicalOperator;
import logicalOperator.LogicalPlanBuilder;
import logicalOperator.LogicalPlanPrinter;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class LogicalJoinTester {
	DatabaseCatalog db = new DatabaseCatalog("configuration_test.txt");;
	
	private LogicalOperator getOperator(String s) throws ParseException {
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		ps = parser.PlainSelect();

		LogicalPlanBuilder plan = new LogicalPlanBuilder(db, ps);
		return plan.getFinalOperator();
	}
	
	@Test
	public void test() throws ParseException {
		String query = "SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Boats.D AND Boats.D = 2 AND Reserves.G <> 10 AND Sailors.B <> Boats.E";
		LogicalOperator op = getOperator(query);
		System.out.println(op.toString());
	}
	
	@Test
	public void test2() throws ParseException {
		String query = "SELECT * FROM Sailors AS S1, Sailors AS S2 WHERE S1.A = S2.B AND S1.B > 10";
		LogicalOperator op = getOperator(query);
		System.out.println(op.toString());
	}
	
	@Test
	public void test3() throws ParseException {
		String query = "SELECT * FROM Sailors AS S1, Sailors AS S2";
		LogicalOperator op = getOperator(query);
		System.out.println(op.toString());
	}

	@Ignore
	@Test
	public void testToString() throws ParseException{
		String query = "SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Boats.D AND Boats.D = 2 AND Reserves.G <> 10 AND Sailors.B <> Boats.E";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		ps = parser.PlainSelect();
		LogicalPlanBuilder plan = new LogicalPlanBuilder(db, ps);
		System.out.println(plan.toString());
	}
	
	@Test
	public void testToString2() throws ParseException{
		String query = "SELECT DISTINCT S.A, R.G FROM Sailors S, Reserves R, Boats B " + 
						"WHERE S.B = R.G AND S.A = B.D AND R.H <> B.D AND R.H < 100 ORDER BY S.A;";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		ps = parser.PlainSelect();
		LogicalPlanBuilder plan = new LogicalPlanBuilder(db, ps);
		LogicalPlanPrinter printer = new LogicalPlanPrinter();
		LogicalOperator logicalPlan = plan.getFinalOperator();
		logicalPlan.accept(printer);
//		System.out.println(printer.toString());
		assertEquals("DupElim\n-Sort[S.A]\n--Project[S.A, R.G]\n---Join[R.H <> B.D]\n[[S.B, R.G], equals null, min null, max null]"+
				"\n[[S.A, B.D], equals null, min null, max null]\n[[R.H], equals null, min null, max 99]\n----Leaf[Sailors]\n----Select[R.H <= 99]"+
				"\n-----Leaf[Reserves]\n----Leaf[Boats]",printer.toString());
	}
	
	
	@Ignore
	@Test
	public void testToString3() throws ParseException{
		String query = "SELECT DISTINCT S.A, R.G FROM Sailors S, Reserves R WHERE S.B = R.G AND S.C = 1;";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		ps = parser.PlainSelect();
		LogicalPlanBuilder plan = new LogicalPlanBuilder(db, ps);
		System.out.println(plan.toString());
	}
}
