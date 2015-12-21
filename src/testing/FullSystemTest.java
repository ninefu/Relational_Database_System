package testing;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import database.DatabaseCatalog;
import database.QueryPlan;
import database.Tuple;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.PlainSelect;
import physicalOperator.PhysicalOperator;

/**
 * This tester is no longer useful
 * By Yihui
 *
 */
public class FullSystemTest {
	String inputdir = "samples/input";
	String outputdir = "samples/expected_output";
	String tempdir = "samples/temp";
	
	DatabaseCatalog db = new DatabaseCatalog("configuration_test.txt");
	
	@Test
	public void testSelectAllOneTable(){
		String s = "SELECT * FROM Sailors";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("1,200,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2,200,200",tup.toString());
			tup = op.getNextTuple();
			assertEquals("3,100,105",tup.toString());
			tup = op.getNextTuple();
			assertEquals("4,100,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("5,100,500",tup.toString());
			tup = op.getNextTuple();
			assertEquals("6,300,400",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testProjectOneTable(){
		String s = "SELECT Sailors.A FROM Sailors;";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("1",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2",tup.toString());
			tup = op.getNextTuple();
			assertEquals("3",tup.toString());
			tup = op.getNextTuple();
			assertEquals("4",tup.toString());
			tup = op.getNextTuple();
			assertEquals("5",tup.toString());
			tup = op.getNextTuple();
			assertEquals("6",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testAliasProjectOneTable(){
		String s = "SELECT S.A FROM Sailors S";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("1",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2",tup.toString());
			tup = op.getNextTuple();
			assertEquals("3",tup.toString());
			tup = op.getNextTuple();
			assertEquals("4",tup.toString());
			tup = op.getNextTuple();
			assertEquals("5",tup.toString());
			tup = op.getNextTuple();
			assertEquals("6",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAliasSelectProjectOneTable(){
		String s = "SELECT * FROM Sailors S WHERE S.A < 3";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("1,200,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2,200,200",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSelectJoin(){
		String s = "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("1,200,50,1,101",tup.toString());
			tup = op.getNextTuple();
			assertEquals("1,200,50,1,102",tup.toString());
			tup = op.getNextTuple();
			assertEquals("1,200,50,1,103",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2,200,200,2,101",tup.toString());
			tup = op.getNextTuple();
			assertEquals("3,100,105,3,102",tup.toString());
			tup = op.getNextTuple();
			assertEquals("4,100,50,4,104",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testJoinAlias(){
		String s = "SELECT * FROM Sailors S, Reserves R WHERE S.A = R.G";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("1,200,50,1,101",tup.toString());
			tup = op.getNextTuple();
			assertEquals("1,200,50,1,102",tup.toString());
			tup = op.getNextTuple();
			assertEquals("1,200,50,1,103",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2,200,200,2,101",tup.toString());
			tup = op.getNextTuple();
			assertEquals("3,100,105,3,102",tup.toString());
			tup = op.getNextTuple();
			assertEquals("4,100,50,4,104",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSelfJoinAlias(){
		String s = "SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("1,200,50,2,200,200",tup.toString());
			tup = op.getNextTuple();
			assertEquals("1,200,50,3,100,105",tup.toString());
			tup = op.getNextTuple();
			assertEquals("1,200,50,4,100,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("1,200,50,5,100,500",tup.toString());
			tup = op.getNextTuple();
			assertEquals("1,200,50,6,300,400",tup.toString());
			
			tup = op.getNextTuple();
			assertEquals("2,200,200,3,100,105",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2,200,200,4,100,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2,200,200,5,100,500",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2,200,200,6,300,400",tup.toString());
			
			tup = op.getNextTuple();
			assertEquals("3,100,105,4,100,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("3,100,105,5,100,500",tup.toString());
			tup = op.getNextTuple();
			assertEquals("3,100,105,6,300,400",tup.toString());
			
			tup = op.getNextTuple();
			assertEquals("4,100,50,5,100,500",tup.toString());
			tup = op.getNextTuple();
			assertEquals("4,100,50,6,300,400",tup.toString());
			
			tup = op.getNextTuple();
			assertEquals("5,100,500,6,300,400",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testDistinctOneTable(){
		String s = "SELECT DISTINCT Reserves.G FROM Reserves;";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("1",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2",tup.toString());
			tup = op.getNextTuple();
			assertEquals("3",tup.toString());
			tup = op.getNextTuple();
			assertEquals("4",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testOrderByOneTable(){
		String s = "SELECT * FROM Sailors ORDER BY Sailors.B;";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("3,100,105",tup.toString());
			tup = op.getNextTuple();
			assertEquals("4,100,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("5,100,500",tup.toString());
			tup = op.getNextTuple();
			assertEquals("1,200,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2,200,200",tup.toString());
			tup = op.getNextTuple();
			assertEquals("6,300,400",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSelectMultiConditionOneTable(){
		String s = "SELECT * from Sailors WHERE Sailors.C < 400 AND Sailors.B = 100;";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("3,100,105",tup.toString());
			tup = op.getNextTuple();
			assertEquals("4,100,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testMultiProjectOneTable(){
		String s = "SELECT Sailors.B, Sailors.C FROM Sailors;";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("200,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("200,200",tup.toString());
			tup = op.getNextTuple();
			assertEquals("100,105",tup.toString());
			tup = op.getNextTuple();
			assertEquals("100,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("100,500",tup.toString());
			tup = op.getNextTuple();
			assertEquals("300,400",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testMultiProjectOneTableTwo(){
		String s = "SELECT Sailors.C, Sailors.B FROM Sailors;";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("50,200",tup.toString());
			tup = op.getNextTuple();
			assertEquals("200,200",tup.toString());
			tup = op.getNextTuple();
			assertEquals("105,100",tup.toString());
			tup = op.getNextTuple();
			assertEquals("50,100",tup.toString());
			tup = op.getNextTuple();
			assertEquals("500,100",tup.toString());
			tup = op.getNextTuple();
			assertEquals("400,300",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testMultiOrderByOneTable(){
		String s = "SELECT * FROM Sailors AS S ORDER BY S.B, S.A;";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("3,100,105",tup.toString());
			tup = op.getNextTuple();
			assertEquals("4,100,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("5,100,500",tup.toString());
			tup = op.getNextTuple();
			assertEquals("1,200,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2,200,200",tup.toString());
			tup = op.getNextTuple();
			assertEquals("6,300,400",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAliasJoinSelect(){
		String s = "SELECT * FROM Reserves AS R, Sailors AS S WHERE R.G < R.H AND S.A = 1 AND R.G = S.A;";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("1,101,1,200,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("1,102,1,200,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals("1,103,1,200,50",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testComplext(){
		String s = "SELECT * FROM Sailors, Boats, Reserves WHERE Sailors.C>50 AND Sailors.A = Reserves.G AND Boats.D = Reserves.H ORDER BY Sailors.B";
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try{
			ps = parser.PlainSelect();
			QueryPlan qp = new QueryPlan(ps,db);
			PhysicalOperator op = qp.getFinalOperator(db);
			
			Tuple tup = op.getNextTuple();
			assertEquals("3,100,105,102,3,4,3,102",tup.toString());
			tup = op.getNextTuple();
			assertEquals("2,200,200,101,2,3,2,101",tup.toString());
			tup = op.getNextTuple();
			assertEquals(null,tup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
