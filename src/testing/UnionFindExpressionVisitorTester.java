package testing;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import ExpressionVisitor.IndexScanExpressionVisitor;
import ExpressionVisitor.UnionFindExpressionVisitor;
import UnionFind.UnionFind;
import UnionFind.UnionFindElement;
import database.DatabaseCatalog;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import physicalOperator.ScanOperator;
import physicalOperator.SelectOperator;

public class UnionFindExpressionVisitorTester {
	
	private Expression getWhere(String s){
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try {
			ps = parser.PlainSelect();
			Expression exp = ps.getWhere();
			return exp;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Test
	public void testGetCondition(){
		Expression where = getWhere("SELECT * FROM Sailors WHERE Sailors.A = 1");
		assertEquals("Sailors.A = 1",where.toString());
	}
	
	@Test 
	public void testGetter(){
		Expression where = getWhere("SELECT * FROM R, S, T, U WHERE R.A <> U.B AND R.A = S.B AND S.C = T.D AND R.A = 2 AND T.D = T.X AND U.Y <> 42");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals("R.A <> U.B", remainder.toString());
		assertEquals("U.Y <> 42",ufv.getRemainderSelect("U").toString());
		assertEquals("[[R.A, S.B], equals 2, min 2, max 2]\n[[S.C, T.D, T.X], equals null, min null, max null]",ufv.getUnionFind().toString());
	}
	
	@Test 
	public void testGette2(){
		Expression where = getWhere("SELECT * FROM R, S, T, U WHERE R.A <> U.B AND R.A = S.B AND S.C = T.D AND R.A = 2 AND T.D = T.X AND U.Y <> 42 AND U.M <>5");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals("R.A <> U.B", remainder.toString());
		assertEquals("U.Y <> 42 AND U.M <> 5",ufv.getRemainderSelect("U").toString());
		assertEquals("[[R.A, S.B], equals 2, min 2, max 2]\n[[S.C, T.D, T.X], equals null, min null, max null]",ufv.getUnionFind().toString());
	}
	
	@Test
	public void testAttrEqualValCondition(){
		Expression where = getWhere("SELECT * from Sailors WHERE Sailors.A = 1");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals 1, min 1, max 1]",ufv.getUnionFind().toString());
	}
	
	@Test
	public void testAttrEqualValConditionOpp(){
		Expression where = getWhere("SELECT * from Sailors WHERE 1 = Sailors.A");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals 1, min 1, max 1]",ufv.getUnionFind().toString());
	}
	
	@Test
	public void testAttrEqualAttrCondition(){
		Expression where = getWhere("SELECT * from Sailors WHERE Sailors.A = Sailors.B");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A, Sailors.B], equals null, min null, max null]",ufv.getUnionFind().toString());
	}
	
	@Test
	public void testGreater(){
		Expression where = getWhere("SELECT * from Sailors WHERE Sailors.A > 1");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals null, min 2, max null]",ufv.getUnionFind().toString());
	}

	@Test
	public void testGreaterOrEqual(){
		Expression where = getWhere("SELECT * from Sailors WHERE Sailors.A >= 1");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals null, min 1, max null]",ufv.getUnionFind().toString());
	}
	
	@Test
	public void testGreaterOrEqualOpp(){
		Expression where = getWhere("SELECT * from Sailors WHERE 1 >= Sailors.A");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals null, min null, max 1]",ufv.getUnionFind().toString());
	}
	
	@Test
	public void testLess(){
		Expression where = getWhere("SELECT * from Sailors WHERE Sailors.A < 1");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals null, min null, max 0]",ufv.getUnionFind().toString());
	}
	
	@Test
	public void testLessOrEqual(){
		Expression where = getWhere("SELECT * from Sailors WHERE Sailors.A <= 1");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals null, min null, max 1]",ufv.getUnionFind().toString());
	}
	
	@Test
	public void testLessOrEqualOpp(){
		Expression where = getWhere("SELECT * from Sailors WHERE 1 <= Sailors.A");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals null, min 1, max null]",ufv.getUnionFind().toString());
	}
	
	
	
	@Test
	public void testMultipleWithAlias(){
		Expression where = getWhere("SELECT DISTINCT S.A, R.G " + 
				"FROM Sailors S, Reserves R, Boats B " + 
				"WHERE S.B = R.G AND S.A = B.D AND R.H <> B.D AND R.H < 100 " + 
				"ORDER BY S.A;");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals("R.H <> B.D", remainder.toString());
		assertEquals("[[S.B, R.G], equals null, min null, max null]\n" +
					"[[S.A, B.D], equals null, min null, max null]\n" + 
					"[[R.H], equals null, min null, max 99]",ufv.getUnionFind().toString());
	}

	@Test
	public void testAnd(){
		Expression where = getWhere("SELECT * from Sailors WHERE Sailors.A < 1 AND Sailors.A > -10");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals null, min -9, max 0]",ufv.getUnionFind().toString());
	}
	
	@Test
	public void testDivision(){
		Expression where = getWhere("SELECT * from Sailors WHERE Sailors.A < 10/1");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals null, min null, max 9]",ufv.getUnionFind().toString());
	}
	
	@Test
	public void testMultiplication(){
		Expression where = getWhere("SELECT * from Sailors WHERE Sailors.A < 10*2");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals null, min null, max 19]",ufv.getUnionFind().toString());
	}
	
	@Test
	public void testAddition(){
		Expression where = getWhere("SELECT * from Sailors WHERE Sailors.A < 10+2");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals null, min null, max 11]",ufv.getUnionFind().toString());
	}

	@Test
	public void testSubtraction(){
		Expression where = getWhere("SELECT * from Sailors WHERE Sailors.A < 10-2");
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		where.accept(ufv);
		Expression remainder = ufv.getRemainderJoins();
		assertEquals(null, remainder);
		assertEquals("[[Sailors.A], equals null, min null, max 7]",ufv.getUnionFind().toString());
	}

	
}
