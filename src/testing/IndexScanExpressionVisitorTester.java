package testing;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import ExpressionVisitor.IndexScanExpressionVisitor;
import database.DatabaseCatalog;
import database.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import physicalOperator.ScanOperator;
import physicalOperator.SelectOperator;
import physicalOperator.SequentialScanOperator;

public class IndexScanExpressionVisitorTester {
	DatabaseCatalog db;
	
	@Before
	public void setup() {
//		db = new DatabaseCatalog("samples/input","samples/expected_output","samples/temp");
		db = new DatabaseCatalog("configuration_test.txt");

	}
	
	private SelectOperator getOperator(String s) {
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try {
			ps = parser.PlainSelect();
			FromItem from = ps.getFromItem();
			ScanOperator child = new SequentialScanOperator(db.getTable(from.toString()), from.getAlias());
			Expression exp = ps.getWhere();
			return new SelectOperator(child, exp);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Test
	public void testEqCondition() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.A = 1");
		IndexScanExpressionVisitor v = new IndexScanExpressionVisitor("A");
		op.expression().accept(v);
		double arr[] = v.getHighAndLow();
		String s1, s2;
		if(arr[0] == Double.MAX_VALUE) {
			s1 = "null";
		} else {
			s1 = Double.toString(Math.round(arr[0]));
		}
		if(arr[1] == -Double.MAX_VALUE) {
			s2 = "null";
		} else {
			s2 = Double.toString(Math.round(arr[1]));
		}
		
		System.out.println("equals test: " + s1 + " " + s2);
		if(v.getRemainderCondition() == null) {
			System.out.println("no remainder");
		} else {
			System.out.println(v.getRemainderCondition().toString());
		}
	}
	
	@Test
	public void testGreaterCondition() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.A >= 0");
		IndexScanExpressionVisitor v = new IndexScanExpressionVisitor("A");
		op.expression().accept(v);
		double arr[] = v.getHighAndLow();
		String s1, s2;
		if(arr[0] == Double.MAX_VALUE) {
			s1 = "null";
		} else {
			s1 = Double.toString(Math.round(arr[0]));
		}
		if(arr[1] == -Double.MAX_VALUE) {
			s2 = "null";
		} else {
			s2 = Double.toString(Math.round(arr[1]));
		}
		
		System.out.println("greater test: " + s1 + " " + s2);
		if(v.getRemainderCondition() == null) {
			System.out.println("no remainder");
		} else {
			System.out.println(v.getRemainderCondition().toString());
		}
	}
	
	@Test
	public void testLessCondition() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.A < 20");
		IndexScanExpressionVisitor v = new IndexScanExpressionVisitor("A");
		op.expression().accept(v);
		double arr[] = v.getHighAndLow();
		String s1, s2;
		if(arr[0] == Double.MAX_VALUE) {
			s1 = "null";
		} else {
			s1 = Double.toString(Math.round(arr[0]));
		}
		if(arr[1] == -Double.MAX_VALUE) {
			s2 = "null";
		} else {
			s2 = Double.toString(Math.round(arr[1]));
		}
		
		System.out.println("less test: " + s1 + " " + s2);
		if(v.getRemainderCondition() == null) {
			System.out.println("no remainder");
		} else {
			System.out.println(v.getRemainderCondition().toString());
		}	}
	
	@Test
	public void testWrongAttributeIndexed() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.A >= 0");
		IndexScanExpressionVisitor v = new IndexScanExpressionVisitor("B");
		op.expression().accept(v);
		double arr[] = v.getHighAndLow();
		String s1, s2;
		if(arr[0] == Double.MAX_VALUE) {
			s1 = "null";
		} else {
			s1 = Double.toString(Math.round(arr[0]));
		}
		if(arr[1] == -Double.MAX_VALUE) {
			s2 = "null";
		} else {
			s2 = Double.toString(Math.round(arr[1]));
		}
		
		System.out.println("wrong attr indexed test: " + s1 + " " + s2);
		if(v.getRemainderCondition() == null) {
			System.out.println("no remainder");
		} else {
			System.out.println(v.getRemainderCondition().toString());
		}	}
	
	@Test
	public void testBetweenCondition() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.A >= 5 AND Sailors.A <= 10");
		IndexScanExpressionVisitor v = new IndexScanExpressionVisitor("A");
		op.expression().accept(v);
		double arr[] = v.getHighAndLow();
		String s1, s2;
		if(arr[0] == Double.MAX_VALUE) {
			s1 = "null";
		} else {
			s1 = Double.toString(Math.round(arr[0]));
		}
		if(arr[1] == -Double.MAX_VALUE) {
			s2 = "null";
		} else {
			s2 = Double.toString(Math.round(arr[1]));
		}
		//System.out.println(op.expression());
		System.out.println("between test: " + s1 + " " + s2);
		if(v.getRemainderCondition() == null) {
			System.out.println("no remainder");
		} else {
			System.out.println(v.getRemainderCondition().toString());
		}	}
	
	@Test
	public void testLeftConditionValid() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.A >= 5 AND Sailors.B <= 10");
		IndexScanExpressionVisitor v = new IndexScanExpressionVisitor("A");
		op.expression().accept(v);
		double arr[] = v.getHighAndLow();
		String s1, s2;
		if(arr[0] == Double.MAX_VALUE) {
			s1 = "null";
		} else {
			s1 = Double.toString(Math.round(arr[0]));
		}
		if(arr[1] == -Double.MAX_VALUE) {
			s2 = "null";
		} else {
			s2 = Double.toString(Math.round(arr[1]));
		}
		//System.out.println(op.expression());
		System.out.println("left only test: " + s1 + " " + s2);
		if(v.getRemainderCondition() == null) {
			System.out.println("no remainder");
		} else {
			System.out.println(v.getRemainderCondition().toString());
		}	}
	
	@Test
	public void testRightConditionValid() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.B >= 5 AND Sailors.A <= 10");
		IndexScanExpressionVisitor v = new IndexScanExpressionVisitor("A");
		op.expression().accept(v);
		double arr[] = v.getHighAndLow();
		String s1, s2;
		if(arr[0] == Double.MAX_VALUE) {
			s1 = "null";
		} else {
			s1 = Double.toString(Math.round(arr[0]));
		}
		if(arr[1] == -Double.MAX_VALUE) {
			s2 = "null";
		} else {
			s2 = Double.toString(Math.round(arr[1]));
		}
		//System.out.println(op.expression());
		System.out.println("right only test: " + s1 + " " + s2);
		if(v.getRemainderCondition() == null) {
			System.out.println("no remainder");
		} else {
			System.out.println(v.getRemainderCondition().toString());
		}	}
	
	@Test
	public void testNotEquals() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.A != 5");
		IndexScanExpressionVisitor v = new IndexScanExpressionVisitor("A");
		op.expression().accept(v);
		double arr[] = v.getHighAndLow();
		String s1, s2;
		if(arr[0] == Double.MAX_VALUE) {
			s1 = "null";
		} else {
			s1 = Double.toString(Math.round(arr[0]));
		}
		if(arr[1] == -Double.MAX_VALUE) {
			s2 = "null";
		} else {
			s2 = Double.toString(Math.round(arr[1]));
		}
		//System.out.println(op.expression());
		System.out.println("not equals test: " + s1 + " " + s2);
		if(v.getRemainderCondition() == null) {
			System.out.println("no remainder");
		} else {
			System.out.println(v.getRemainderCondition().toString());
		}	}
	
	@Test
	public void testNotEqualsAndOtherThing() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.A != 5 AND Sailors.A > 3");
		IndexScanExpressionVisitor v = new IndexScanExpressionVisitor("A");
		op.expression().accept(v);
		double arr[] = v.getHighAndLow();
		String s1, s2;
		if(arr[0] == Double.MAX_VALUE) {
			s1 = "null";
		} else {
			s1 = Double.toString(Math.round(arr[0]));
		}
		if(arr[1] == -Double.MAX_VALUE) {
			s2 = "null";
		} else {
			s2 = Double.toString(Math.round(arr[1]));
		}
		//System.out.println(op.expression());
		System.out.println("not equals and other thing test: " + s1 + " " + s2);
		if(v.getRemainderCondition() == null) {
			System.out.println("no remainder");
		} else {
			System.out.println(v.getRemainderCondition().toString());
		}
	}
	
	@Test
	public void testMultipleBadConditions() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.A != 5 AND Sailors.A > 3 AND Sailors.B > 0");
		IndexScanExpressionVisitor v = new IndexScanExpressionVisitor("A");
		op.expression().accept(v);
		double arr[] = v.getHighAndLow();
		String s1, s2;
		if(arr[0] == Double.MAX_VALUE) {
			s1 = "null";
		} else {
			s1 = Double.toString(Math.round(arr[0]));
		}
		if(arr[1] == -Double.MAX_VALUE) {
			s2 = "null";
		} else {
			s2 = Double.toString(Math.round(arr[1]));
		}
		//System.out.println(op.expression());
		System.out.println("multiple nonindexed conditions test: " + s1 + " " + s2);
		if(v.getRemainderCondition() == null) {
			System.out.println("no remainder");
		} else {
			System.out.println(v.getRemainderCondition().toString());
		}
	}
	
	
}
