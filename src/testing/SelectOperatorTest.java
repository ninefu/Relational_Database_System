package testing;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

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

public class SelectOperatorTest {
	DatabaseCatalog db;
	
	@Before
	public void setup() {
//		db = new DatabaseCatalog("samples/input","samples/expected_output","samples/temp");
		db = new DatabaseCatalog("configuration_test.txt");

	}
	
	public HashMap<String,String> getAliases(PlainSelect ps){
		String alias = ps.getFromItem().getAlias();
		if (alias == null){
			return null;
		}
		String[] from_string = ps.getFromItem().toString().split(" ");
		String table = from_string[0];
		HashMap<String, String> aliasMap = new HashMap<String,String>();
		aliasMap.put(alias, table);
		return aliasMap;
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
	public void testNoTuplesNoReferences() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE 9 < 8;\n");
		int count = 0;
		Tuple tup;
		while((tup = op.getNextTuple()) != null) {
			count++;
		}
		assertEquals(0, count);
	}
	
	@Test
	public void testAllTuplesNoReferences() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE 7 < 8;\n");
		int count = 0;
		Tuple tup;
		while((tup = op.getNextTuple()) != null) {
			count++;
		}
		assertEquals(10000, count);
	}
	
	@Test
	public void testAllTuplesReference() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.A >= 0");
		int count = 0;
		Tuple tup;
		while((tup = op.getNextTuple()) != null) {
			count++;
		}
		assertEquals(10000, count);
	}
	
	@Test
	public void testNoTuplesReference() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.B <= 99");
		int count = 0;
		Tuple tup;
		while((tup = op.getNextTuple()) != null) {
			count++;
		}
		assertEquals(96, count);
	}
	
	@Test
	public void testSomeTuplesReference() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.C = 4932");
		int count = 0;
		Tuple tup;
		while((tup = op.getNextTuple()) != null) {
			count++;
		}
		assertEquals(2, count);
	}
	
	@Test
	public void testLogicalReference() {
		SelectOperator op = getOperator("SELECT * from Sailors WHERE Sailors.C < 10000 AND Sailors.B = 100");
		int count = 0;
		Tuple tup;
		while((tup = op.getNextTuple()) != null) {
			count++;
		}
		assertEquals(2, count);
	}

}
