package testing;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import database.DatabaseCatalog;
import database.Table;
import database.Tuple;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import physicalOperator.TupleNestedJoinOperator;
import physicalOperator.PhysicalOperator;
import physicalOperator.ScanOperator;
import physicalOperator.SequentialScanOperator;

/**
 * Again, this tester is not useful
 * @author yihuifu
 *
 */
public class JoinOperatorTest {
	DatabaseCatalog db;
	
	@Before
	public void setup() {
//		db = new DatabaseCatalog("samples/input","samples/expected_output","samples/temp");
		db = new DatabaseCatalog("configuration_test.txt");

	}
	
	private String getTableName(FromItem from) {
		return from.toString().split(" ")[0];
	}
	
	@SuppressWarnings("unchecked")
	private TupleNestedJoinOperator getOperator(String s) {
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try {
			ps = parser.PlainSelect();
			FromItem from = ps.getFromItem();
			List<Join> joins = ps.getJoins();
			String tableName = getTableName(from);
			Table table = db.getTable(tableName);
			PhysicalOperator leftChild = new SequentialScanOperator(table, from.getAlias());
			TupleNestedJoinOperator joinOp = null;
			for(Join j : joins) {
				// Pull conditions from the "on" clause (which we normally wouldn't use) because it's easier to isolate
				// then having to split up the where clause, which is more QueryPlan functionality anyway
				PhysicalOperator rightChild = new SequentialScanOperator(db.getTable(getTableName(j.getRightItem())), j.getRightItem().getAlias());
				joinOp = new TupleNestedJoinOperator(leftChild, rightChild, j.getOnExpression());
				leftChild = joinOp;
			}
			return joinOp;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Test
	public void testCrossProduct() {
		TupleNestedJoinOperator op = getOperator("SELECT * FROM Sailors, Boats");
		Tuple tup = op.getNextTuple();
		assertEquals("2228,3083,8277,7811,4505,5766", tup.toString());
		assertArrayEquals(new String[]{"Sailors.A", "Sailors.B", "Sailors.C", "Boats.D", "Boats.E", "Boats.F"}, tup.getFields());
	}
	
	@Test
	public void testJoinConditionExpression() {
		TupleNestedJoinOperator op = getOperator("SELECT * from Sailors, Boats ON Sailors.A = Boats.E");
		Tuple tup = op.getNextTuple();
		assertEquals("2228,3083,8277,2031,2228,3666", tup.toString());
		tup = op.getNextTuple();
		assertEquals("5354,743,6031,8166,5354,7443", tup.toString());
	}
		
	@Test
	public void testSelfJoin() {
		TupleNestedJoinOperator op = getOperator("SELECT * from Sailors S1, Sailors S2 ON S1.A = S2.A");
		Tuple tup = op.getNextTuple();
		assertEquals("2228,3083,8277,2228,3083,8277", tup.toString());
		tup = op.getNextTuple();
		assertEquals("5354,743,6031,5354,743,6031", tup.toString());
		tup = op.getNextTuple();
		assertEquals("5354,743,6031,5354,8314,1685", tup.toString());
		tup = op.getNextTuple();
		assertEquals("7495,298,9221,7495,298,9221", tup.toString());
		tup = op.getNextTuple();
		assertEquals("7495,298,9221,7495,981,7118", tup.toString());
		tup = op.getNextTuple();
		assertEquals("1884,337,4632,1884,337,4632", tup.toString());
//		tup = op.getNextTuple();
//		assertNull(tup);	// new input is sooooo long.
	}
}
