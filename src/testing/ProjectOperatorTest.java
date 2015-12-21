package testing;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import database.DatabaseCatalog;
import database.Table;
import database.Tuple;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import physicalOperator.ProjectOperator;
import physicalOperator.ScanOperator;
import physicalOperator.SequentialScanOperator;

public class ProjectOperatorTest {
	DatabaseCatalog db;
		
	@Before
	public void setup() {
//		db = new DatabaseCatalog("samples/input","samples/expected_output","samples/temp");
		db = new DatabaseCatalog("configuration_test.txt");
	}
	
	private ProjectOperator getOperator(String s) {
		CCJSqlParser parser = new CCJSqlParser(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		PlainSelect ps;
		try {
			ps = parser.PlainSelect();
			FromItem from = ps.getFromItem();
			Table table = db.getTable(from.toString());
			ScanOperator child = new SequentialScanOperator(table, from.getAlias());
			@SuppressWarnings("unchecked")
			List<SelectItem> selectList = ps.getSelectItems();
			ArrayList<String> columns = new ArrayList<String>();
			for(SelectItem si : selectList) {
				if(si instanceof AllColumns) {
					for(String c : table.getQualifiedSchema(from.getAlias())) {
						columns.add(c);
					}
				} else {
					SelectExpressionItem sei = (SelectExpressionItem)si;
					Column col = (Column) sei.getExpression();
					columns.add(col.getWholeColumnName());
				}
			}
			String[] columnArray = new String[columns.size()];
			return new ProjectOperator(columns.toArray(columnArray), child);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Test
	public void testBasic() {
		ProjectOperator op = getOperator("SELECT Sailors.A from Sailors");
		Tuple tup = op.getNextTuple();
		assertEquals(tup.getValueAtField("Sailors.A"), new Integer(2228));
		assertEquals(tup.getValueAtField("Sailors.B"), null);
		assertEquals(tup.getValueAtField("Sailors.C"), null);
		
		tup = op.getNextTuple();
		assertEquals(tup.getValueAtField("Sailors.A"), new Integer(5354));
		assertEquals(tup.getValueAtField("Sailors.B"), null);
		assertEquals(tup.getValueAtField("Sailors.C"), null);
		
		tup = op.getNextTuple();
		assertEquals(tup.getValueAtField("Sailors.A"), new Integer(7495));
		
		tup = op.getNextTuple();
		assertEquals(tup.getValueAtField("Sailors.A"), new Integer(1884));
		
		tup = op.getNextTuple();
		assertEquals(tup.getValueAtField("Sailors.A"), new Integer (7494));
		
		tup = op.getNextTuple();
		assertEquals(tup.getValueAtField("Sailors.A"), new Integer(2540));
		
//		tup = op.getNextTuple();
//		assertNull(tup);
	}
	
	@Test
	public void testTwoSelections() {
		ProjectOperator op = getOperator("SELECT Sailors.B, Sailors.C FROM Sailors");
		Tuple tup = op.getNextTuple();
		assertEquals("3083,8277", tup.toString());

		tup = op.getNextTuple();
		assertEquals("743,6031", tup.toString());
		
		tup = op.getNextTuple();
		assertEquals("298,9221", tup.toString());
		
		tup = op.getNextTuple();
		assertEquals("337,4632", tup.toString());
		
		tup = op.getNextTuple();
		assertEquals("8450,5172", tup.toString());
		
		tup = op.getNextTuple();
		assertEquals("5492,4298", tup.toString());
		
//		tup = op.getNextTuple();
//		assertNull(tup);
	}
	
	@Test
	public void testCorrectOrder() {
		ProjectOperator op = getOperator("SELECT Sailors.C, Sailors.B FROM Sailors");
		Tuple tup = op.getNextTuple();
		assertEquals("8277,3083", tup.toString());

		tup = op.getNextTuple();
		assertEquals("6031,743", tup.toString());
		
		tup = op.getNextTuple();
		assertEquals("9221,298", tup.toString());
		
		tup = op.getNextTuple();
		assertEquals("4632,337", tup.toString());
		
		tup = op.getNextTuple();
		assertEquals("5172,8450", tup.toString());
		
		tup = op.getNextTuple();
		assertEquals("4298,5492", tup.toString());
		
//		tup = op.getNextTuple();
//		assertNull(tup);
	}
}
