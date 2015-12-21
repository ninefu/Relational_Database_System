import java.io.FileReader;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Example class for getting started with JSQLParser. Reads SQL statements from
 * a file and prints them to screen; then extracts SelectBody from each query
 * and also prints it to screen.
 * 
 * @author Lucja Kot
 */
public class ParserExample {

	private static final String queriesFile = "samples/input/testQueries.sql";

	public static void main(String[] args) {
		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
			Statement statement;
			while ((statement = parser.Statement()) != null) {
				System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				System.out.println("Select body is " + select.getSelectBody());
				PlainSelect ps = (PlainSelect) select.getSelectBody();
				System.out.println("PlainSelect is " + ps);
				Distinct dist = ps.getDistinct();
				System.out.println("Distinct in selection is " + dist);
				List<SelectItem> select_list = ps.getSelectItems();
				for (SelectItem si : select_list){
					System.out.println("Select item is " + si.toString());
				}
				System.out.println("Select target is " + select_list);
				FromItem from_item = ps.getFromItem();
				System.out.println("From item is " + from_item.toString());
				String alias = from_item.getAlias();
				System.out.println("Alias is: " + alias);
				List<Join> joins = ps.getJoins();
				System.out.println("Join tables are " + joins);
				if (joins != null){
					for (Join join : joins){
						System.out.println("Join item's alias are " + join.getRightItem().getAlias() );
					}
				}
				Expression where = ps.getWhere();
				System.out.println("Where expressions are " + where);
				// where expression visitor
				List<OrderByElement> orderby = ps.getOrderByElements();
				System.out.println("Order by " + orderby);
				if (orderby != null){
					for (OrderByElement ob : orderby){
						System.out.println("Orderby expression " + ob.getExpression().toString());
					}
				}
				
				
				System.out.println("     ");
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}