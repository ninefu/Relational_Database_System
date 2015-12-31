package logicalOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import ExpressionVisitor.JoinExpressionVisitor;
import ExpressionVisitor.UnionFindExpressionVisitor;
import database.DatabaseCatalog;
import database.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * LogicalPlanBuilder evaluates the parsed query, and constructs a tree-like logical query plan
 * where the root is the last executed operator and the leaf is the first
 * executed operator.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class LogicalPlanBuilder { // Mostly cannibalized from QueryPlan
	private DatabaseCatalog database;
	private PlainSelect plainSel;
	
	public LogicalPlanBuilder(DatabaseCatalog db, PlainSelect ps) {
		database = db;
		plainSel = ps;
	}
	
	/**
	 * Returns the actual name of the table references
	 * from the "from item"
	 * @param fi item extracted from the "from" clause of the query
	 * @return String name of the table referenced in the from clause.
	 */
	private String getNameFromItem(FromItem fi) {
		return fi.toString().split(" ")[0];
	}
	
	/**
	 * Method to get a list of select items in the format of [table name].[column name]
	 * 
	 * @param select_list
	 * 				A list of SelectItem.
	 * @return An array of string representing each select item.
	 */
	private String[] getSelectTarget(List<SelectItem> select_list){
		if(select_list.get(0).toString().equals("*")) {
			return null;
		}
		String[] target = new String[select_list.size()];
		for (int i = 0; i < select_list.size(); i++){
			target[i] = select_list.get(i).toString();
		}
		return target;
	}
	
	/**
	 * Method to get a list of orderby items in the format of [table reference].[column name]
	 * 
	 * @param orderby
	 * 				A list of OrderByElement
	 * @return An array of string representing each orderby item.
	 */
	public ArrayList<String> getOrderBy(List<OrderByElement> orderby){
		if (orderby == null){
			return null;
		}
		ArrayList<String> order = new ArrayList<String>();
		for (int i = 0; i < orderby.size(); i++){
			order.add(orderby.get(i).toString());
		}
		return order;
	}
	
	/**
	 * Method to build the logical plan tree
	 * @return LogicalOperator
	 * 			root of plan tree
	 */
	@SuppressWarnings("unchecked")
	public LogicalOperator getFinalOperator() {
		// set up, the same as the previous version
		String tableName = null;
		String tableAlias = null;
		String tableRef = null;
		ArrayList<String> tableRefList = new ArrayList<String>();
		ArrayList<String> tableNameList = new ArrayList<String>();
		FromItem fromItem = plainSel.getFromItem();
		List<Join> joins = plainSel.getJoins();
		Expression where = plainSel.getWhere();
		UnionFindExpressionVisitor ufv = new UnionFindExpressionVisitor();
		LogicalOperator finalOperator = null;
		
		// The topmost operator is a duplicate elimination operator, followed 
		// by a sort, a projection, and a single logical join operator.The 
		// children of the join are selections or leaves as appropriate.
		
		// Also, always assume that FROM includes at least one valid table
		tableName = getNameFromItem(fromItem);
		tableAlias = fromItem.getAlias();
		tableRef = tableAlias == null? tableName : tableAlias;
		tableNameList.add(tableName);
		tableRefList.add(tableRef);
		
		JoinExpressionVisitor jev = new JoinExpressionVisitor();
		if (where != null) {
			where.accept(jev);
			where.accept(ufv);
		}
		
		if (joins != null){
			for (Join j : joins){
				tableName = getNameFromItem(j.getRightItem());
				tableAlias = j.getRightItem().getAlias();
				tableRef = tableAlias == null? tableName : tableAlias;
				tableNameList.add(tableName);
				tableRefList.add(tableRef);
			}
		}
		finalOperator = new LogicalJoin(tableNameList, tableRefList, ufv, jev, database);
		
		String[] projects = getSelectTarget(plainSel.getSelectItems());
		
		boolean isProjection = !(projects == null);
		// project operator
		if (isProjection){
			finalOperator = new LogicalProject(projects, finalOperator);
		}
		
		// sort operator
		String[] fullOrderList;
		ArrayList<String> allColumns = new ArrayList<String>();
		for(int i = 0; i < tableNameList.size(); i++) {
			Table table = database.getTable(tableNameList.get(i));
			String[] theseColumns = table.getQualifiedSchema(tableRefList.get(i));
			for(String c : theseColumns) {
				allColumns.add(c);
			}
		}
		
		ArrayList<String> orderList = getOrderBy(plainSel.getOrderByElements());
		boolean isSorted = orderList != null;
		if(orderList == null) {
			orderList = new ArrayList<String>();
		}
		
		if(isProjection) {
			fullOrderList = new String[projects.length];
		} else {
			fullOrderList = new String[allColumns.size()];
		}
		for (int i = 0; i < orderList.size(); i++){
			fullOrderList[i] = orderList.get(i);
		}
		int position = orderList.size();
		if(isProjection) {
			for(String col : projects) {
				if(!orderList.contains(col)) {
					fullOrderList[position] = col;
					position++;
				}
			}
		} else {
			for(String col : allColumns) {
				if(!orderList.contains(col)) {
					fullOrderList[position] = col;
					position++;
				}
			}
		}
		if(isSorted) {	
			finalOperator = new LogicalSort(fullOrderList, finalOperator, orderList);
		}

		// duplicate eliminate operator
		Distinct distinct = plainSel.getDistinct();
		if (distinct != null) {
			if(!isSorted) {
				finalOperator = new LogicalSort(fullOrderList, finalOperator, null);
			}
			finalOperator = new LogicalDistinct((LogicalSort)finalOperator);
		}
		
		return finalOperator;
	}
}
