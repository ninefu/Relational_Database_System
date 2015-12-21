/**
 * 
 */
package database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import ExpressionVisitor.JoinExpressionVisitor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import physicalOperator.DuplicateEliminateOperator;
import physicalOperator.TupleNestedJoinOperator;
import physicalOperator.PhysicalOperator;
import physicalOperator.ProjectOperator;
import physicalOperator.ScanOperator;
import physicalOperator.SelectOperator;
import physicalOperator.SequentialScanOperator;
import physicalOperator.InMemorySortOperator;

/**
 * (Not used in Project 3 and 4)
 * QueryPlan evaluates the parsed query, and constructs a tree-like query plan
 * where the root is the last executed operator and the leaf is the first
 * executed operator.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class QueryPlan {
	
	private Distinct distinct;
	private FromItem fromItem;
	private List<Join> joins;
	private Expression where;
	private String[] selectTarget;
	private ArrayList<String> orderList;
	
	@SuppressWarnings("unchecked")
	public QueryPlan(PlainSelect ps, DatabaseCatalog db){
		List<SelectItem> selectList = ps.getSelectItems();
		List<OrderByElement> orderBy = ps.getOrderByElements();
		distinct = ps.getDistinct();
		fromItem = ps.getFromItem();
		joins = ps.getJoins();
		where = ps.getWhere();
		selectTarget = convertSelect(selectList); // Lists of strings representing the columns to be selected
		orderList = getOrderBy(orderBy); // List of strings representing which columns to be sorted by
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
	 * Method to get a list of select items in the format of [table name].[column name]
	 * If the query uses aliases, then replace the aliases with corresponding full
	 * table names.
	 * 
	 * @param select_list
	 * 				A list of SelectItem.
	 * @return An array of string representing each select item.
	 */
	public String[] getSelectTarget(List<SelectItem> select_list){
		String[] target = new String[select_list.size()];
		for (int i = 0; i < select_list.size(); i++){
			String selectItem = select_list.get(i).toString();
			
			if (select_list.get(0).toString().equals("*")){
				target[0] = "*";
				break;
			} else {
				target[i] = selectItem;
			}
		}
		return target;
	}
	
	/**
	 * Method to convert a list SelectItem to an array of String
	 * @param lst
	 * 			a list of SelectItem
	 * @return String
	 * 				representing the the list
	 */
	private String[] convertSelect(List<SelectItem> lst) {
		String[] ret = new String[lst.size()];
		int index = 0;
		for(SelectItem si : lst) {
			ret[index] = si.toString();
			index++;
		}
		return ret;
	}
		
	/**
	 * Method to get the mapping from an alias to its full table name.
	 * 
	 * @param ps
	 * 			PlainSelect Object of the query.
	 * @return HashMap<String,String>
	 * 				key is the alias and value is the its corresponding 
	 * 				full table name
	 */
	public HashMap<String,String> getAliases(PlainSelect ps){
		String alias = fromItem.getAlias();
		if (alias == null){
			return new HashMap<String, String>();
		}
		String[] from_string = fromItem.toString().split(" ");
		String table = from_string[0];
		HashMap<String, String> aliasMap = new HashMap<String,String>();
		aliasMap.put(alias, table);
		if (joins != null){
			for (Join join : joins){
				alias = join.getRightItem().getAlias();
				from_string = join.getRightItem().toString().split(" ");
				table = from_string[0];
				aliasMap.put(alias, table);
			}
		}
		return aliasMap;
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
	 * Method to get the root of a query plan
	 * @param db
	 * 				A DatabaseCatalog object
	 * @return root
	 * 				Root of the operator tree
	 */
	public PhysicalOperator getFinalOperator(DatabaseCatalog db){
	
		String tableName = null;
		String tableAlias = null;
		PhysicalOperator finalOperator = null;
		List<String> tableRefList = new ArrayList<String>();
		List<String> tableNameList = new ArrayList<String>();
		
		// scan operator. always assume that FROM includes at least one valid table
		tableName = getNameFromItem(fromItem);
		tableAlias = fromItem.getAlias();
		String tableRef = tableAlias == null ? tableName : tableAlias;
		finalOperator = new SequentialScanOperator(db.getTable(tableName), tableAlias);
		
		// select operator
		if (joins == null && where != null){
			finalOperator = new SelectOperator(finalOperator, where);
		}
		
		// join operator
		
		JoinExpressionVisitor jev = new JoinExpressionVisitor();
		if (where != null) {
			where.accept(jev);
		}
		tableRefList.add(tableRef);
		tableNameList.add(tableName);
		Expression firstSelect = jev.getSelectionCondition(tableRef);
		if(firstSelect != null) {
			finalOperator = new SelectOperator(finalOperator, firstSelect);
		}
		if(joins != null) {
			for(Join j : joins) {
				tableName = getNameFromItem(j.getRightItem());
				tableAlias = j.getRightItem().getAlias();
				tableRef = tableAlias == null? tableName : tableAlias;
				Table rightTable = db.getTable(tableName);
				PhysicalOperator rightChild = new SequentialScanOperator(rightTable, tableAlias);
				Expression select = jev.getSelectionCondition(tableRef);
				if(select != null) {
					rightChild = new SelectOperator(rightChild, select);
				}
				Expression joinCond = jev.getMultiJoinCondition(tableRef, tableRefList);
				finalOperator = new TupleNestedJoinOperator(finalOperator, rightChild, joinCond);
				tableRefList.add(tableRef);
				tableNameList.add(tableName);
			}
		}
		
		boolean isProjection = !selectTarget[0].equals("*");
		// project operator
		if (isProjection){
			finalOperator = new ProjectOperator(selectTarget, finalOperator);
		}
		
		
		// sort operator
		String[] fullOrderList;
		ArrayList<String> allColumns = new ArrayList<String>();
		for(int i = 0; i < tableNameList.size(); i++) {
			String[] theseColumns = db.getTable(tableNameList.get(i)).getQualifiedSchema(tableRefList.get(i));
			for(String c : theseColumns) {
				allColumns.add(c);
			}
		}
		if (orderList != null) {
			// Select * from sailor orderby s.a
			// get the table schema
			// create a new string[], add order_list to it
			// add the rest table schema to it
			if(isProjection) {
				fullOrderList = new String[selectTarget.length];
			} else {
				fullOrderList = new String[allColumns.size()];
			}
			for (int i = 0; i < orderList.size(); i++){
				fullOrderList[i] = orderList.get(i);
			}
			int position = orderList.size();
			if(isProjection) {
				for(String col : selectTarget) {
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
		
			finalOperator = new InMemorySortOperator(fullOrderList, finalOperator);
		}
		
		// duplicate eliminate operator
		if (distinct != null){
			if (orderList == null){
				if(isProjection) {
					fullOrderList = selectTarget;
				} else {
					fullOrderList = new String[allColumns.size()];
					fullOrderList = allColumns.toArray(fullOrderList);
				}
				finalOperator = new InMemorySortOperator(fullOrderList, finalOperator);
			}
			finalOperator = new DuplicateEliminateOperator((InMemorySortOperator) finalOperator);
		}
		
		return finalOperator;
	}
	
}
