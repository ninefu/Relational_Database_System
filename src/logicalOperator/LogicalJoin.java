/**
 * 
 */
package logicalOperator;

import java.util.ArrayList;
import java.util.HashMap;

import ExpressionVisitor.JoinExpressionVisitor;
import ExpressionVisitor.UnionFindExpressionVisitor;
import UnionFind.UnionFindElement;
import database.DatabaseCatalog;
import database.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;

/**
 * A join operator to build the logical operator tree. It has as many child operators as
 * the number of tables in the FROM clause. Unusable conditions will be stored in remainderJoins.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class LogicalJoin extends LogicalOperator {
	private Expression remainderJoins; // like R.A <> U.B
	private ArrayList<LogicalOperator> children; // scan op or select op
	private UnionFindExpressionVisitor ufv;
	private JoinExpressionVisitor jev;
	private DatabaseCatalog db;
	private ArrayList<String> names; // in the same order as the FROM clause
	private ArrayList<String> refs; // in the same order as the FROM clause
//	private HashMap<String, ArrayList<Expression>> connected; 
	private HashMap<String, ArrayList<Expression>> single; // attr <>= val, or table2.attr1 = table2.attr2 key is table name,
	private HashMap<String, ArrayList<Expression>> otherJoin; //R.A <> U.B
	private HashMap<String, ArrayList<Expression>> equalJoin; // table1.attr1 = table2.attr2, key is table name
//	private ArrayList<Expression> diffEquality; // like table1.attr1 = table2.attr2
	private HashMap<String, HashMap<String, UnionFindElement>> unionFindMap; 
	
	/**
	 * Construct a LogicalJoin operator with a list of child operators and remaining unusable conditions
	 * @param tableName，a list of true table names,
	 * @param tableRef, a list of table names, alias if exists
	 * @param Ufv, UnionFindExpressionVisitor
	 * @param database, DatabaseCatalog
	 */
	public LogicalJoin(ArrayList<String> tableName, ArrayList<String> tableRef, UnionFindExpressionVisitor Ufv, JoinExpressionVisitor j, DatabaseCatalog database){
		assert tableName.size() == tableRef.size();
		
		remainderJoins = Ufv.getRemainderJoins();
		children = new ArrayList<LogicalOperator>();
		ufv = Ufv;
		db = database;
		names = tableName;
		refs = tableRef;
//		connected = new HashMap<String, ArrayList<Expression>>();
		single = new HashMap<String, ArrayList<Expression>>();
		unionFindMap = new HashMap<String, HashMap<String, UnionFindElement>>(); 
//		diffEquality = new ArrayList<Expression>();
		otherJoin = ufv.getNotEqualJoin();
		equalJoin = new HashMap<String, ArrayList<Expression>>();
		jev = j;
		
		// build base scan operators. The key is the alias
		HashMap<String, LogicalScan> scans = buildScans();
		// reconstruct expression from the visitor and organized them by table alias
		buildExpression();
		buildUnionFindMap();
		// build child operator
		produceChildren(scans);		
	}
	
	/**
	 * Get usable and unusable conditions from the visitor, merge them if necessary, then
	 * general a child operator, either a scan operator if no conditions at all or a selection
	 * operator if either condition exists
	 * @param scans, a HashMap where key is the alias of a table and the value 
	 * is a LogicalScan operator
	 */
	public void produceChildren(HashMap<String, LogicalScan> scans){
		for (String table : refs){
			HashMap<String, UnionFindElement> ufes = unionFindMap.get(table);
			LogicalOperator child = null;
			Expression condition;
			// no condition for this table, leaf/scan operator
			if (!single.containsKey(table) && ufv.getRemainderSelect(table) == null){
				child = scans.get(table);
			// no residual condition
			}else if (ufv.getRemainderSelect(table) == null){
				condition = ufv.mergeExpressions(single.get(table));
				child = new LogicalSelect(condition,scans.get(table), ufes, null);
			// no usuable conditions but only residual condition, like attr <> val
			}else if (!single.containsKey(table)){
				condition = ufv.getRemainderSelect(table);
				child = new LogicalSelect(condition,scans.get(table), ufes, condition);
			// have both type of conditions
			}else{
//				ArrayList<Expression> con = connected.get(table);
//				con.add(ufv.getRemainderSelect(table));
				Expression extra = ufv.getRemainderSelect(table);
				condition = ufv.mergeExpressions(single.get(table));
				child = new LogicalSelect(condition,scans.get(table), ufes, extra);
			}
			if (child instanceof LogicalSelect){
				((LogicalSelect)child).setUFs(ufv.getUnionFind().findForTable(table));
			}
			children.add(child);	
		}
	}
	
	private void buildUnionFindMap() {
		for(UnionFindElement ufe : ufv.getUnionFind().getUnionFindElements()) {
			for(Column col : ufe.getAttributes()) {
				String tableName = col.toString().split("\\.")[0];
				if(!unionFindMap.containsKey(tableName)) {
//					unionFindMap.put(tableName, new HashMap<String, UnionFindElement>());
					HashMap<String, UnionFindElement> ele = new HashMap<String, UnionFindElement>();
					ele.put(col.getWholeColumnName(), ufe);
					unionFindMap.put(tableName, ele);
				}else{
					HashMap<String, UnionFindElement> ele = unionFindMap.get(tableName);
					ele.put(col.getWholeColumnName(), ufe);
					unionFindMap.put(tableName, ele);
				}
//				unionFindMap.get(tableName).put(col.getWholeColumnName(), ufe);
			}
		}

	}
	
	/**
	 * Build expressions from UnionFind elements in the visitor and organize
	 * them by table alias
	 */
	private void buildExpression(){
		// get UnionFindElements from the visitor
		ArrayList<UnionFindElement> eles = ufv.getUnionFind().getUnionFindElements();
		
		for (UnionFindElement ele : eles){
			// get attributes and boundaries
			ArrayList<Column> attrs = ele.getAttributes();
			int lower = ele.getLower();
			int upper = ele.getUpper();
			Integer equality = ele.getEquality();
			
			// get numerical constraint
			if (equality != (Integer) null){
				numericEquality(attrs,equality, ele);
			}else{
				lowerUpper(attrs,lower,upper, ele);
			}
			
			// get attr = attr constraint
			attrEquality(attrs, ele);
		}
	}
	
	
	/**
	 * produce attr = attr condition for attributes within an UnionFindElement
	 * @param attrs, an ArrayList of Column sharing the same bounds
	 */
	public void attrEquality(ArrayList<Column> attrs, UnionFindElement ufe){
		for (int i = 0; i < attrs.size(); i++){
			for (int j = i + 1; j < attrs.size(); j++){
				EqualsTo cur = new EqualsTo();
				Column left = attrs.get(i);
				Column right = attrs.get(j);
				cur.setLeftExpression(left);
				cur.setRightExpression(right);
				
				String leftAlias = left.toString().split("\\.")[0];
				String rightAlias = right.toString().split("\\.")[0];
				if (leftAlias.equals(rightAlias)){
					updateExpression(leftAlias, cur);
				}else{
//					diffEquality.add(cur);
					if (equalJoin.containsKey(leftAlias)){
						ArrayList<Expression> exp = equalJoin.get(leftAlias);
						exp.add(cur);
						equalJoin.put(leftAlias,exp);
					}else{
						ArrayList<Expression> exp = new ArrayList<Expression>();
						exp.add(cur);
						equalJoin.put(leftAlias,exp);
					}
					
					if (equalJoin.containsKey(rightAlias)){
						ArrayList<Expression> exp = equalJoin.get(rightAlias);
						exp.add(cur);
						equalJoin.put(rightAlias,exp);
					}else{
						ArrayList<Expression> exp = new ArrayList<Expression>();
						exp.add(cur);
						equalJoin.put(rightAlias,exp);
					}	
				}
				
//				updateExpression(rightAlias,cur);		
			}
		}
	}
	
	/**
	 * produce attr > val and/or attr < val condition for each attribute in 
	 * an UnionFindElement
	 * @param attrs, an arrayList of Column object
	 * @param lower, the lower bound of the element
	 * @param upper, the upper bound of the element
	 */
	public void lowerUpper(ArrayList<Column> attrs, int lower, int upper, UnionFindElement ufe){
		for (Column attr : attrs){
			String alias = attr.toString().split("\\.")[0];
			if (lower != Integer.MIN_VALUE){
				GreaterThanEquals cur = new GreaterThanEquals();
				LongValue val = new LongValue(((Integer) lower).toString());
				cur.setLeftExpression(attr);
				cur.setRightExpression(val);
				updateExpression(alias,cur);
			}
			if (upper != Integer.MAX_VALUE){
				MinorThanEquals cur = new MinorThanEquals();
				LongValue val = new LongValue(((Integer) upper).toString());
				cur.setLeftExpression(attr);
				cur.setRightExpression(val);
				updateExpression(alias,cur);
			}
		}
	}
	
	/**
	 * produce attr = val condition for each attribute in an UnionFindElement
	 * @param attrs, an arrayList of Column object
	 * @param equality, Integer, the equality constraint
	 */
	public void numericEquality(ArrayList<Column> attrs, Integer equality, UnionFindElement ufe){
		for (Column attr : attrs){
			EqualsTo cur = new EqualsTo();
			LongValue val = new LongValue(equality.toString());
			cur.setLeftExpression(attr);
			cur.setRightExpression(val);
			String alias = attr.toString().split("\\.")[0];
			updateExpression(alias,cur);
		}
	}
	
	/**
	 * A helper function to add an expression that belongs to an table (alias)
	 * @param alias, the alias for a table
	 * @param cur, an expression
	 */
	public void updateExpression(String alias, Expression cur){
		if (single.containsKey(alias)){
//			single.get(alias).add(cur);
			ArrayList<Expression> list = single.get(alias);
			list.add(cur);
			single.put(alias, list);
		}else{
			ArrayList<Expression> list = new ArrayList<Expression>();
			list.add(cur);
			single.put(alias, list);
		}
	}
	
	/**
	 * Build the base scan operator
	 * @return a HashMap where the key is the alias and the value is a LogicalScan
	 * operator
	 */
	public HashMap<String, LogicalScan> buildScans(){
		HashMap<String, LogicalScan> scans = new HashMap<String, LogicalScan>();
		for (int i = 0; i < refs.size(); i++){
			Table baseTable = db.getTable(names.get(i));
			scans.put(refs.get(i), new LogicalScan(baseTable, refs.get(i)));
		}
		return scans;
	}
	
	/**
	 * Method to get the join condition
	 * @return Expression
	 * 			representing the join condition
	 */
	public Expression getRemainderJoin() {
		return remainderJoins;
	}

	/**
	 * Method to accept the LogicalPlanVisitor
	 */
	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);
	}
	
//	/**
//	 * Only for testing, feel free to change or comment it.
//	 */
//	public String toString(){
//		StringBuilder sb = new StringBuilder();
//		sb.append(remainderJoins).toString();
//		sb.append("\n");
//		if (diffEquality.size() > 0){
//			for (Expression exp : diffEquality){
//				sb.append(exp.toString());
//				sb.append(" ");
//			}
//		}
//		for (LogicalOperator op : children){
//			sb.append("\n");
//			if (op instanceof LogicalSelect){
//				sb.append(((LogicalScan) ((LogicalSelect) op).child()).table().getName());
//				sb.append(": ");
//				sb.append(((LogicalSelect) op).condition().toString());
//			}else if (op instanceof LogicalScan){
//				sb.append(((LogicalScan) op).alias());
//			}
//		}
//		return sb.toString();
//	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		if (remainderJoins == null){
			sb.append("Join[" + null + "]");
		}else{
			sb.append("Join[" + remainderJoins.toString() + "]");
		}
		if (ufv.getUnionFind().getUnionFindElements().size() != 0){
			sb.append("\n");
			sb.append(ufv.getUnionFind().toString());
		}
		return sb.toString();
	}
	
	/**
	 * Get the children of this operator
	 * @return an arrayList of child operators
	 */
	public ArrayList<LogicalOperator> getChildren(){
		return children;
	}

	/**
	 * Not supported. To get the child operator of LogicalJoin,
	 * please use getChildren().
	 */
	@Override
	public LogicalOperator child() {
		return null;
	}

	/**
	 * Get the number of child operators
	 * @return int, number of child operators
	 */
	@Override
	public int childSize() {
		return children.size();
	}

	public Expression getEqualJoin(String tableName){
		if (equalJoin.containsKey(tableName)){
			return ufv.mergeExpressions(equalJoin.get(tableName));
		}
		return null;
	}
	
	public Expression getLastJoin(String tableName){
		ArrayList<Expression> left = new ArrayList<Expression>();
		if (equalJoin.containsKey(tableName)){
			left.addAll(equalJoin.get(tableName));
		}
		if (otherJoin.containsKey(tableName)){
			left.addAll(otherJoin.get(tableName));
		}
		
		return ufv.mergeExpressions(left);
	}
	
	public Expression getEqualCondition(String leftTable, String rightTable){
		ArrayList<Expression> left = new ArrayList<Expression>();
		if (equalJoin.containsKey(leftTable)){
			left.addAll(equalJoin.get(leftTable));
		}
		
		ArrayList<Expression> right = new ArrayList<Expression>();
		if (equalJoin.containsKey(rightTable)){
			right.addAll(equalJoin.get(rightTable));
		}
		
		ArrayList<Expression> overlap = getOverlap(left, right);
		return ufv.mergeExpressions(overlap);
	}
	
	public Expression getJoinCondition(String leftTable, String rightTable){
		ArrayList<Expression> left = new ArrayList<Expression>();
		if (equalJoin.containsKey(leftTable)){
			left.addAll(equalJoin.get(leftTable));
		}
		if (otherJoin.containsKey(leftTable)){
			left.addAll(otherJoin.get(leftTable));
		}
		
		ArrayList<Expression> right = new ArrayList<Expression>();
		if (equalJoin.containsKey(rightTable)){
			right.addAll(equalJoin.get(rightTable));
		}
		if (otherJoin.containsKey(rightTable)){
			right.addAll(otherJoin.get(rightTable));
		}
		
		ArrayList<Expression> overlap = getOverlap(left, right);
		return ufv.mergeExpressions(overlap);
	}
	
	public ArrayList<Expression> getOverlap(ArrayList<Expression> left, ArrayList<Expression> right){
		ArrayList<Expression> res = new ArrayList<Expression>();
		for (Expression e : left){
			if (right.contains(e)){
				res.add(e);
			}
		}
		return res;
	}
	
	public ArrayList<String> getNames(){
		return names;
	}
	
	public ArrayList<String> getRefs(){
		return refs;
	}
}
