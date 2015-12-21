/**
 * 
 */
package ExpressionVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * JoinExpressionVisitor recursively walk the expression to get the 
 * selectionConditions and joinConditions.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class JoinExpressionVisitor implements ExpressionVisitor {
	private HashMap<String, Expression> selectionConditions; // A map of the string representation of a table to the expression that references it
	private HashMap<String, Expression> joinConditions;// A map of the string representation of two tables [table1],[table2] to the expression that references them
	private Stack<String> references; // Stack representing the table references encountered in the expression
	private Stack<String> tableReferences;
	private HashMap<String, ArrayList<String>> sortColumns;
	
	public JoinExpressionVisitor() {
		selectionConditions = new HashMap<String, Expression>();
		joinConditions = new HashMap<String, Expression>();
		sortColumns = new HashMap<String, ArrayList<String>>();
		references = new Stack<String>();
		tableReferences = new Stack<String>();
	}
	
	/**
	 * An inner class to get the sort order for the two chidren of SortMergeJoin
	 */
	private class TableValue {
		private ArrayList<String> leftSorts;
		private ArrayList<String> rightSorts;
		
		public TableValue(String leftsort, String rightsort) {
			leftSorts = new ArrayList<String>();
			rightSorts = new ArrayList<String>();
			leftSorts.add(leftsort);
			rightSorts.add(rightsort);
		}
		
		public void addLeft(String l) {
			if(!leftSorts.contains(l)) {
				leftSorts.add(l);
			}
		}
		
		public void addRight(String r) {
			if(!rightSorts.contains(r)) {
				rightSorts.add(r);
			}
		}
		
		public String[] getLeftSorts() {
			String[] res = new String[leftSorts.size()];
			return leftSorts.toArray(res);
		}
		
		public String[] getRightSorts() {
			String[] res = new String[rightSorts.size()];
			return rightSorts.toArray(res);
		}
	}
	
	
	/**
	 * Retrieves a selection condition on the given table
	 * 
	 * @param tableName 
	 * 					String name of the table to retrieve a selection condition on
	 * @return Expression 
	 * 				representing the selection condition on the table specified by tableName;
	 * 			null if none exists.
	 */
	public Expression getSelectionCondition(String tableName) {
		return selectionConditions.get(tableName);
	}
	
	/**
	 * Retrieves a join condition on the two given tables
	 * 
	 * @param left 
	 * 				One of the tables on which to find a join condition
	 * @param right 
	 * 				The table to be joined to the first; order does not matter
	 * @return Expression 
	 * 				representing the join condition on the table specified tables left and right.
	 * 			Null if none exists.
	 */
	public Expression getJoinCondition(String left, String right) {
		if(left.compareTo(right) > 0) { //Ensures that the key order for joins is always alphabetical
			String temp = right;
			right = left;
			left = temp;
		}
		String key = String.format("%s,%s", left, right);
		return joinConditions.get(key);
	}
	
	/**
	 * Retrieves an expression that references the right table and any of the tables
	 * specified in the given list.
	 * @param right 
	 * 			Table name that must be included in the join condition expression
	 * @param left 
	 * 				List of tables that may be included in the join condition expression; if any join condition
	 * 				references the right table and a table from this list, that expression will be added to
	 * 				the returned expression as a conjunct.
	 * @return Expression 
	 * 				that represents the join condition of the right table with any table in the left list.
	 */
	public Expression getMultiJoinCondition(String right, List<String> left) {
		Expression result = null;
		for(String s : left) {
			Expression e = getJoinCondition(s, right);
			if(e != null) {
				if(result == null) {
					result = e;
				} else {
					result = new AndExpression(result, e);
				}
			}
		}
		return result;	
	}
	
	/**
	 * Method to the sort order for a table
	 * @param table
	 * @return String[] representing the sorting order
	 */
	public String[] getSortOrderForTable(String table) {
		ArrayList<String> ordr = sortColumns.get(table);
		if(ordr != null) {
			String[] res = new String[ordr.size()];
			ordr.toArray(res);
			return res;
		}
		return null;
	}
	
	/**
	 * Method to the sort order for a list of tables
	 * @param tables
	 * @return String[] representing the sorting order
	 */
	public String[] getSortOrderForTable(List<String> tables) {
		ArrayList<String> cols = new ArrayList<String>();
		for(String table : tables) {
			String[] lab = getSortOrderForTable(table);
			if(lab != null) {
				for(String s : lab) {
					if(!cols.contains(s)) {
						cols.add(s);
					}
				}
			}
		}
		String[] finalOrder = new String[cols.size()];
		cols.toArray(finalOrder);
		return finalOrder;
	}
	
	
		
	/**
	 * Pops two references off of the stack and pushes a single
	 * String back onto the stack -- one that represents either
	 * that the result had a references (a string name of the table),
	 *  or did not (null).
	 */
	private void pushReferencesBinary() {
		String right = references.pop();
		String left = references.pop();
		String rightC = tableReferences.pop();
		String leftC = tableReferences.pop();
		if(right != null) {
			references.push(right);
		} else if(left != null) {
			references.push(left);
		} else {
			references.push(null);
		}
		if(rightC != null) {
			tableReferences.push(rightC);
		} else if(leftC != null) {
			tableReferences.push(leftC);
		} else {
			tableReferences.push(null);
		}
	}
	
	/**
	 * Puts addExp at key in joinConditions if there is no current
	 * expression at key, else creates a new AndExpression from the value
	 * already at key and addExp.
	 * @param key Reference to two tables of the from [table1],[table2]
	 * @param addExp Expression to be added to the conjunct expression 
	 * 					for this table pair
	 */
	private void putOrExtendJoin(String key, Expression addExp) {
		if(joinConditions.containsKey(key)) {
			Expression oldExp = joinConditions.get(key);
			Expression newExp = new AndExpression(oldExp, addExp);
			joinConditions.put(key, newExp);
		} else {
			joinConditions.put(key, addExp);
		}
	}
	
	/**
	 * Puts addExp at key in selectionConditions if there is no current
	 * expression at key, else creates a new AndExpression from the value
	 * already at key and addExp.
	 * @param key Reference to a table, the table's name
	 * @param addExp Expression to be added to the conjunct expression for
	 * 					this table.
	 */
	private void putOrExtendSelection(String key, Expression addExp) {
		if(selectionConditions.containsKey(key)) {
			Expression oldExp = selectionConditions.get(key);
			Expression newExp = new AndExpression(oldExp, addExp);
			selectionConditions.put(key, newExp);
		} else {
			selectionConditions.put(key, addExp);
		}
	}
	
	/**
	 * Takes in an expression with two children and pops two references off of the stack
	 * If both are references (they are not null), adds an entry to the joinConditions map.
	 * If only one is an actual reference, adds an entry to the selectionCondition map
	 * @param arg0 Candidate expression for a possible conjunct to a join condition on two tables
	 * 				or selection condition on one table. 
	 */
	private void processConditions(Expression arg0) {
		String right = references.pop();
		String left = references.pop();
		if(left !=null && right !=null) {
			if(left.compareTo(right) > 0) { //Ensures that the key order for joins is always alphabetical
				String temp = right;
				right = left;
				left = temp;
			}
			String key = String.format("%s,%s", left, right);
			putOrExtendJoin(key, arg0);
		} else if(left != null) {
			putOrExtendSelection(left, arg0);
		} else if(right != null) {
			putOrExtendSelection(right,  arg0);
		}
	}
	
	private void putOrExtendSortBy(String key, String col) {
		if(sortColumns.containsKey(key)) {
			ArrayList<String> v = sortColumns.get(key);
			if(!v.contains(col)) {
				v.add(col);
			}
		} else {
			ArrayList<String> v = new ArrayList<String>();
			v.add(col);
			sortColumns.put(key, v);
		}
	}
	
	private void processEqualityConditions(EqualsTo arg0) {
		String right = tableReferences.pop();
		String left = tableReferences.pop();
		if(right != null && left != null) {
			String[] rightTok = right.split(",");
			String[] leftTok = left.split(",");
			String rightTable = rightTok[0];
			String leftTable = leftTok[0];
			String rightCol = rightTok[1];
			String leftCol = leftTok[1];
			putOrExtendSortBy(rightTable, rightCol);
			putOrExtendSortBy(leftTable, leftCol);
		}	
	}
	
	
	/**
	 * Pushes a null to the stack, indicating an operand is not a table reference.
	 */
	@Override
	public void visit(DoubleValue arg0) {
		references.push(null);
		tableReferences.push(null);
	}

	/**
	 * Pushes a null to the stack, indicating an operand is not a table reference.
	 */
	@Override
	public void visit(LongValue arg0) {
		references.push(null);
		tableReferences.push(null);
	}
	
	/**
	 * Recursively visits the left and right expressions, then processes
	 * any table references in those expressions.
	 */
	@Override
	public void visit(Addition arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		pushReferencesBinary();
	}

	/**
	 * Recursively visits the left and right expressions, then processes
	 * any table references in those expressions.
	 */
	@Override
	public void visit(Division arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		pushReferencesBinary();	
	}

	/**
	 * Recursively visits the left and right expressions, then processes
	 * any table references in those expressions.
	 */
	@Override
	public void visit(Multiplication arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		pushReferencesBinary();	
	}

	/**
	 * Recursively visits the left and right expressions, then processes
	 * any table references in those expressions.
	 */
	@Override
	public void visit(Subtraction arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		pushReferencesBinary();	
	}

	/**
	 * Recursively visits the left and right expressions, then processes
	 * any table references in those expressions.
	 */
	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}
	
	/**
	 * Recursively visits the left and right expressions, then processes
	 * any table references in those expressions.
	 */
	@Override
	public void visit(EqualsTo arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		processConditions(arg0);
		processEqualityConditions(arg0);
	}

	/**
	 * Recursively visits the left and right expressions, then processes
	 * any table references in those expressions.
	 */
	@Override
	public void visit(GreaterThan arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		processConditions(arg0);
		tableReferences.pop();
		tableReferences.pop();
	}

	/**
	 * Recursively visits the left and right expressions, then processes
	 * any table references in those expressions.
	 */
	@Override
	public void visit(GreaterThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		processConditions(arg0);
		tableReferences.pop();
		tableReferences.pop();
	}
	
	/**
	 * Recursively visits the left and right expressions, then processes
	 * any table references in those expressions.
	 */
	@Override
	public void visit(MinorThan arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		processConditions(arg0);
		tableReferences.pop();
		tableReferences.pop();
	}

	/**
	 * Recursively visits the left and right expressions, then processes
	 * any table references in those expressions.
	 */
	@Override
	public void visit(MinorThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		processConditions(arg0);	
		tableReferences.pop();
		tableReferences.pop();
	}

	/**
	 * Recursively visits the left and right expressions, then processes
	 * any table references in those expressions.
	 */
	@Override
	public void visit(NotEqualsTo arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		processConditions(arg0);
		tableReferences.pop();
		tableReferences.pop();
	}

	/**
	 * Pushes a table reference (the name of that table) to the stack
	 */
	@Override
	public void visit(Column arg0) {
		String ref = arg0.getTable().getAlias();
		if (ref == null){
			ref = arg0.getTable().getName();
		}
		references.push(ref);
		String col = String.format("%s.%s", ref, arg0.getColumnName());
		tableReferences.push(String.format("%s,%s", ref, col));
	}

	/**
	 * Method to visit NullValue. Not supported.
	 */
	@Override
	public void visit(NullValue arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit Function. Not supported.
	 */
	@Override
	public void visit(Function arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit InverseExpression. Not supported.
	 */
	@Override
	public void visit(InverseExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit JdbcParameter. Not supported.
	 */
	@Override
	public void visit(JdbcParameter arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");	
	}

	/**
	 * Method to visit DateValue. Not supported.
	 */
	@Override
	public void visit(DateValue arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit TimeValue. Not supported.
	 */
	@Override
	public void visit(TimeValue arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit TimestampValue. Not supported.
	 */
	@Override
	public void visit(TimestampValue arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit Parenthesis. Not supported.
	 */
	@Override
	public void visit(Parenthesis arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit StringValue. Not supported.
	 */
	@Override
	public void visit(StringValue arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit OrExpression. Not supported.
	 */
	@Override
	public void visit(OrExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit Between. Not supported.
	 */
	@Override
	public void visit(Between arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit InExpression. Not supported.
	 */
	@Override
	public void visit(InExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit IsNullExpression. Not supported.
	 */
	@Override
	public void visit(IsNullExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit LikeExpression. Not supported.
	 */
	@Override
	public void visit(LikeExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit SubSelect. Not supported.
	 */
	@Override
	public void visit(SubSelect arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
		
	}

	/**
	 * Method to visit CaseExpression. Not supported.
	 */
	@Override
	public void visit(CaseExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");	
	}

	/**
	 * Method to visit WhenClause. Not supported.
	 */
	@Override
	public void visit(WhenClause arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
	}

	/**
	 * Method to visit ExistsExpression. Not supported.
	 */
	@Override
	public void visit(ExistsExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
	}
	
	/**
	 * Method to visit AllComparisonExpression. Not supported.
	 */
	@Override
	public void visit(AllComparisonExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");	
	}

	/**
	 * Method to visit AnyComparisonExpression. Not supported.
	 */
	@Override
	public void visit(AnyComparisonExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");	
	}

	/**
	 * Method to visit Concat. Not supported.
	 */
	@Override
	public void visit(Concat arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
	}

	/**
	 * Method to visit Matches. Not supported.
	 */
	@Override
	public void visit(Matches arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
	}

	/**
	 * Method to visit BitwiseAnd. Not supported.
	 */
	@Override
	public void visit(BitwiseAnd arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
	}

	/**
	 * Method to visit BitwiseOr. Not supported.
	 */
	@Override
	public void visit(BitwiseOr arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
	}

	/**
	 * Method to visit BitwiseXor. Not supported.
	 */
	@Override
	public void visit(BitwiseXor arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");
	}

}
