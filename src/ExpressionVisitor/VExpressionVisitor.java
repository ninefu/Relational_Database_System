package ExpressionVisitor;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
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

public class VExpressionVisitor implements ExpressionVisitor {
	
	private Stack<String> references;
	private HashMap<String, ArrayList<String>> equalityReferences;
	private Set<Entry<String, String>> equalityPairs;
	
	public VExpressionVisitor() {
		references = new Stack<String>();
		equalityPairs = new HashSet<Entry<String, String>>();
		equalityReferences = new HashMap<String, ArrayList<String>>();
	}
	
	public List<String> getEqualityReferences(String a) {
		return equalityReferences.get(a);
	}
	
	public Set<Entry<String, String>> getEqualityPairs() {
		return equalityPairs;
	}

	
	/**
	 * Pushes a sentinel null value, since we
	 * don't really care about a comparison to
	 * a number.
	 */
	@Override
	public void visit(LongValue arg0) {
		references.push(null);

	}
	
	/**
	 * Pushes a reference to the table an column of this expression
	 * @param arg0
	 */
	@Override
	public void visit(Column arg0) {
		references.push(arg0.getWholeColumnName());
	}
	
	/**
	 * Visits the left and right children on an AND expression.
	 * @param arg0 The AndExpression to be visited
	 */
	@Override
	public void visit(AndExpression arg0) {
		arg0.getRightExpression().accept(this);
		arg0.getLeftExpression().accept(this);

	}
	
	/**
	 * Visits the left and right children; if the results pushed
	 * by either is null, this indicates that the comparison involved a 
	 * number and we skip it. If the children on both sides of the comparison
	 * were columns, we add them to an equality references table.
	 * @param arg0
	 */
	@Override
	public void visit(EqualsTo arg0) {
		arg0.getRightExpression().accept(this);
		arg0.getLeftExpression().accept(this);
		String left = references.pop();
		String right = references.pop();
		if(left != null && right != null) {
			if(left.compareTo(right) > 0) {
				String temp = right;
				right = left;
				left = temp;
			}
			Entry<String, String> ep = new AbstractMap.SimpleEntry<String, String>(left, right);
			if(!equalityPairs.contains(ep)) {
				equalityPairs.add(ep);
			}
			if(!equalityReferences.containsKey(left)) {
				equalityReferences.put(left, new ArrayList<String>());
			}
			if(!equalityReferences.containsKey(right)) {
				equalityReferences.put(right, new ArrayList<String>());
			}
			equalityReferences.get(left).add(right);
			equalityReferences.get(right).add(left);
		}

	}

	@Override
	public void visit(NullValue arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(Function arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(InverseExpression arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(JdbcParameter arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(DoubleValue arg0) {
		references.push(null);

	}

	@Override
	public void visit(DateValue arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(TimeValue arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(TimestampValue arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(Parenthesis arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(StringValue arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(Addition arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(Division arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(Multiplication arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(Subtraction arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}


	@Override
	public void visit(OrExpression arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(Between arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}


	@Override
	public void visit(GreaterThan arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(InExpression arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(IsNullExpression arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(LikeExpression arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(MinorThan arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(MinorThanEquals arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(NotEqualsTo arg0) {
		arg0.getRightExpression().accept(this);
		arg0.getLeftExpression().accept(this);
		String left = references.pop();
		String right = references.pop();
		if(left != null && right != null) {
			if(left.compareTo(right) > 0) {
				String temp = right;
				right = left;
				left = temp;
			}
			Entry<String, String> ep = new AbstractMap.SimpleEntry<String, String>(left, right);
			if(!equalityPairs.contains(ep)) {
				equalityPairs.add(ep);
			}
			if(!equalityReferences.containsKey(left)) {
				equalityReferences.put(left, new ArrayList<String>());
			}
			if(!equalityReferences.containsKey(right)) {
				equalityReferences.put(right, new ArrayList<String>());
			}
			equalityReferences.get(left).add(right);
			equalityReferences.get(right).add(left);
		}
//		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(SubSelect arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(CaseExpression arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(WhenClause arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(ExistsExpression arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(Concat arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(Matches arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(BitwiseAnd arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(BitwiseOr arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

	@Override
	public void visit(BitwiseXor arg0) {
		throw new UnsupportedOperationException("VExpression visitor does not support this type of expression.");

	}

}
