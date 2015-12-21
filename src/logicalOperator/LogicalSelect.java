/**
 * 
 */
package logicalOperator;

import java.util.Map;
import java.util.Set;

import UnionFind.UnionFindElement;
import net.sf.jsqlparser.expression.Expression;

/**
 * A select operator to build the logical operator tree
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class LogicalSelect extends LogicalOperator {
	private Expression condition;
	private LogicalOperator op;
	private Expression extraSelect;
	private Map<String, UnionFindElement> unionFinds;
	private Set<UnionFindElement> unionFinds2;
	
	public LogicalSelect(Expression selectCondition, LogicalOperator operator, Map<String, UnionFindElement> ufes, Expression extra){
		condition = selectCondition;
		op = operator;
		unionFinds = ufes;
		extraSelect = extra;
	}
	
	public UnionFindElement unionFind(String att) {
		return unionFinds.get(att);
	}
	
	public void setUFs(Set<UnionFindElement> u) {
		unionFinds2 = u;
	}

	
	/**
	 * Method to get the selection condition
	 * @return Expression
	 * 			the selection condition
	 */
	public Expression condition() {
		return condition;
	}
	
	/**
	 * Method to get the child operator
	 * @return LogicalOperator
	 * 			this operator's child
	 */
	public LogicalOperator child() {
		return op;
	}

	/**
	 * Method to accept the LogicalPlanVisitor
	 */
	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);
	}
	
	public String toString(){
		return String.format("Select[%s]", condition.toString());
	}

	@Override
	public int childSize() {
		return 1;
	}
	
}
