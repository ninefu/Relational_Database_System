package logicalOperator;

/**
 * A distinct operator to build a logical operator tree.
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class LogicalDistinct extends LogicalOperator {
	
	private LogicalSort child;
	
	public LogicalDistinct(LogicalSort op) {
		child = op;
	}
	/**
	 * Method to get this operator's child
	 * @return child of this operator
	 */
	public LogicalOperator child() {
		return child;
	}

	/**
	 * Method to accept logicalPlanVisitor
	 */
	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);
	}
	
	public String toString(){
		return "DupElim";
	}
	@Override
	public int childSize() {
		return 1;
	}

}
