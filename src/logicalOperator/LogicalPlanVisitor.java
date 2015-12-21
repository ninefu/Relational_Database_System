package logicalOperator;

/**
 * An interface to visit logical operators
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public interface LogicalPlanVisitor {
	
	public void visit(LogicalScan arg0);
	
	public void visit(LogicalSort arg0);
	
	public void visit(LogicalSelect arg0);
	
	public void visit(LogicalProject arg0);
	
	public void visit(LogicalJoin arg0);
	
	public void visit(LogicalDistinct arg0);

}
