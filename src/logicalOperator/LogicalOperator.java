/**
 * 
 */
package logicalOperator;

/** 
 *  An abstract Logical Operator class
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 * TODO: If we can't think of anything concrete implementation-wise to go in this class,
 * 		 we should just turn it into an interface instead.
 */
public abstract class LogicalOperator {
	
	public abstract void accept(LogicalPlanVisitor visitor);

	public abstract LogicalOperator child();
	
	public abstract int childSize();
	
}
