package physicalOperator;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import ExpressionVisitor.IndexScanExpressionVisitor;
import ExpressionVisitor.JoinExpressionVisitor;
import UnionFind.UnionFindElement;
import database.Table;
import index.BTreeIndex;
import index.IndexConfig;
import logicalOperator.LogicalDistinct;
import logicalOperator.LogicalJoin;
import logicalOperator.LogicalOperator;
import logicalOperator.LogicalPlanVisitor;
import logicalOperator.LogicalProject;
import logicalOperator.LogicalScan;
import logicalOperator.LogicalSelect;
import logicalOperator.LogicalSort;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import util.Constants;

/**
 * A visitor that visitor the logical operator tree and build a physical
 * query plan based on the configuration files
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class PhysicalPlanBuilder implements LogicalPlanVisitor {
	
	private Stack<PhysicalOperator> operators;
	private PhysicalPlanConfig config;
	private IndexConfig inConfig;
	private static int joinBufferPages = 3;
	
	private static int sortNum = 0;
	
	public PhysicalPlanBuilder(PhysicalPlanConfig conf,IndexConfig ic) {
		operators = new Stack<PhysicalOperator>();
		config = conf;
		inConfig = ic;
	}
	
	/**
	 * Get the root of the operator tree
	 * @return A physical operator representing the root of the plan
	 */
	public PhysicalOperator getPlan() {
		return operators.peek();
	}

	/**
	 * Method to visit a LogicalScan operator and build a physical operator
	 */
	@Override
	public void visit(LogicalScan arg0) {
		operators.push(new SequentialScanOperator(arg0.table(), arg0.alias(), config.isHumanReadable()));
	}

	/**
	 * Method to visit a LogicalSort Operator and build a physical operator
	 * according to the configuration file.
	 */
	@Override
	public void visit(LogicalSort arg0) {
		arg0.child().accept(this);
		PhysicalOperator sortOp = null;
		// hard code the sorting algorithm
		new File(config.getTempDir()+"\\"+sortNum).mkdir();
		sortOp = new ExternalSortOperator(config.getSortBufferPages(),config.isHumanReadable(),config.getTempDir()+"\\"+sortNum,arg0.order(),operators.pop());
		sortNum++;
		
//		switch(config.getSortMethod()) {
//		case EXTERNAL:
//			new File(config.getTempDir()+"\\"+sortNum).mkdir();
//			sortOp = new ExternalSortOperator(config.getSortBufferPages(),config.isHumanReadable(),config.getTempDir()+"\\"+sortNum,arg0.order(),operators.pop());
//			sortNum++;
//			break;
//		case MEMORY:
//			sortOp = new InMemorySortOperator(arg0.order(), operators.pop());
//			break;
//		}
		operators.push(sortOp);
	}
	
	private double reductionFactor(Table baseTable, UnionFindElement constraints, String att) {
		int lowAtt = baseTable.getLow(att);
		int highAtt = baseTable.getHigh(att);
		int range = highAtt - lowAtt + 1;
		Integer lowSel = constraints.getLower() == Integer.MIN_VALUE ? null : constraints.getLower();
		Integer highSel = constraints.getUpper() == Integer.MAX_VALUE ? null : constraints.getUpper();
		if(lowSel == null && highSel == null) {
			return 1; // No reduction factor, this covers the entire range.
		} else if(lowSel == null) {
			// <= some value
			return (highSel - lowAtt + 1)/range;
		} else if(highSel == null) {
			// >= some value
			return (highAtt - lowSel + 1)/range;
		} else {
			// some value <= x <= some value
			return (highAtt - lowAtt + 1)/range;
		}
	}
	

	/**
	 * Method to visit a LogicalSelect Operator and build a physical operator
	 */
	@Override
	public void visit(LogicalSelect arg0) {
		arg0.child().accept(this);
		PhysicalOperator child = operators.pop();
		ScanOperator c = (ScanOperator)child;
		Table baseTable = ((ScanOperator)child).getBaseTable();
		int numIndexes = inConfig.getIndex(baseTable.getName()).values().size();
		if (numIndexes == 0){
			operators.push(c);
		}else{
			BTreeIndex[] indexes = new BTreeIndex[numIndexes];
			double[] costs = new double[numIndexes + 1];
			inConfig.getIndex(baseTable.getName()).values().toArray(indexes);
			
			int numTuples = baseTable.getNumTuples();
			int pages = (int)Math.ceil(numTuples/Constants.pageSize);
			
			int i = 0;
			for(BTreeIndex index : indexes) {
				String att = index.indexedOn();
				UnionFindElement constraints = arg0.unionFind(att);
				if(constraints != null) {
					int leaves = index.numLeaves();
					double rFactor = reductionFactor(baseTable, constraints, att);
					double cost = 0.0;
					if(index.isClustered()) {
						cost = 3 + pages * rFactor;
					} else {
						cost = 3 + (leaves * rFactor) + (numTuples * rFactor);
					}
					costs[i] = cost;
					i++;
				}
				costs[i] = pages;
				double minCost = costs[0];
				int minIndex = 0;
				int j = 0;
				for(; j < i+1; j++) {
					if(costs[j] < minCost) {
						minCost = costs[j];
						minIndex = j;
					}
				}
				BTreeIndex winner = indexes[minIndex];
				String attribute = winner.indexedOn();
				ScanOperator so = null;
				if(minIndex == i) {
					so = (ScanOperator)child;
					operators.push(new SelectOperator(so, arg0.condition()));
				} else {
					UnionFindElement ufe = arg0.unionFind(attribute);
					IndexScanExpressionVisitor ise = new IndexScanExpressionVisitor(attribute);
					so = new IndexScanOperator(c.getBaseTable(), c.getAlias(), ufe.getLower(), ufe.getUpper(), winner);
					arg0.condition().accept(ise);
					operators.push(new SelectOperator(so, ise.getRemainderCondition()));
				}
			}
		}
		
		// Calculate scan cose
	
		//if the third line in plan_builder_config.txt is 1, i.e., we need to use indexes if possible
		//make sure the selection operator has a leaf/scan as its child
//		if (config.isIndexed() && child instanceof ScanOperator && inConfig != null){
//			Table baseTable = ((ScanOperator)child).getBaseTable();
//			String alias = ((ScanOperator)child).getAlias();
//			String tableName = baseTable.getName();
//			double[] bounds;
//			BTreeIndex candidate = inConfig.getIndex(tableName);
//			//check if there is an index on this table;
//			if (candidate != null){
//				String indexedColumn = candidate.indexedOn();
//				IndexScanExpressionVisitor inVis = new IndexScanExpressionVisitor(indexedColumn);
//				arg0.condition().accept(inVis);
//				bounds = inVis.getHighAndLow();
//				// check if we can use the index on this select condition
//				if (bounds[0] != Double.MAX_VALUE || bounds[1] != -Double.MAX_VALUE){
//					operators.push(new IndexScanOperator(baseTable,alias,(int)bounds[0],(int)bounds[1],inConfig));
//				}else{
//					operators.push(new SelectOperator(child, arg0.condition()));
//				}
//				if (inVis.getRemainderCondition() != null){
//					operators.push(new SelectOperator(operators.pop(),inVis.getRemainderCondition()));
//				}
//			}else{
//				operators.push(new SelectOperator(child, arg0.condition()));
//			}
//		} else {	
//			operators.push(new SelectOperator(child, arg0.condition()));
//		}
		
	}

	/**
	 * Method to visit a LogicalProject operator and build a physical operator
	 */
	@Override
	public void visit(LogicalProject arg0) {
		arg0.child().accept(this);
		operators.push(new ProjectOperator(arg0.columns(), operators.pop()));
	}

	/**
	 * Method to visit a LogicalJoin operator and build a physical operator
	 * according to the configuration file
	 */
	@Override
	public void visit(LogicalJoin arg0) {
		ArrayList<LogicalOperator> children = arg0.getChildren();
		JoinOperator joinOp = null;
		if (children.size() == 1){
			children.get(0).accept(this);
			PhysicalOperator op = operators.pop();
			operators.push(op);
		}else{
			ArrayList<String> names = arg0.getNames();
			ArrayList<String> refs = arg0.getRefs();
			children.get(0).accept(this);
			children.get(1).accept(this);
			PhysicalOperator left = operators.pop();
			PhysicalOperator right = operators.pop();
			Expression condition = arg0.getJoinCondition(refs.get(0), refs.get(1));
//			Expression equalJoin = arg0.getEqualCondition(refs.get(0), refs.get(1));
			joinOp = new BlockNestedLoopJoinOperator(joinBufferPages,left,right,condition);
			
			for (int i = 2; i < children.size(); i++){
				children.get(i).accept(this);
				AndExpression con = new AndExpression();
				con.setLeftExpression(condition);
				con.setRightExpression(arg0.getLastJoin(refs.get(i)));
				condition = con;
				joinOp = new BlockNestedLoopJoinOperator(joinBufferPages,joinOp,operators.pop(),condition);
			}
			operators.push(joinOp);		
		}

//		arg0.innerChild().accept(this); // Push inner child first so that we can pop outer child first
//		arg0.outerChild().accept(this);
//		JoinOperator joinOp = null;
//		int pages;
//		switch(config.getJoinMethod()) {
//		case BNLJ:
////			pages = config.getJoinBufferPages();
////			joinOp = new BlockNestedLoopJoinOperator(pages,operators.pop(), operators.pop(), arg0.condition());
//			
//			// hard code the number of buffer pages for BNLJ
//			joinOp = new BlockNestedLoopJoinOperator(joinBufferPages,operators.pop(), operators.pop(), arg0.condition());
//			break;
//		case SMJ:
//			PhysicalOperator sortOuter = null;
//			PhysicalOperator sortInner = null;
//			switch(config.getSortMethod()) {
//			case MEMORY:
//				sortOuter = new InMemorySortOperator(arg0.getLeftSorts(), operators.pop());
//				sortInner = new InMemorySortOperator(arg0.getRightSorts(), operators.pop());
//				break;
//			case EXTERNAL:
//				pages = config.getSortBufferPages();
//				new File(config.getTempDir()+"\\"+sortNum).mkdir();
//				sortOuter = new ExternalSortOperator(pages, config.isHumanReadable(), config.getTempDir()+"\\"+sortNum, arg0.getLeftSorts(), operators.pop()); sortNum++;
//				new File(config.getTempDir()+"\\"+sortNum).mkdir();
//				sortInner = new ExternalSortOperator(pages, config.isHumanReadable(), config.getTempDir()+"\\"+sortNum, arg0.getRightSorts(), operators.pop()); sortNum++;
//				break;
//			}
//			joinOp = new SortMergeJoinOperator(sortOuter, sortInner, arg0.condition(), arg0.getLeftSorts(), arg0.getRightSorts());
//			break;
//		case TNLJ:
//			joinOp = new TupleNestedJoinOperator(operators.pop(), operators.pop(), arg0.condition());
//			break;
//		}
//		operators.push(joinOp);
	}

	/**
	 * Method to visit a LogicalDistinct operator and build a physical operator
	 */
	@Override
	public void visit(LogicalDistinct arg0) {
		arg0.child().accept(this);
		SortOperator child = (SortOperator) operators.pop(); // If not a SortOperator, OK to throw a runtime exception
															 // given that this would indicate programmer error when building
															 // the logical plan.
		operators.push(new DuplicateEliminateOperator(child));
	}

}
