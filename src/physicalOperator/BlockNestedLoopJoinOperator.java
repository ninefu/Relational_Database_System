package physicalOperator;

import database.Tuple;
import net.sf.jsqlparser.expression.Expression;
import util.Constants;

/**
 * BlockNestedLoopJoinOperator uses the block nested loop algorithm with pageNum 
 * number of buffer pages and joins the tuples from the left (outer) and right 
 * (inner) operators according to the Expression condition.
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class BlockNestedLoopJoinOperator extends JoinOperator {
	
	private int bufferPageNum;
	private Tuple innerTuple; // our current outer tuple that we are comparing to tuples in buffer.
	private Tuple[] buffer;
	private int positionInBuffer; // Indicates the index we are at in the outer buffer of pages (buffer)
	private int tuplesInBuffer;
	
	/**
	 * Constructs a join operator that uses the block nested loop algorithm with pageNum number of 
	 * buffer pages and joins the tuples from the left (outer) and right (inner) operators according
	 * to the Expression condition.
	 * @param pageNum
	 * @param left
	 * @param right
	 * @param cond
	 */
	public BlockNestedLoopJoinOperator(int pageNum, PhysicalOperator left, PhysicalOperator right, Expression cond) {
		super(left, right, cond);
		bufferPageNum = pageNum;
		positionInBuffer = 0;
		innerTuple = innerChild.getNextTuple();
		tuplesInBuffer = 0;
		Tuple firstTuple = outerChild.getNextTuple();
		if(firstTuple != null) {
			int tupleNum = (bufferPageNum * Constants.pageSize)/(4 * firstTuple.getValues().length);
			buffer = new Tuple[tupleNum];
			buffer[0] = firstTuple;
			tuplesInBuffer = 1;
			readPageStartingAt(1);
		} else {
			buffer = new Tuple[0];
		}
		
	}
	
	/** 
	 * Gets the number of buffer pages this join operator is using
	 * @return The integer number of buffer pages being used
	 */
	public int getNumBufferPages() {
		return bufferPageNum;
	}
	
	/**
	 * Reads tuples from outerChild into the buffer until the buffer is full,
	 * starting at a given index in the buffer.
	 * @param i Index in the buffer to start reading tuples
	 * @return True if > 0 tuples were read into the buffer, false otherwise
	 */
	private boolean readPageStartingAt(int i) {
		Tuple next = null;
		int read = 0;
		while(i < buffer.length && (next = outerChild.getNextTuple()) != null) {
			buffer[i] = next;
			i++;
			read++;
			tuplesInBuffer++;
		}
		return read > 0; // If we never read a tuple, there are no more blocks
	}
	
	/**
	 * Reads tuples from outerChildinto into the buffer from [0..buffer.length)
	 * @return True if > 0 tuples were read into the buffer, false otherwise
	 */
	private boolean readPage() {
		tuplesInBuffer = 0;
		return readPageStartingAt(0);
	}

	/**
	 * Reset the BNLJ operator to the first tuple
	 */
	@Override
	public void reset() {
		super.reset();
		innerTuple = innerChild.getNextTuple();
		positionInBuffer = 0;
		readPage();
	}

	/**
	 * Get the next tuple from the operator
	 * @return Tuple representing the next tuple from the operator
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple combined = null;
		boolean found = false;
		while(!found) {
			if(innerTuple != null) {
				if(positionInBuffer < tuplesInBuffer) {
					combined = combineTuples(buffer[positionInBuffer], innerTuple);
					positionInBuffer++;
					found = checkCondition(combined);
				} else {
					// We have reached the end of this block, so we need to go back
					// to the beginning and get a new innerTuple
					positionInBuffer = 0;
					innerTuple = innerChild.getNextTuple();
				}
			} else {
				// We have reached the end of tuples in S, so we need to reset the
				// inner child and read a new block
				innerChild.reset();
				innerTuple = innerChild.getNextTuple();
				if(!readPage()) {
					// If readPage returns false, there are no more pages to read, so we are done.
					return null;
				}
			}
		}
		return combined;
	}

	/**
	 * Not implemented
	 */
	@Override
	public void reset(int index) {
		throw new UnsupportedOperationException();
		
	}

	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}

}
