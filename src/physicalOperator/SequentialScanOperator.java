package physicalOperator;

import database.Table;
import database.Tuple;
import util.BinaryTupleReader;
import util.ReadableTupleReader;
import util.TupleReader;

/**
 * ScanOperator keeps track of stream representing the base table that
 * it is supposed to read from. Each call of getNextTuple() reads the next
 * line in the file and constructs a Tuple object from the data. Calling reset
 * will reset the stream to the beginning of the file.
 *
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class SequentialScanOperator extends ScanOperator {
	
	private TupleReader reader;
	
	public SequentialScanOperator(Table table, String alias) {
		this(table, alias, false);
	}
	
	public SequentialScanOperator(Table table, String alias, boolean humanReadable) {
		super(table, alias);
		if(humanReadable) {
			reader = new ReadableTupleReader(table.getDataFile(), schema);
		} else {
			reader = new BinaryTupleReader(table.getDataFile(), schema);
		}
	}

	
	/** Method to reset the scan operator to the beginning of the file.
	 */
	@Override
	public void reset() {
		reader.reset();
	}

	/** Method to read the next line of the file and get the next tuple repeatedly.
	 * 
	 * @return Tuple
	 * 				the next available tuple output
	 */
	@Override
	public Tuple getNextTuple() {
		return reader.readNextTuple();
	}

	/**
	 * Not implemented
	 */
	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}

}
