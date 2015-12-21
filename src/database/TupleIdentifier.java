package database;

/**
 * Pretty much just a structure to hold the page number, tuple number
 * pair that uniquiely identifies a tuple.
 *
 ** @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class TupleIdentifier {
	private final int pageNo;
	private final int tupleNo;
	
	public TupleIdentifier(int p, int t) {
		pageNo = p;
		tupleNo = t;
	}
	
	/**
	 * Get the page number the tuple is on
	 * @return int
	 */
	public int getPageNumber() {
		return pageNo;
	}
	
	/**
	 * Get the tuple number for this tuple on a page
	 * @return int
	 */
	public int getTupleNumber() {
		return tupleNo;
	}

}
