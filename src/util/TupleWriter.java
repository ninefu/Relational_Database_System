package util;

import database.Tuple;

/**
 * 
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 *
 */
public interface TupleWriter {
	
	public void writeTuple(Tuple tup);
	
	public void reset();
	
	public void close();

}
