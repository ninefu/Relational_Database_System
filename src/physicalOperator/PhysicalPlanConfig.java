package physicalOperator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * PhysicalPlanConfig specifies the requirements for sorting and joing based on
 * a configuration file
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class PhysicalPlanConfig {
	
	public enum SortMethod { MEMORY, EXTERNAL };
	public enum JoinMethod { TNLJ, BNLJ, SMJ };
	
	private SortMethod sortMethod;
	private JoinMethod joinMethod;
	private static int joinBufferPages = 3;
	private static int sortBufferPages = 3;
	private boolean humanReadable;
	private String tempDir;
	private boolean index;
	
	public PhysicalPlanConfig() {
		sortMethod = SortMethod.EXTERNAL;
		joinMethod = JoinMethod.TNLJ;
		humanReadable = false;
		index = false;
//		parseConfig(config);
	}
	
	public PhysicalPlanConfig(JoinMethod jm, SortMethod sm, boolean hr, boolean in) {
		sortMethod = sm;
		joinMethod = jm;
		humanReadable = hr;
		index = in;
	}
	
	/**
	 * Whether the data input is in HumanReadable format or binary format
	 * @return True if it is human readable, false if it is in binary format
	 */
	public boolean isHumanReadable() {
		return humanReadable;
	}
	
	public boolean isIndexed() {
		return index;
	}
	
	/**
	 * Set the value of Humanreadable
	 * @param Boolean r
	 */
	public void setHumanReadable(boolean r) {
		humanReadable = r;
	}
	
	/**
	 * Set the number of buffer pages for sorting
	 * @param integer p
	 */
	public void setSortBufferPages(int p) {
		sortBufferPages = p;
	}
	
	/**
	 * Set the number of buffer pages for joining
	 * @param integer p
	 */
	public void setJoinBufferPages(int p) {
		joinBufferPages = p;
	}
	
	public void setTempDir(String s) {
		tempDir = s;
	}
	
	public String getTempDir() {
		return tempDir;
	}
	
	/**
	 * Get the sort method specified in the configuration file
	 * @return sort method
	 */
	public SortMethod getSortMethod() {
		return sortMethod;
	}
	
	/**
	 * Get the join method specified in the configuration file
	 * @return join method
	 */
	public JoinMethod getJoinMethod() {
		return joinMethod;
	}
	
	/**
	 * Get the number of buffer pages if the sort method is External sort
	 * @return Integer, number of buffer pages
	 */
	public int getSortBufferPages() {
		if(sortMethod == SortMethod.MEMORY) {
			throw new RuntimeException("Should not be getting buffer pages for in-memory sort.");
		}
		return sortBufferPages;
	}
	
	/**
	 * Get the number of buffer pages if the join method for BNLJ
	 * @return integer, number of buffer pages
	 */
	public int getJoinBufferPages() {
		if(joinMethod != JoinMethod.BNLJ) {
			throw new RuntimeException("Should not be getting buffer pages for something other than BNLJ join.");
		}
		return joinBufferPages;
	}
	
	/**
	 * Read a configuration file and update the specifications accordingly
	 * @param a configuration file f.
	 */
	private void parseConfig(File f) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(f));
			String joinLine = br.readLine();
			String sortLine = br.readLine();
			String indexLine = br.readLine();
			br.close();
			if(joinLine != null) {
				String[] tokens = joinLine.split(" ");
				int joinType = Integer.parseInt(tokens[0]);
				switch(joinType) {
					case 0: 
						joinMethod = JoinMethod.TNLJ;
						break;
					case 1:
						joinBufferPages = Integer.parseInt(tokens[1]);
						joinMethod = JoinMethod.BNLJ;
						break;
					case 2:
						joinMethod = JoinMethod.SMJ;
						break;
					default:
						System.err.println(String.format("Didn't recognize option %d, defaulting to TNLJ join method.", joinType));		
				}
			} else {
				System.err.println("Unable to find join specification, defaulting to TNLJ join.");
			}
			if(sortLine != null) {
				String[] tokens = sortLine.split(" ");
				int sortType = Integer.parseInt(tokens[0]);
				switch(sortType) {
					case 0: 
						sortMethod = SortMethod.MEMORY;
						break;
					case 1:
						sortBufferPages = Integer.parseInt(tokens[1]);
						if(sortBufferPages < 3) {
							System.err.println("External sort requires at least 3 buffer pages. Defaulting to 3.");
							sortBufferPages = 3;
						}
						sortMethod = SortMethod.EXTERNAL;
						break;
					default:
						System.err.println(String.format("Didn't recognize option %d, defaulting to in-memory sort method.", sortType));		
				}
			} else {
				System.err.println("Unable to find sort specification, defaulting to in-memory sort.");
			}
			if (indexLine != null){
				String[] tokens = indexLine.split(" ");
				int useIndex = Integer.parseInt(tokens[0]);
				switch(useIndex){
					case 0:
						index = false;
						break;
					case 1:
						index = true;
						break;
				}
			}else{
				System.err.println("Unable to find index specification, defaulting to full-scan implementation.");
			}
		
		} catch (IOException e) {
			System.err.println("Error parsing config file: using defaults.");
			e.printStackTrace();
		}
	}

}
