package UnionFind;

import java.util.ArrayList;

import net.sf.jsqlparser.schema.Column;

/**
 * Element of union-find keeps track of a list of attributes which share the same numeric 
 * lower bound, upper bound, and equality constraint.
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class UnionFindElement {
	private ArrayList<Column> attrs;
	private int lower;
	private int upper;
	private Integer equality;
	
	@SuppressWarnings("null")
	public UnionFindElement(){
		attrs = new ArrayList<Column>();
		lower = Integer.MIN_VALUE;
		upper = Integer.MAX_VALUE;
		equality = (Integer) null;
	}
	
	/**
	 * Check if this element contains a given attribute
	 * @param attr
	 * @return true if contains, otherwise false
	 */
	public boolean contains(Column attr){
		for (Column Attr : attrs){
			if ((attr.getWholeColumnName()).equals(Attr.getWholeColumnName())){
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Get the lower bound of the unionFind
	 * @return int, lower bound
	 */
	public int getLower(){
		return lower;
	}
	
	/**
	 * Get the upper bound of the unionFind
	 * @return int, upper bound
	 */
	public int getUpper(){
		return upper;
	}
	
	/**
	 * Get the equality constraints
	 * @return int
	 */
	public Integer getEquality(){
		return equality;
	}
	
	/**
	 * Add an attribute to the UnionFind
	 * @param String, attributes
	 */
	public void addAttr(Column attr){
		if (!attrs.contains(attr)){
			attrs.add(attr);
		}
	}
	
	/**
	 * Add a list of attributes to the UnionFind
	 * @param an ArrayList of attribute string
	 */
	public void addAttrs(ArrayList<Column> Attrs){
		for (int i = 0; i < Attrs.size(); i++){
			if (!attrs.contains(Attrs.get(i))){
				attrs.add(Attrs.get(i));
			}
		}
	}
	
	/**
	 * Set the lower bound
	 * @param bound, int
	 */
	public void setLower(int bound){
		lower = bound;
	}
	
	/**
	 * Set the upper bound
	 * @param bound,int
	 */
	public void setUpper(int bound){
		upper = bound;
	}
	
	/**
	 * Set the equality constraints
	 * @param bound,int
	 */
	public void setEquality(int bound){
		equality = bound;
		lower = bound;
		upper = bound;
	}
	
	/**
	 * Get the attributes in the UnionFind
	 * @return
	 */
	public ArrayList<Column> getAttributes(){
		return attrs;
	}
	
	/**
	 * Print the UnionFindElement as a string.
	 * Example: [[S.A, B.D], equals null, min null, max null]
	 * @return String
	 */
	public String toString(){
		if (attrs.size() > 0){
			StringBuilder sb = new StringBuilder();
			sb.append("[[");
			sb.append(attrs.get(0));
			for (int i = 1; i < attrs.size(); i++){
				sb.append(", ");
				sb.append(attrs.get(i).toString());
			}
			if (equality == (Integer) null){
				sb.append("], equals " + "null");
			}else{
				sb.append("], equals " + equality.toString());
			}
			
			if (lower == Integer.MIN_VALUE){
				sb.append(", min " + null);
			}else{
				sb.append(", min " + lower);
			}
			
			if (upper == Integer.MAX_VALUE){
				sb.append(", max " + null);
			}else{
				sb.append(", max " + upper);
			}
			sb.append("]");
			return sb.toString();
		}
		return null;
	}
}
