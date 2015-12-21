package UnionFind;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.schema.Column;

/**
 * UnionFind stores a collection of union-find elements. It can find whether an attribute
 * is contained in one of the element, and can merge two elements into one element. 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class UnionFind {
	private ArrayList<UnionFindElement> unionFinds;
	
	public UnionFind(){
		unionFinds = new ArrayList<UnionFindElement>();
	}
	
	public Set<UnionFindElement> findForTable(String tableName) {
		Set<UnionFindElement> ret = new HashSet<UnionFindElement>();
		for(UnionFindElement ufe : unionFinds) {
			for(Column col : ufe.getAttributes()) {
				if(col.getWholeColumnName().startsWith(tableName)) {
					ret.add(ufe);
				}
			}
		}
		return ret;
	}
	
	/**
	 * find and return the union-find element containing the attribute
	 * if no such element is found, create it and return it;
	 * @param attr, a particular attribute
	 * @return a union-find element contains the attribute
	 */
	public UnionFindElement findUF(Column attr){
		for (int i = 0; i < unionFinds.size(); i++){
			if (unionFinds.get(i).contains(attr)){
				return unionFinds.get(i);
			}
		}
		// if not found, create one
		UnionFindElement cur = new UnionFindElement();
		cur.addAttr(attr);
		unionFinds.add(cur);
		return cur;
	}
	
	/**
	 * Merge two unionFind elements and return a new one. The lower bound is the
	 * larger lower bound and the upper bound is the smaller upper bound.
	 * The original two unionFind elements will be removed from the elements array.
	 * 
	 * @param left, a unionFind element
	 * @param right, a unionFind element
	 * @return a newly created unionFind element
	 */
	public UnionFindElement union(UnionFindElement left, UnionFindElement right){
		UnionFindElement res = new UnionFindElement();
		
		//update attributes array
		// avoid duplicate attributes
		res.addAttrs(left.getAttributes());
		res.addAttrs(right.getAttributes());
		
		//update lower bound
		int newLower = (left.getLower() > right.getLower()) ? left.getLower() : right.getLower();
		res.setLower(newLower);
		
		//update upper bound
		int newUpper = (left.getUpper() < right.getUpper()) ? left.getUpper() : right.getUpper();
		res.setUpper(newUpper);
		
		//update equality
		//left element has an equality constraint
		if (left.getEquality() != (Integer) null && right.getEquality() == (Integer) null){
			res.setEquality(left.getEquality());
		//right element has an equality constraint
		}else if (left.getEquality() == (Integer) null && right.getEquality() != (Integer) null){
			res.setEquality(right.getEquality());
		// both elements have equality constraints
		}else if (left.getEquality() != (Integer) null && right.getEquality() != (Integer) null){
			//if two unionFinds get unioned, they are supposed to have the same equality constraint
			assert left.getEquality() == right.getEquality();
			res.setEquality(left.getEquality());
		}
	
		unionFinds.remove(left);
		unionFinds.remove(right);
		unionFinds.add(res);
		return res;
	}
	
	/**
	 * Get the unionFind elements in this UnionFind data structure
	 * @return an arrayList of UnionFindElements
	 */
	public ArrayList<UnionFindElement> getUnionFindElements(){
		return unionFinds;
	}
	
	/**
	 * Print out each UnionFind element one by one in String
	 * @return String
	 */
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < unionFinds.size(); i++){
			if (i > 0){
				sb.append("\n");
			}
			sb.append(unionFinds.get(i).toString());
		}
		return sb.toString();
	}
}
