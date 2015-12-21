package testing;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;

import org.junit.Ignore;
import org.junit.Test;

import database.Tuple;
import util.BinaryTupleReader;

public class TestCaseTester {
	String inputdir;
	String outputdir;
	String temp;
	String[] schema = new String[]{"A","B","C","D","E","F","G","H"};
	File f_output;
	File f_expected;
	BinaryTupleReader output;
	BinaryTupleReader expected;
	Tuple tu_exp;
	Tuple tu_out;

	@Test
	public void testQuery1OneWay(){
		f_output = new File("./test_cases/output/query1");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query1");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> out = new ArrayList<Tuple>();
		while (tu_out != null){
			out.add(tu_out);
			tu_out = output.readNextTuple();
		}
		
		int count = 0;
		while (tu_exp != null){
			assertEquals(true,out.contains(tu_exp));
			tu_exp = expected.readNextTuple();
			count++;
		}
		assertEquals(out.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery1TheOtherWay(){
		f_output = new File("./test_cases/output/query1");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query1");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> exp = new ArrayList<Tuple>();
		while (tu_exp != null){
			exp.add(tu_exp);
			tu_exp = expected.readNextTuple();
		}
		
		int count = 0;
		while (tu_out != null){
			assertEquals(true,exp.contains(tu_out));
			tu_out = output.readNextTuple();
			count++;
		}
		assertEquals(exp.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery2OneWay(){
		f_output = new File("./test_cases/output/query2");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query2");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> out = new ArrayList<Tuple>();
		while (tu_out != null){
			out.add(tu_out);
			tu_out = output.readNextTuple();
		}
		
		int count = 0;
		while (tu_exp != null){
			assertEquals(true,out.contains(tu_exp));
			tu_exp = expected.readNextTuple();
			count++;
		}
		assertEquals(out.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuer2TheOtherWay(){
		f_output = new File("./test_cases/output/query2");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query2");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> exp = new ArrayList<Tuple>();
		while (tu_exp != null){
			exp.add(tu_exp);
			tu_exp = expected.readNextTuple();
		}
		
		int count = 0;
		while (tu_out != null){
			assertEquals(true,exp.contains(tu_out));
			tu_out = output.readNextTuple();
			count++;
		}
		assertEquals(exp.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery3OneWay(){
		f_output = new File("./test_cases/output/query3");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query3");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> out = new ArrayList<Tuple>();
		while (tu_out != null){
			out.add(tu_out);
			tu_out = output.readNextTuple();
		}
		
		int count = 0;
		while (tu_exp != null){
			assertEquals(true,out.contains(tu_exp));
			tu_exp = expected.readNextTuple();
			count++;
		}
		assertEquals(out.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery3TheOtherWay(){
		f_output = new File("./test_cases/output/query3");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query3");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> exp = new ArrayList<Tuple>();
		while (tu_exp != null){
			exp.add(tu_exp);
			tu_exp = expected.readNextTuple();
		}
		
		int count = 0;
		while (tu_out != null){
			assertEquals(true,exp.contains(tu_out));
			tu_out = output.readNextTuple();
			count++;
		}
		assertEquals(exp.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery4OneWay(){
		f_output = new File("./test_cases/output/query4");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query4");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> out = new ArrayList<Tuple>();
		while (tu_out != null){
			out.add(tu_out);
			tu_out = output.readNextTuple();
		}
		
		int count = 0;
		while (tu_exp != null){
			assertEquals(true,out.contains(tu_exp));
			tu_exp = expected.readNextTuple();
			count++;
		}
		assertEquals(out.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery4TheOtherWay(){
		f_output = new File("./test_cases/output/query4");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query4");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> exp = new ArrayList<Tuple>();
		while (tu_exp != null){
			exp.add(tu_exp);
			tu_exp = expected.readNextTuple();
		}
		
		int count = 0;
		while (tu_out != null){
			assertEquals(true,exp.contains(tu_out));
			tu_out = output.readNextTuple();
			count++;
		}
		assertEquals(exp.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery5OneWay(){
		f_output = new File("./test_cases/output/query5");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query5");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> out = new ArrayList<Tuple>();
		while (tu_out != null){
			out.add(tu_out);
			tu_out = output.readNextTuple();
		}
		
		int count = 0;
		while (tu_exp != null){
			assertEquals(true,out.contains(tu_exp));
			tu_exp = expected.readNextTuple();
			count++;
		}
		assertEquals(out.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery5TheOtherWay(){
		f_output = new File("./test_cases/output/query5");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query5");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> exp = new ArrayList<Tuple>();
		while (tu_exp != null){
			exp.add(tu_exp);
			tu_exp = expected.readNextTuple();
		}
		
		int count = 0;
		while (tu_out != null){
			assertEquals(true,exp.contains(tu_out));
			tu_out = output.readNextTuple();
			count++;
		}
		assertEquals(exp.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}

	
	@Test
	public void testQuery6OneWay(){
		f_output = new File("./test_cases/output/query6");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query6");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> out = new ArrayList<Tuple>();
		while (tu_out != null){
			out.add(tu_out);
			tu_out = output.readNextTuple();
		}
		
		int count = 0;
		while (tu_exp != null){
			assertEquals(true,out.contains(tu_exp));
			tu_exp = expected.readNextTuple();
			count++;
		}
		assertEquals(out.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery6TheOtherWay(){
		f_output = new File("./test_cases/output/query6");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query6");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> exp = new ArrayList<Tuple>();
		while (tu_exp != null){
			exp.add(tu_exp);
			tu_exp = expected.readNextTuple();
		}
		
		int count = 0;
		while (tu_out != null){
			assertEquals(true,exp.contains(tu_out));
			tu_out = output.readNextTuple();
			count++;
		}
		assertEquals(exp.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery7OneWay(){
		f_output = new File("./test_cases/output/query7");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query7");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> out = new ArrayList<Tuple>();
		while (tu_out != null){
			out.add(tu_out);
			tu_out = output.readNextTuple();
		}
		
		int count = 0;
		while (tu_exp != null){
			assertEquals(true,out.contains(tu_exp));
			tu_exp = expected.readNextTuple();
			count++;
		}
		assertEquals(out.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery7TheOtherWay(){
		f_output = new File("./test_cases/output/query7");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query7");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> exp = new ArrayList<Tuple>();
		while (tu_exp != null){
			exp.add(tu_exp);
			tu_exp = expected.readNextTuple();
		}
		
		int count = 0;
		while (tu_out != null){
			assertEquals(true,exp.contains(tu_out));
			tu_out = output.readNextTuple();
			count++;
		}
		assertEquals(exp.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery8OneWay(){
		f_output = new File("./test_cases/output/query8");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query8");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> out = new ArrayList<Tuple>();
		while (tu_out != null){
			out.add(tu_out);
			tu_out = output.readNextTuple();
		}
		
		int count = 0;
		while (tu_exp != null){
			assertEquals(true,out.contains(tu_exp));
			tu_exp = expected.readNextTuple();
			count++;
		}
		assertEquals(out.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery8TheOtherWay(){
		f_output = new File("./test_cases/output/query8");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query8");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> exp = new ArrayList<Tuple>();
		while (tu_exp != null){
			exp.add(tu_exp);
			tu_exp = expected.readNextTuple();
		}
		
		int count = 0;
		while (tu_out != null){
			assertEquals(true,exp.contains(tu_out));
			tu_out = output.readNextTuple();
			count++;
		}
		assertEquals(exp.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery9OneWay(){
		f_output = new File("./test_cases/output/query9");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query9");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> out = new ArrayList<Tuple>();
		while (tu_out != null){
			out.add(tu_out);
			tu_out = output.readNextTuple();
		}
		
		int count = 0;
		while (tu_exp != null){
			assertEquals(true,out.contains(tu_exp));
			tu_exp = expected.readNextTuple();
			count++;
		}
		assertEquals(out.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	@Test
	public void testQuery9TheOtherWay(){
		f_output = new File("./test_cases/output/query9");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query9");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		ArrayList<Tuple> exp = new ArrayList<Tuple>();
		while (tu_exp != null){
			exp.add(tu_exp);
			tu_exp = expected.readNextTuple();
		}
		
		int count = 0;
		while (tu_out != null){
			assertEquals(true,exp.contains(tu_out));
			tu_out = output.readNextTuple();
			count++;
		}
		assertEquals(exp.size(),count);
		System.out.println(count);
		output.close();
		expected.close();
	}
	
	
	
	@Test
	public void test10OrderDependent() {
		f_output = new File("./test_cases/output/query10");
		output = new BinaryTupleReader(f_output,schema);
		tu_out = output.readNextTuple();
		
		f_expected = new File("./test_cases/expected_results/query10");
		expected = new BinaryTupleReader(f_expected,schema);
		tu_exp = expected.readNextTuple();
		
		int count = 0;
		
		while (tu_exp != null || tu_out != null){
			System.out.println(count);
//			if (tu_exp.toString() != tu_out.toString()){
//				System.out.println("Count: " + count);
				System.out.println("Expected " + tu_exp.toString());
				System.out.println("Output " + tu_out.toString());
//			}
			assertEquals(tu_exp.toString(),tu_out.toString());
			tu_exp = expected.readNextTuple();
			tu_out = output.readNextTuple();
			count++;
		}
		assertEquals(null,tu_exp);
		assertEquals(null,tu_out);
	}
//	
//	@Ignore
//	@Test
//	public void testOrderIndependentOneWay(){
//		f_output = new File("./test_cases/4a/output/query5");
//		output = new BinaryTupleReader(f_output,schema);
//		tu_out = output.readNextTuple();
//		
//		f_expected = new File("./test_cases/4a/expected/query5");
//		expected = new BinaryTupleReader(f_expected,schema);
//		tu_exp = expected.readNextTuple();
//		
//		ArrayList<Tuple> out = new ArrayList<Tuple>();
//		while (tu_out != null){
//			out.add(tu_out);
//			tu_out = output.readNextTuple();
//		}
//		
//		int count = 0;
//		while (tu_exp != null){
//			assertEquals(true,out.contains(tu_exp));
//			tu_exp = expected.readNextTuple();
//			count++;
//		}
//		assertEquals(out.size(),count);
//		System.out.println(count);
//	}
//	
//	@Ignore
//	@Test
//	public void testOrderIndependentTheOtherWay(){
//		f_output = new File("./test_cases/4a/output/query5");
//		output = new BinaryTupleReader(f_output,schema);
//		tu_out = output.readNextTuple();
//		
//		f_expected = new File("./test_cases/4a/expected/query5");
//		expected = new BinaryTupleReader(f_expected,schema);
//		tu_exp = expected.readNextTuple();
//		
//		ArrayList<Tuple> exp = new ArrayList<Tuple>();
//		while (tu_exp != null){
//			exp.add(tu_exp);
//			tu_exp = expected.readNextTuple();
//		}
//		
//		int count = 0;
//		while (tu_out != null){
//			assertEquals(true,exp.contains(tu_out));
//			tu_out = output.readNextTuple();
//			count++;
//		}
//		assertEquals(exp.size(),count);
//		System.out.println(count);
//	}
}
