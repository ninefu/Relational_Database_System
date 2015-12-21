package physicalOperator;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import database.Tuple;
import util.BinaryTupleReader;
import util.BinaryTupleWriter;
import util.Constants;
import util.ReadableTupleReader;
import util.ReadableTupleWriter;
import util.TupleReader;
import util.TupleWriter;

/**
 * External Sort Operator sorts the tuple by using B number of buffer pages. In the initial pass,
 * it reads in all tuples and output N/B sorted files. In the following passes, it merge B-1 files
 * and outputs 1 sorted and merged files. It keeps doing this until only one file is generated in
 * a pass.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class ExternalSortOperator extends SortOperator {
	

	private int bufferPageNum; // number of pages in the buffer
	private String tempDir; // location for temp files
	private boolean humanReadable;
	private int lastPass;
	private int tuplesPerBuffer;
	private File sortedFile;
	private TupleReader sortedReader;
	
	private Tuple tuple;

	public ExternalSortOperator(int pageNum, boolean readable, String temp,
			String[] orderbyOrder, PhysicalOperator c) {
		super(orderbyOrder, c);
		bufferPageNum = pageNum;
		humanReadable = readable;
		tempDir = temp;
		
		// External merge sort algorithm from 13.3 in textbook (page 427)
		// Given B buffer pages in memory and need to sort N page file
		// Read in B pages, sort them, write to temp file
		// Do this until we have N/B sorted runs of B pages each
		System.out.println("External merge sorting");
		Tuple current = child.getNextTuple();
		tuple = current;
		int numAttr = tuple.getValues().length;
		String[] schema = current.getFields();
		int runCount = 0;
		tuplesPerBuffer = (bufferPageNum * Constants.pageSize)/(4 * numAttr) + 1;
		int tuplesInCurrentBuffer = 0;
		while(current != null) {
			Tuple buf[] = new Tuple[tuplesPerBuffer];
			buf[tuplesInCurrentBuffer] = current;
			tuplesInCurrentBuffer += 1;
			for(int i = 1; i < tuplesPerBuffer; i++) {
				// Read until the buffer is full
				current = child.getNextTuple();
				if(current == null) break;
				buf[tuplesInCurrentBuffer] = current;
				tuplesInCurrentBuffer++;
			}
			// Either we filled the buffer, or we reached EOF.
			// If we reached EOF without filling it, need to not have nulls in it.
			Tuple[] newBuf;
			int nonNulls = 0;
			for(int i = 0; i < buf.length; i++) {
				if(buf[i] != null) nonNulls++;
			}
			newBuf = new Tuple[nonNulls];
			int nbIndex = 0;
			for(int i = 0; i < buf.length; i++) {
				if(buf[i] != null) {
					newBuf[nbIndex] = buf[i];
					nbIndex++;
				}
			}
			Arrays.sort(newBuf, new tupleComparator(order));
			// Run is sorted, write it out.
			File parent = new File(tempDir);
			parent.mkdirs();
			File f = new File(tempDir, "pass0run"+runCount);
			if(humanReadable) {
				ReadableTupleWriter w = new ReadableTupleWriter(f);
				for(int i = 0; i < newBuf.length; i++) {
					w.writeTuple(newBuf[i]);
				}
				w.close();
			} else {
				BinaryTupleWriter w = new BinaryTupleWriter(f, numAttr);
				for(int i = 0; i < newBuf.length; i++) {
					w.writeTuple(newBuf[i]);
				}
				w.close();
			}
			runCount++;
			tuplesInCurrentBuffer = 0;
			current = child.getNextTuple();
		}
		
		// While there are multiple runs, merge B-1 adjacent runs
		System.out.println(runCount + " runs sorted, merging");
		int pass = 0;
		while(runCount > 1) {
			System.out.println("Beginning merge pass " + pass);
			int nextRunCount = 0;
			for(int i = 0; i < runCount; i += (bufferPageNum - 1)) {
				Tuple[][] runs;
				File[] files;
				TupleReader[] readers;
				if(runCount - i > bufferPageNum - 1) {
					runs = new Tuple[bufferPageNum - 1][tuplesPerBuffer];
					files = new File[bufferPageNum - 1];
					if(humanReadable) {
						readers = new ReadableTupleReader[bufferPageNum - 1];
					} else {
						readers = new BinaryTupleReader[bufferPageNum - 1];
					}
				} else {
					runs = new Tuple[runCount - i][tuplesPerBuffer];
					files = new File[runCount - i];
					if(humanReadable) {
						readers = new ReadableTupleReader[runCount - i];
					} else {
						readers = new BinaryTupleReader[runCount - i];
					}
				}
				for(int j = 0; j < files.length; j++) {
					int runNum = i + j;
					files[j] = new File(tempDir, "pass"+pass+"run"+runNum);
					if(humanReadable) {
						readers[j] = new ReadableTupleReader(files[j], schema);
					} else {
						readers[j] = new BinaryTupleReader(files[j], schema);
					}
				}
								
				int next = pass + 1;
				int runCountTemp = i/(bufferPageNum - 1);
				File outFile = new File(tempDir, "pass"+next+"run"+ runCountTemp);
				
				ArrayList<Tuple> outputPage = new ArrayList<Tuple>();
				for (int i1 = 0; i1 < readers.length; i1++){
					Tuple tu = readers[i1].readNextTuple();
					while (tu != null){
						outputPage.add(tu);
						tu = readers[i1].readNextTuple();
					}
				}
				Collections.sort(outputPage,new tupleComparator(order));
				
				TupleWriter writer;
				if(humanReadable) {
					writer = new ReadableTupleWriter(outFile);
				} else {
					writer = new BinaryTupleWriter(outFile, numAttr);
				}
				for (int m = 0; m < outputPage.size(); m++){
					writer.writeTuple(outputPage.get(m));
				}

	
				writer.close();
				nextRunCount++;
			}
			pass++;
			runCount = nextRunCount;
			tuplesPerBuffer *= (bufferPageNum - 1);
			System.out.println(runCount + " runs left");
		}
		
		lastPass = pass;
		
		sortedFile = new File(tempDir, "pass"+lastPass+"run0");
		if(humanReadable) {
			sortedReader = new ReadableTupleReader(sortedFile, tuple.getFields());
		} else {
			sortedReader = new BinaryTupleReader(sortedFile, tuple.getFields());
		}
	}
	
	
	/**
	 * Reset the operator
	 */
	@Override
	public void reset() {
		sortedReader.reset();
	}

	/**
	 * 
	 * @return
	 */
	@Override
	public Tuple getNextTuple() {
		return sortedReader.readNextTuple();
	}

	@Override
	public void reset(int index) {
		if(humanReadable) {
			((ReadableTupleReader)sortedReader).reset(index);
		} else {
			((BinaryTupleReader)sortedReader).reset(index);
		}
	}


	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}

}
