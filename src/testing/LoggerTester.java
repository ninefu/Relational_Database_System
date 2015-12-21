package testing;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.junit.Test;

import util.Logger;

public class LoggerTester {
	String logDir = "samples/logger.txt";
	
	Logger log = new Logger(logDir); 
	
	@Test
	public void testToString(){
		String s = log.toString();
		assertEquals("samples/logger.txt",s);
	}
	
	@Test
	public void testLog() throws IOException{
		String s1 = "1";
		String s2 = "2";
		String s3 = "3";
		log.log(s1);
		log.log(s2);
		log.log(s3);
		System.out.println("logged");
		
		String t1 = "";
		String t2 = "";
		String t3 = "";
		try (BufferedReader reader = new BufferedReader(new FileReader(new File("samples/","logger.txt")))){
				t1 = reader.readLine();
				t2 = reader.readLine();
				t3 = reader.readLine();
				assertEquals(s1,t1);
				assertEquals(s2,t2);
				assertEquals(s3,t3);
		} catch (FileNotFoundException e) {
			System.err.println("Error occured while reading the log file.");
		}
	}

}
