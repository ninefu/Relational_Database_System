/**
 * 
 */
package util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;

/**
 * A class to log debugging statements to a log file. 
 * New log content will be appended to the end of previous content.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class Logger {
	final String logDir; //dir + logger name
	
	public Logger(String dir){
		logDir = dir;
	}
	
	/**
	 * Method to write a statement to the log file.
	 * @param content
	 * 				String representing the content to be logged.
	 */
	public void log(String content){
		try(PrintWriter output = new PrintWriter(new BufferedWriter(new FileWriter(logDir,true)))){
			Date time = new Date(System.currentTimeMillis());
			output.println(time.toString() + ": " + content);
//			output.println(content); // for unit test
		} catch (IOException e) {
			System.err.println("Error occured during logging");
			e.printStackTrace();
		}	
	}
	
	/**
	 * Method to get the name of the logger file
	 * @return String representing the file directory and file name.
	 */
	public String toString(){
		return logDir;
	}
}
