package edu.carleton.clusteringbenchmark.errorframework;

import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * ErrorLogger is the class that handles all errors and exceptions that
 * the user should be aware of.  To write an error to the log, use the 
 * writeExceptionToLog method.  To write an error to a message dialog box,
 * use the displayException method.  To alert the user to any errors that have 
 * been written to the error log recently, use the flushLog method.  All methods
 * are static.
 * 
 * @author ritza
 *
 */
public class ErrorLogger {
	public static boolean testing;  /* Used for disposing of modal dialogs when
									 * simply running tests.  Set to false in
									 * Mainframe for normal use, but can be
									 * set to true in any tests that 
									 * would provoke a modal dialog. - steinbel
									 */
	public static File dir; 
	public static File file; 
	public static boolean error = false;
	
	/**
	 * Writes the exception to the log timestamped with that particular day's 
	 * date.  The logs are in errorframework/ErrorLogs directory.
	 * @param type - type of error (SQLServer, Importing, etc.)
	 * @param message - message to write to log.
	 */
	public static void writeExceptionToLog(String type, String message){
		file = new File("errorframework"+File.separator+"ErrorLogs"+File.separator+"ErrorLog"+constructDate()+".txt");
		
		try {
			if (!file.exists())
				initializeLog();
			BufferedWriter writer = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(file, true)));
			writer.write(constructTime()+" : "+type+" Error: "+message+"\n");
			writer.close();
		} catch (IOException e) {
			System.err.println("Error writing to "+file.toString());
			e.printStackTrace();
		}
	}

	/**
	 * Writes the exception to the log timestamped with that particular day's 
	 * date.  The logs are in errorframework/ErrorLogs directory.
	 * @param type - type of error (SQLServer, Importing, etc.)
	 * @param message - message to write to log.
	 */
	public static void writeExceptionToLogAndPrompt(String type, String message){
		writeExceptionToLog(type,message);
		error=true;
	}

	/**
	 * There are only 10 log files at a time, representing the errors collected
	 * over the past 10 days' worth of working with Enchilada.  A new error
	 * log is created when the date changes, and the oldest log is erased.
	 * @throws IOException
	 */
	private static void initializeLog() throws IOException {
		System.out.println("initializing log and deleting the oldest file.");
		
		dir = new File("errorframework"+File.separator+"ErrorLogs");
		File[] files = dir.listFiles();
		if (files != null && files.length == 10) {
			File oldestFile = files[0];
			long oldestTime = files[0].lastModified();
			for (int i=1;i<files.length;i++) {
				if (files[i].lastModified() < oldestTime) {
					oldestFile = files[i];
					oldestTime = files[i].lastModified();
				}
			}
			oldestFile.delete();
		}
		
		file = new File("errorframework"+File.separator+"ErrorLogs"+File.separator+"ErrorLog"+constructDate()+".txt");
		System.out.println("Generating new error log: " + file.toString());
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		writer.write("ErrorLog: started @ "+constructDate()+"\n");
		writer.close();
	}
	
	/**
	 * Simple wrapper method to display an exception using the JOptionPane.
	 * Used for wrong user input, etc.
	 * @param parent - parent Container
	 * @param message - message to output.
	 */
	public static void displayException(Component parent, String message) {
		//unit tests don't show the dialogs because they require a response - steinbel
		if (!testing)
			JOptionPane.showMessageDialog(parent, message);
	}
	
	/**
	 * When flushLog is called, if any errors have been written since the last
	 * flushLog method call then a message is displayed alerting the user to 
	 * take a look at the error log.
	 * @param parent - parent Container
	 * @return true if errors are identified; false otherwise.
	 */
	public static boolean flushLog(Component parent) {
		if (error) {
			displayException(parent, "One or more errors has occurred.  Please" +
					" check the ErrorLog timestamped as " +constructDate()+".");
			error = false;
			return true;
		}
		return false;
		
	}
	
	/**
	 * Constructs the date (year, month and day) for timestamping the 
	 * error log files.
	 * @return the date
	 */
	private static String constructDate() {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("CST"));	
		String DATE_FORMAT = "yyyy-MM-dd";
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(TimeZone.getTimeZone("CST"));
		return sdf.format(cal.getTime());
	}
	
	/**
	 * Constructs the time (hour, minute, second) for timestamping each
	 * individual error that gets written to the log.
	 * @return the time
	 */
	private static String constructTime() {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("CST"));	
		String DATE_FORMAT = "HH:mm:ss:SSS";
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(TimeZone.getTimeZone("CST"));
		return sdf.format(cal.getTime());
	}
}