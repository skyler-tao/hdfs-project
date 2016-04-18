package skyler.tao.hadoop.readwrite;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.junit.Test;

public class HdfsReadWriteTest {

//	public static final String L1S = "\\x1A";
//	public static final String L1CS = "\\x1C";

	@Test
	public void parseLineTest() throws Exception {
		
		String file = "stats_log_sample.log";
	    BufferedReader reader = null;
	    System.out.println("Start...");
	    try {
	        reader = new BufferedReader(new FileReader(file));
	        String message = null;
	        HdfsReadWrite targetClass = new HdfsReadWrite();
	        while ((message = reader.readLine()) != null) {
	        	String parsedLine = targetClass.parseLine(message);
	    		System.out.println(parsedLine);
	        }
	    } catch (Exception e) {
	    	System.out.println("ERROR!");
	    	e.printStackTrace();
	    } finally {
	    	reader.close();
	    }
	}
	
//	@Test
	public void reqtime2dateTest() {
		String reqtime = null;
		long millis = Long.parseLong(reqtime) * 1000;
		Date date = new Date(millis);
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		System.out.println(format.format(date).toString());
	}
	
//	@Test
	public void reqtime2date2() {
		String reqtime = "1460629505";
		long millis = Long.parseLong(reqtime) * 1000;
		Instant instant = Instant.ofEpochMilli(millis);
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		System.out.println(fmt.format(instant.atZone(ZoneId.systemDefault())));
	}
}
