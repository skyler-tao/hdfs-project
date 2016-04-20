package skyler.tao.hadoop.readwrite;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.json.JSONObject;
import org.junit.Test;

public class HdfsReadWriteTest {

//	public static final String L1S = "\\x1A";
//	public static final String L1CS = "\\x1C";

	@Test
	public void parseLineTest() throws Exception {
		
		String input = "stats_log_sample.log";
		String output = "stats_log_sample_out.log";
	    BufferedReader reader = null;
	    BufferedWriter writer = null;
	    System.out.println("Start...");
	    try {
	        reader = new BufferedReader(new FileReader(input));
	        writer = new BufferedWriter(new FileWriter(output));
	        String message = null;
	        ReadWriteProcess targetClass = new ReadWriteProcess();
	        while ((message = reader.readLine()) != null) {
	        	String parsedLine = targetClass.parseLine(message);
	        	if (parsedLine == null)
					continue;
	        	writer.write(parsedLine);
	        }
	    } catch (Exception e) {
	    	System.out.println("ERROR!");
	    	e.printStackTrace();
	    } finally {
	    	reader.close();
	    	writer.close();
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
	
//	@Test
	public void optStringTest() {
		JSONObject json = new JSONObject();
		json.append("reqtime", null);
		String reqtime = json.optString("reqtime", "null");
		System.out.println("reqtime: " + reqtime);
		if (reqtime == null)
			System.out.println("false");
		if ("null".equals(reqtime))
			System.out.println("true");
		if (reqtime.contains("null"))
			System.out.println("get it!");
	}
}
