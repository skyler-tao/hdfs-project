package skyler.tao.hadoop.readwrite;

import java.io.BufferedReader;
import java.io.FileReader;

import org.json.JSONObject;
import org.junit.Test;

public class StatsParserTest {

	@Test
	public void statsParserTest() throws Exception {
		    String file = "stats_log_sample.log";
		    BufferedReader reader = null;
		    try {
		        reader = new BufferedReader(new FileReader(file));
		        String log = null;
		        while ((log = reader.readLine()) != null) {
		            StatsParser p = new StatsParser(log);
		            JSONObject json = p.getJSONObject();
//		            System.out.print(json.getString("uid"));
//		            int available_pos_num = json.getInt("available_pos");
//		            System.out.println(", " + available_pos_num);
//		            System.out.print(", L2: " + p.getL2Count());
//		            System.out.println(", L3: " + p.getL3Count());
//		            System.out.println(json.getString("reqid"));
//		            System.out.println(json.getString("reqtime"));
//		            System.out.println(json.optString("tmeta_l2"));
		            System.out.println(json.optString("pos"));
		        }
		    } catch (Exception ex) {
		        System.out.println(ex);
		    } finally {
		    	if (reader != null)
		    		reader.close();
		    }
	}
}
