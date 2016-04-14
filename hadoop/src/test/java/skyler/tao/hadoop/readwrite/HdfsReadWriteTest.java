package skyler.tao.hadoop.readwrite;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.json.JSONObject;
import org.junit.Test;

public class HdfsReadWriteTest {

	public static final String L1S = "\\x1A";
	public static final String L1CS = "\\x1C";

	@Test
	public void parseTmeta() throws Exception {
		
		String file = "stats_log_sample.log";
	    BufferedReader reader = null;
	    System.out.println("Start...");
	    try {
	        reader = new BufferedReader(new FileReader(file));
	        String message = null;
	        while ((message = reader.readLine()) != null) {
	        	StatsParser parser = new StatsParser(message);
        		StatsHiveTarget target = new StatsHiveTarget();
        		JSONObject json = parser.getJSONObject();
        		target.setReqtime(json.optString("reqtime","null"));
        		target.setReqid(json.optString("reqid","null"));
        		target.setUid(json.optString("uid", "null"));
        		target.setFrom(json.optString("from","null"));
        		target.setPlatform(json.optString("platform","null"));
        		target.setVersion(json.optString("version","null"));
        		target.setIp(json.optString("ip","null"));
        		target.setProxy_source(json.optString("proxy_source","null"));
        		target.setWm(json.optString("wm","null"));
        		target.setAvailable_pos(json.optString("available_pos","null"));
        		target.setCategory_r(json.optString("category_r","null"));
        		target.setIdfa(json.optString("idfa","null"));
        		target.setImei(json.optString("imei","null"));
        		target.setLocation(json.optString("location","null"));
        		target.setIs_unread_pool(json.optString("is_unread_pool","null"));
        		target.setLoadmore(json.optString("loadmore","null"));
        		target.setFeedsnum(json.optString("feedsnum","null"));
        		target.setUnread_status(json.optString("unread_status","null"));
        		target.setLast_span(json.optString("last_span","null"));
        		target.setProduct_r(json.optString("product_r","null"));
        		target.setRefresh_type(json.optString("refresh_type","null"));
        		target.setPostostock(json.optString("pos","null"));
        		target.setTmeta(json.optString("tmeta_l2","null"));
        		Timestamp timestamp = new Timestamp(json.optLong("reqtime"));
        		Date date = new Date(timestamp.getTime());
        		target.setStat_date(date.toString());
        		target.setService_name(json.optString("service_name","null"));
	    		System.out.println(target.toString());
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
		String reqtime = "1460629505";
		System.out.println(Long.parseLong(reqtime));
//		Timestamp timestamp = new Timestamp(Long.parseLong(reqtime));
//		System.out.println(timestamp.toString());
		Calendar start = Calendar.getInstance();
		start.setTimeInMillis(Long.parseLong(reqtime));
		System.out.println(start.getWeekYear());
		Date date = start.getTime();
//		System.out.println(date.toString());
		SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-DD");
//		System.out.println(format.format(date).toString());
	}
}
