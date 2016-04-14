package skyler.tao.hadoop.readwrite;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.json.JSONObject;

public class HdfsReadWrite extends Configured implements Tool {
    
    public static final String FS_PARAM_NAME = "fs.defaultFS";
    private static final Logger logger = Logger.getLogger(HdfsReadWrite.class);
    
    public int run(String[] args) throws Exception {
        
        if (args.length < 2) {
            logger.info("HdfsReadWrite [hdfs input path] [hdfs output path]");
            return 1;
        }
        
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        
        Configuration conf = getConf();
        logger.info("configured filesystem = " + conf.get(FS_PARAM_NAME));
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath,false)));
        
        String message = null;
        String messageProcessed = null;
        try {
        	while ((message = br.readLine())!= null) {
        		
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
        		
        		messageProcessed = target.toString();
        		bw.write(messageProcessed);
        	}
            bw.flush();
        } catch (Exception e) {
        	
        } finally {
        	br.close();
        	bw.close();
        }
        return 0;
    }
    
    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new HdfsReadWrite(), args);
        System.exit(returnCode);
    }
}
