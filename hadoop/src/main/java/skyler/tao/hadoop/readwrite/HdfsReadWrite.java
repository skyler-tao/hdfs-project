package skyler.tao.hadoop.readwrite;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import skyler.tao.hadoop.HdfsReader;

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
        		target.setReqtime(json.optString("reqtime"));
        		target.setReqid(json.optString("reqid"));
        		target.setFrom(json.optString("from"));
        		target.setPlatform(json.optString("platform"));
        		target.setVersion(json.optString("version"));
        		target.setIp(json.optString("ip"));
        		target.setProxy_source(json.optString("proxy_source"));
        		target.setWm(json.optString("wm"));
        		target.setAvailable_pos(json.optString("available_pos"));
        		target.setCategory_r(json.optString("category_r"));
        		target.setIdfa(json.optString("idfa"));
        		target.setImei(json.optString("imei"));
        		target.setLocation(json.optString("location"));
        		target.setIs_unread_pool(json.optInt("is_unread_pool"));
        		target.setLoadmore(json.optInt("loadmore"));
        		target.setFeedsnum(json.optInt("feedsnum"));
        		target.setUnread_status(json.optInt("unread_status"));
        		target.setLast_span(json.optInt("last_span"));
        		target.setProduct_r(json.optString("product_r"));
        		target.setRefresh_type(json.optInt("refresh_type"));
        		target.setPostostock(json.optString("pos"));
        		target.setTmeta("null");
        		target.setStat_date("null");
        		target.setService_name(json.optString("service_name"));
        		
        		messageProcessed = target.toString();
        		bw.write(messageProcessed);
        	}
        } catch (Exception e) {
        	
        } finally {
        	br.close();
        	bw.close();
        }
        return 0;
    }
    
    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new HdfsReader(), args);
        System.exit(returnCode);
    }
}