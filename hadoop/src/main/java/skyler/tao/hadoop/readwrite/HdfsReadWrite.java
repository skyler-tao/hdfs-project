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
        		target.setReqtime(json.getString("reqtime"));
        		target.setReqid(json.getString("reqid"));
        		target.setFrom(json.getString("from"));
        		target.setPlatform(json.getString("platform"));
        		target.setVersion(json.getString("version"));
        		target.setIp(json.getString("ip"));
        		target.setProxy_source(json.getString("proxy_source"));
        		target.setWm(json.getString("wm"));
        		target.setAvailable_pos(json.getString("available_pos"));
        		target.setCategory_r(json.getString("category_r"));
        		target.setIdfa(json.getString("idfa"));
        		target.setImei(json.getString("imei"));
        		target.setLocation(json.getString("location"));
        		target.setIs_unread_pool(json.getInt("is_unread_pool"));
        		target.setLoadmore(json.getInt("loadmore"));
        		target.setFeedsnum(json.getInt("feedsnum"));
        		target.setUnread_status(json.getInt("unread_status"));
        		target.setLast_span(json.getInt("last_span"));
        		target.setProduct_r(json.getString("product_r"));
        		target.setRefresh_type(json.getInt("refresh_type"));
        		target.setPostostock(json.getString("pos"));
        		target.setTmeta("null");
        		target.setStat_date("null");
        		target.setService_name(json.getString("service_name"));
        		
        		messageProcessed = target.toString();
                logger.info(messageProcessed);
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
