package skyler.tao.hadoop.readwrite;

public class StatsHiveTarget {

	private String reqtime;
	private String reqid;
	private String uid;
	private String from;
	private String platform;
	private String version;
	private String ip;
	private String proxy_source;
	private String wm;
	private String available_pos;
	private String category_r;
	private String idfa;
	private String imei;
	private String location;
	private String is_unread_pool;
	private String loadmore;
	private String feedsnum;
	private String unread_status;
	private String last_span;
	private String product_r;
	private String refresh_type;
	private String postostock;
	private String tmeta;
	private String stat_date;
	private String service_name;
	
	private final String DELIMITED = "*";
	
	public String getReqtime() {
		return reqtime;
	}
	public void setReqtime(String reqtime) {
		this.reqtime = reqtime;
	}
	public String getReqid() {
		return reqid;
	}
	public void setReqid(String reqid) {
		this.reqid = reqid;
	}
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	public String getFrom() {
		return from;
	}
	public void setFrom(String from) {
		this.from = from;
	}
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getProxy_source() {
		return proxy_source;
	}
	public void setProxy_source(String proxy_source) {
		this.proxy_source = proxy_source;
	}
	public String getWm() {
		return wm;
	}
	public void setWm(String wm) {
		this.wm = wm;
	}
	public String getAvailable_pos() {
		return available_pos;
	}
	public void setAvailable_pos(String available_pos) {
		this.available_pos = available_pos;
	}
	public String getCategory_r() {
		return category_r;
	}
	public void setCategory_r(String category_r) {
		this.category_r = category_r;
	}
	public String getIdfa() {
		return idfa;
	}
	public void setIdfa(String idfa) {
		this.idfa = idfa;
	}
	public String getImei() {
		return imei;
	}
	public void setImei(String imei) {
		this.imei = imei;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getIs_unread_pool() {
		return is_unread_pool;
	}
	public void setIs_unread_pool(String is_unread_pool) {
		this.is_unread_pool = is_unread_pool;
	}
	public String getLoadmore() {
		return loadmore;
	}
	public void setLoadmore(String loadmore) {
		this.loadmore = loadmore;
	}
	public String getFeedsnum() {
		return feedsnum;
	}
	public void setFeedsnum(String feedsnum) {
		this.feedsnum = feedsnum;
	}
	public String getUnread_status() {
		return unread_status;
	}
	public void setUnread_status(String unread_status) {
		this.unread_status = unread_status;
	}
	public String getLast_span() {
		return last_span;
	}
	public void setLast_span(String last_span) {
		this.last_span = last_span;
	}
	public String getProduct_r() {
		return product_r;
	}
	public void setProduct_r(String product_r) {
		this.product_r = product_r;
	}
	public String getRefresh_type() {
		return refresh_type;
	}
	public void setRefresh_type(String refresh_type) {
		this.refresh_type = refresh_type;
	}
	public String getPostostock() {
		return postostock;
	}
	public void setPostostock(String postostock) {
		this.postostock = postostock;
	}
	public String getTmeta() {
		return tmeta;
	}
	public void setTmeta(String tmeta) {
		this.tmeta = tmeta;
	}
	public String getStat_date() {
		return stat_date;
	}
	public void setStat_date(String stat_date) {
		this.stat_date = stat_date;
	}
	public String getService_name() {
		return service_name;
	}
	public void setService_name(String service_name) {
		this.service_name = service_name;
	}
	
	@Override
	public String toString() {
		return reqtime + DELIMITED + 
			   reqid + DELIMITED +
			   uid + DELIMITED +
			   from + DELIMITED +
			   platform + DELIMITED +
			   version + DELIMITED +
			   ip + DELIMITED +
			   proxy_source + DELIMITED +
			   wm + DELIMITED +
			   available_pos + DELIMITED +
			   category_r + DELIMITED +
			   idfa + DELIMITED +
			   imei + DELIMITED +
			   location + DELIMITED +
			   is_unread_pool + DELIMITED +
			   loadmore + DELIMITED +
			   feedsnum + DELIMITED +
			   unread_status + DELIMITED +
			   last_span + DELIMITED +
			   product_r + DELIMITED +
			   refresh_type + DELIMITED +
			   postostock + DELIMITED +
			   tmeta + DELIMITED + 
			   stat_date + DELIMITED +
			   service_name + "\n";
	}
}
