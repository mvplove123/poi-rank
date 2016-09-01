package com.map.model;

import java.sql.ResultSet;

import oracle.spatial.geometry.JGeometry;
import oracle.sql.STRUCT;

/**
 * sdb库中t_poi实体类
 * <p>记录t_poi表的字段
 * 
 * @ClassName: Poi
 * @author: huajin.shen	
 * @date: 2015年6月23日 上午11:50:21
 * @version: 1.0
 */
public class Poi {
	public final static String SQL_INSERT_SDB = "insert into t_poi(name,englishname,alias,keyword,dataid,phone,address,postalcode,url,brief,大类,小类,来源,layercode,from_x,to_x,g_from,g_to,adjustablerank,kind,标识,cpid,lastmodify,modifyname,所属省,所属城市,所属区县,hidden_folder,hidden_memo,geoloc,apply,source,uniqueid,credibility,nav_class,nav_pid,nav_p_pid,tag,mergealias,guid,surrounding_info,labelname,flag) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"; 
	public final static String SQL_INSERT_SDB_BAKSRC = "insert into t_poi_baksrc(name,englishname,alias,keyword,dataid,phone,address,postalcode,url,brief,大类,小类,来源,layercode,from_x,to_x,g_from,g_to,adjustablerank,kind,标识,cpid,lastmodify,modifyname,所属省,所属城市,所属区县,hidden_folder,hidden_memo,geoloc,apply,source,uniqueid,credibility,nav_class,nav_pid,nav_p_pid,tag,mergealias,guid,surrounding_info,src_id,info,labelname,flag) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	public final static String SQL_INSERT_PUB = "insert into t_lyr_entity(uniqueid,layerid,caption,alias,keyword,phone,address,postalcode,geometry,vmltextdesc,layercatalogid,styleid,labelstyleid,dataid,poiurl,poidesc,category,subcategory,province,city,county,spatial_score,text_score,poi_score,createdate,lastupdate,englishname,surrounding_info,laiyuan,mergealias) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	public final static String SQL_UPDATE_SDB = "update t_poi set name=?,englishname=?,alias=?,keyword=?,guid=?,phone=?,address=?,postalcode=?,url=?,brief=?,大类=?,小类=?,来源=?,layercode=?,from_x=?,to_x=?,g_from=?,g_to=?,adjustablerank=?,kind=?,标识=?,cpid=?,lastmodify=?,modifyname=?,所属省=?,所属城市=?,所属区县=?,hidden_folder=?,hidden_memo=?,geoloc=?,apply=?,source=?,uniqueid=?,credibility=?,nav_class=?,nav_pid=?,nav_p_pid=?,tag=?,mergealias=?,surrounding_info=?,labelname=?,flag=? where dataid=?";
	public final static String SQL_UPDATE_PUB = "update t_lyr_entity set uniqueid=?,layerid=?,caption=?,alias=?,keyword=?,phone=?,address=?,postalcode=?,geometry=?,vmltextdesc=?,layercatalogid=?,styleid=?,labelstyleid=?,poiurl=?,poidesc=?,category=?,subcategory=?,province=?,city=?,county=?,spatial_score=?,text_score=?,poi_score=?,createdate=?,lastupdate=?,englishname=?,surrounding_info=?,laiyuan=?,mergealias=? where dataid =?";
	
	public String name = null;

	public String englishname = null;

	public long uniqueid = 0;

	public String alias = null;
	
	public String mergealias = null;

	public String keyword = null;

	public String dataid = null;

	public String phone = null;

	public String address = null;

	public String postalcode = null;

	public String url = null;

	public String brief = null;

	public String bigclass = null;

	public String smallclass = null;

	public String source = null;

	public String laiyuan = null;

	public String layercode = null;
	
	public String labelname = null;

	public long from_x = 0;

	public long to_x = 0;

	public long g_from = 0;

	public long g_to = 0;

	public String directionflag = null;

	public long adjustablerank = 0;

	public String kind = null;

	public String sign = null;

	public long cpid = 0;

	public long lastmodify = 0;

	public String modifyname = null;

	public String province = null;

	public String city = null;

	public String county = null;

	public String hidden_folder = null;

	public String hidden_memo = null;

	public String flag = null;

	public String guid = null;
	
	public JGeometry jGeometry;

	public double x = 0;

	public double y = 0;
	
	public String wkt = null;

	public String apply = null;

	public String surrounding_info = null;

	public String entity = null;
	
	public String credibility=null;
	
	public String nav_class=null;
	
	public String nav_pid=null;
	
	public String nav_p_pid=null;
	
	public String tag=null;	
	
	/******************* extra field *******************/
	public String src_id = null;
	// 深度信息
	public String deep = null;
	// DoFlag
	public String doFlag = "1";
	
	public String short_road = null;
	
	public long layerid = 0;	// 该字段来自t_lyr_entity
	
	public boolean dirty = false;	// 该字段为地名数据入库时去重使用
	// 结构化信息
	public String structs = null;
	/*** 是否是poi点,默认为true */
	public boolean isPoint = true;
	// 扩展字段,赋别名时用,一般情况下都用不到该字段
	public boolean isMainNode = false;




	public String hotelcommentnum="0"; //酒店类评论数
	public String commentcount="0"; //景点类评论数
	public String recordCount="0"; //其他类评论数

	public String minprice="0";//酒店价格
	public String price="0"; //景点类价格
	public String sellingPrice="0";
	public String avgPrice="0";//其他类价格

	public String hotelrank="0"; //酒店星级打分
	public String praise="0"; //景点类星级打分
	public String scoremap="0"; //其他类星级打分









	public Poi() {
	}

	public Poi(ResultSet rs) {
		try {
			this.name = rs.getString("name");
			this.englishname = rs.getString("englishname");
			this.uniqueid = rs.getLong("uniqueid");
			this.alias = rs.getString("alias");
			this.mergealias=rs.getString("mergealias");
			this.keyword = rs.getString("keyword");
			this.dataid = rs.getString("dataid");
			this.phone = rs.getString("phone");
			this.address = rs.getString("address");
			this.postalcode = rs.getString("postalcode");
			this.url = rs.getString("url");
			this.brief = rs.getString("brief");
			this.bigclass = rs.getString("大类");
			this.smallclass = rs.getString("小类");
			this.laiyuan = rs.getString("来源");
			this.layercode = rs.getString("layercode");
			this.labelname = rs.getString("labelname");
			this.from_x = rs.getLong("from_x");
			this.to_x = rs.getLong("to_x");
			this.g_from = rs.getLong("g_from");
			this.g_to = rs.getLong("g_to");
			this.directionflag = rs.getString("directionflag");
			this.adjustablerank = rs.getLong("adjustablerank");
			this.kind = rs.getString("kind");
			this.sign = rs.getString("标识");
			this.cpid = rs.getLong("cpid");
			try {
				this.lastmodify = rs.getTimestamp("lastmodify").getTime();
			} catch (Exception e) {	// sdb库中出现为空的情况
				this.lastmodify = System.currentTimeMillis();
			}
			this.modifyname = rs.getString("modifyname");
			this.province = rs.getString("所属省");
			this.city = rs.getString("所属城市");
			this.county = rs.getString("所属区县");
			this.hidden_folder = rs.getString("hidden_folder");
			this.hidden_memo = rs.getString("hidden_memo");
			this.flag = rs.getString("flag");
			this.guid = rs.getString("guid");
			if (this.src_id == null) {
				int splitindex = this.guid.indexOf("_");
				if ( splitindex != -1 && !guid.endsWith("_"))
					this.src_id= guid.substring(splitindex+1);
				else 
					this.src_id= guid;
			}
			STRUCT st = (STRUCT) rs.getObject("GEOLOC");
			this.jGeometry = JGeometry.load(st);
			x = this.jGeometry.getPoint()[0];
			y = this.jGeometry.getPoint()[1];
			this.apply = rs.getString("apply");
			this.surrounding_info = rs.getString("surrounding_info");
			this.source = rs.getString("source");
			try {
				this.entity = rs.getString("entity");
			} catch (Exception e) {
			}
			this.credibility=rs.getString("credibility");
			this.nav_class=rs.getString("nav_class");
			this.nav_pid=rs.getString("nav_pid");
			this.nav_p_pid=rs.getString("nav_p_pid");
			this.tag=rs.getString("tag");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
//	public boolean equals(Object obj) {
//	if (this == obj) {
//	    return true;
//	}
//	if (obj instanceof Poi) {
//		Poi p = (Poi)obj;
//		// 比较的字段个数同构造函数Poi(rs)中字段数相同
//		if (Util.stringEquals(this.name, p.name)
//	    	&& Util.stringEquals(this.englishname, p.englishname)
//	    	&& this.uniqueid == p.uniqueid
//	    	&& Util.stringEquals(this.alias, p.alias)
//	    	&& Util.stringEquals(this.mergealias, p.mergealias)
//	    	&& Util.stringEquals(this.keyword, p.keyword)
//	    	&& Util.stringEquals(this.dataid, p.dataid)
//	    	&& Util.stringEquals(this.phone, p.phone)
//	    	&& Util.stringEquals(this.address, p.address)
//	    	&& Util.stringEquals(this.postalcode, p.postalcode)
//	    	&& Util.stringEquals(this.url, p.url)
//	    	&& Util.stringEquals(this.brief, p.brief)
//	    	&& Util.stringEquals(this.bigclass, p.bigclass)
//	    	&& Util.stringEquals(this.smallclass, p.smallclass)
//	    	&& Util.stringEquals(this.source, p.source)
//	    	&& Util.stringEquals(this.laiyuan, p.laiyuan)
//	    	&& Util.stringEquals(this.layercode, p.layercode)
//	    	&& Util.stringEquals(this.labelname, p.labelname)
//	    	&& this.from_x == p.from_x
//	    	&& this.to_x == p.to_x
//	    	&& this.g_from == p.g_from
//	    	&& this.g_to == p.g_to
//	    	&& Util.stringEquals(this.directionflag, p.directionflag)
//	    	&& this.adjustablerank == p.adjustablerank
//	    	&& Util.stringEquals(this.kind, p.kind)
//	    	&& Util.stringEquals(this.sign, p.sign)
//	    	&& this.cpid == p.cpid
//	    	// && this.lastmodify == p.lastmodify
//	    	&& Util.stringEquals(this.modifyname, p.modifyname)
//	    	&& Util.stringEquals(this.province, p.province)
//	    	&& Util.stringEquals(this.city, p.city)
//	    	&& Util.stringEquals(this.county, p.county)
//	    	&& Util.stringEquals(this.hidden_folder, p.hidden_folder)
//	    	&& Util.stringEquals(this.hidden_memo, p.hidden_memo)
//	    	&& Util.stringEquals(this.flag, p.flag)
//	    	&& Util.stringEquals(this.guid, p.guid)
//	    	&& this.x == p.x
//	    	&& this.y == p.y
//	    	&& Util.stringEquals(this.apply, p.apply)
//	    	&& Util.stringEquals(this.surrounding_info, p.surrounding_info)
//	    	&& Util.stringEquals(this.entity, p.entity)
//	    	&& Util.stringEquals(this.credibility, p.credibility)
//	    	&& Util.stringEquals(this.nav_class, p.nav_class)
//	    	&& Util.stringEquals(this.nav_pid, p.nav_pid)
//	    	&& Util.stringEquals(this.nav_p_pid, p.nav_p_pid)
//	    	&& Util.stringEquals(this.tag, p.tag)) {
//	    	return true;
//	    }
//	}
//	return false;
//	}
//
//	// test
//	public double similarity(Poi p) {
//		double sim = 0;double sum = 37;
//		if (Util.stringEquals(this.name, p.name)){
//			sim++;
//		}
//	    if (Util.stringEquals(this.englishname, p.englishname)){
//	    	sim++;
//	    }
//	    if (this.uniqueid == p.uniqueid){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.alias, p.alias)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.mergealias, p.mergealias)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.keyword, p.keyword)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.dataid, p.dataid)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.phone, p.phone)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.address, p.address)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.postalcode, p.postalcode)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.url, p.url)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.brief, p.brief)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.bigclass, p.bigclass)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.smallclass, p.smallclass)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.source, p.source)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.layercode, p.layercode)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.labelname, p.labelname)){
//	    	sim++;
//	    }
//	    if (this.from_x == p.from_x){
//	    	sim++;
//	    }
//	    if (this.to_x == p.to_x){
//	    	sim++;
//	    }
//	    if (this.g_from == p.g_from){
//	    	sim++;
//	    }
//	    if (this.g_to == p.g_to){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.directionflag, p.directionflag)){
//	    	sim++;
//	    }
//	    if (this.adjustablerank == p.adjustablerank){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.kind, p.kind)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.sign, p.sign)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.province, p.province)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.city, p.city)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.county, p.county)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.guid, p.guid)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.apply, p.apply)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.surrounding_info, p.surrounding_info)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.entity, p.entity)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.credibility, p.credibility)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.nav_class, p.nav_class)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.nav_pid, p.nav_pid)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.nav_p_pid, p.nav_p_pid)){
//	    	sim++;
//	    }
//	    if (Util.stringEquals(this.tag, p.tag)) {
//	    	sim++;
//	    }
//		return sim / sum;
//	}
//
//	/*** 转为Lyr格式 */
//	public Lyr toLyr() {
//		Lyr lyr = new Lyr();
//		lyr.caption = this.name;
//		lyr.layerid = this.layerid;
//		lyr.englishname = this.englishname;
//		lyr.uniqueid = this.uniqueid;
//		lyr.alias = this.alias;
//		lyr.mergealias = this.mergealias;
//		String lyrkeyword = null;
//		if (this.keyword==null||this.keyword.trim().equals("")) {
//			lyrkeyword = this.tag;
//		} else {
//			if (this.tag==null||this.tag.trim().equals("")) {
//				lyrkeyword = this.keyword;
//			} else {
//				lyrkeyword= this.keyword + "," + this.tag;
//			}
//		}
//		lyr.keyword = lyrkeyword;
//		lyr.dataid = "1_" + this.dataid;
//		lyr.phone = this.phone;
//		lyr.address = this.address;
//		lyr.postalcode = this.postalcode;
//		lyr.poiurl = this.url;
//		lyr.poidesc = this.brief;
//		lyr.category = this.bigclass;
//		lyr.subcategory = this.smallclass;
//		lyr.createdate = this.lastmodify;
//		lyr.province = this.province;
//		lyr.city = this.city;
//		lyr.county = this.county;
//		lyr.x = this.x;
//		lyr.y = this.y;
//		lyr.wkt = this.wkt;
//		lyr.surrounding_info = this.surrounding_info;
//		lyr.laiyuan = this.laiyuan;
//		return lyr;
//	}
	
	public Object get(String fieldName) throws Exception {
		fieldName = fieldName.toLowerCase();
		if (fieldName.equals("name")) {
			return this.name;
		} else if (fieldName.equals("englishname")) {
			return this.englishname;
		} else if (fieldName.equals("uniqueid")) {
			return this.uniqueid;
		} else if (fieldName.equals("alias")) {
			return this.alias;
		} else if (fieldName.equals("mergealias")) {
			return this.mergealias;
		} else if (fieldName.equals("keyword")) {
			return this.keyword;
		} else if (fieldName.equals("dataid")) {
			return this.dataid;
		} else if (fieldName.equals("phone")) {
			return this.phone;
		} else if (fieldName.equals("address")) {
			return this.address;
		} else if (fieldName.equals("postalcode")) {
			return this.postalcode;
		} else if (fieldName.equals("url")) {
			return this.url;
		} else if (fieldName.equals("brief")) {
			return this.brief;
		} else if (fieldName.equals("bigclass")) {
			return this.bigclass;
		} else if (fieldName.equals("smallclass")) {
			return this.smallclass;
		} else if (fieldName.equals("source")) {
			return this.source;
		} else if (fieldName.equals("laiyuan")) {
			return this.laiyuan;
		} else if (fieldName.equals("layercode")) {
			return this.layercode;
		} else if (fieldName.equals("labelname")) {
			return this.labelname;
		} else if (fieldName.equals("from_x")) {
			return this.from_x;
		} else if (fieldName.equals("to_x")) {
			return this.to_x;
		} else if (fieldName.equals("g_from")) {
			return this.g_from;
		} else if (fieldName.equals("g_to")) {
			return this.g_to;
		} else if (fieldName.equals("directionflag")) {
			return this.directionflag;
		} else if (fieldName.equals("adjustablerank")) {
			return this.adjustablerank;
		} else if (fieldName.equals("kind")) {
			return this.kind;
		} else if (fieldName.equals("sign")) {
			return this.sign;
		} else if (fieldName.equals("cpid")) {
			return this.cpid;
		} else if (fieldName.equals("lastmodify")) {
			return this.lastmodify;
		} else if (fieldName.equals("modifyname")) {
			return this.modifyname;
		} else if (fieldName.equals("province")) {
			return this.province;
		} else if (fieldName.equals("city")) {
			return this.city;
		} else if (fieldName.equals("county")) {
			return this.county;
		} else if (fieldName.equals("hidden_folder")) {
			return this.hidden_folder;
		} else if (fieldName.equals("hidden_memo")) {
			return this.hidden_memo;
		} else if (fieldName.equals("flag")) {
			return this.flag;
		} else if (fieldName.equals("guid")) {
			return this.guid;
		} else if (fieldName.equals("x")) {
			return this.x;
		} else if (fieldName.equals("y")) {
			return this.y;
		} else if (fieldName.equals("apply")) {
			return this.apply;
		} else if (fieldName.equals("surrounding_info")) {
			return this.surrounding_info;
		} else if (fieldName.equals("entity")) {
			return this.entity;
		} else if (fieldName.equals("credibility")) {
			return this.credibility;
		} else if (fieldName.equals("nav_class")) {
			return this.nav_class;
		} else if (fieldName.equals("nav_pid")) {
			return this.nav_pid;
		} else if (fieldName.equals("nav_p_pid")) {
			return this.nav_p_pid;
		} else if (fieldName.equals("tag")) {
			return this.tag;
		} else if (fieldName.equals("src_id")) {
			return this.src_id;
		} else if (fieldName.equals("deep")) {
			return this.deep;
		} else {
			throw new Exception("Field "+fieldName+" not exist!");
		}
	}
	
	//
	public String toString() {
		// name englishname uniqueid alias keyword dataid phone address
		// postalcode url brief bigclass smallclass source layercode from_x to_x
		// g_from g_to adjustablerank kind sign cpid lastmodify modifyname
		// province city county hidden_folder hidden_memo flag guid x y apply
		StringBuffer s = new StringBuffer();
		s.append(name).append("\t");
		s.append(englishname).append("\t");
		s.append(uniqueid).append("\t");
		s.append(alias).append("\t");
		s.append(mergealias).append("\t");		
		s.append(keyword).append("\t");
		s.append(dataid).append("\t");
		s.append(phone).append("\t");
		s.append(address).append("\t");
		s.append(postalcode).append("\t");
		s.append(url).append("\t");
		s.append(brief).append("\t");
		s.append(bigclass).append("\t");
		s.append(smallclass).append("\t");
		s.append(laiyuan).append("\t");
		s.append(layercode).append("\t");
		s.append(from_x).append("\t");
		s.append(to_x).append("\t");
		s.append(g_from).append("\t");
		s.append(g_to).append("\t");
		s.append(directionflag).append("\t");
		s.append(adjustablerank).append("\t");
		s.append(kind).append("\t");
		s.append(sign).append("\t");
		s.append(cpid).append("\t");
		s.append(lastmodify).append("\t");
		s.append(modifyname).append("\t");
		s.append(province).append("\t");
		s.append(city).append("\t");
		s.append(county).append("\t");
		s.append(hidden_folder).append("\t");
		s.append(hidden_memo).append("\t");
		s.append(flag).append("\t");
		s.append(guid).append("\t");
		s.append(x).append("\t");
		s.append(y).append("\t");
		s.append(apply).append("\t");
		s.append(credibility).append("\t");
		s.append(nav_class);
		s.append(nav_pid);
		s.append(nav_p_pid);
		s.append(tag);
		return s.toString();
	}
}
