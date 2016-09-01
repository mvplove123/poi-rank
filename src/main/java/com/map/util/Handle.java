/**
 * Copyright (C), 2010-2015, Beijing Sogo Co., Ltd.
 *
 * @Title: Handle.java
 * @Package: com.sogou.map.data
 * @author: huajin.shen
 * @date: 2015年7月10日 下午4:06:55
 * @version: v1.0
 */
package com.map.util;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.map.model.DateTime;
import com.map.model.Poi;
import com.map.model.QuadTree;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.dom4j.*;
import org.dom4j.dom.DOMCDATA;
import org.dom4j.tree.DefaultCDATA;


/**
 * 记录Poi实体转为xml
 *
 * @ClassName: Handle
 * @author: huajin.shen
 * @date: 2015年7月10日 下午4:06:55
 * @version: 1.0
 */
public class Handle {
    //	static {
//		try {
//			String log4j = Config.PATH + "config/log4j.properties";
//			PropertyConfigurator.configure(log4j);
//		} catch (Exception e) { }
//	}
    private static Log log = LogFactory.getLog(Handle.class);
//	protected static Hashtable<String, String> typeTable = getTypeTable(); // 大小类表

    /**
     * poi转为xml格式
     * @param item poi对象
     * @param isbase 是否是基数据({@link PoiToXml}设为true)
     * @throws Exception
     * @return: String
     */
//	public static String poiToXml(Poi item, boolean isbase) throws Exception {
//		try {
//			Document doc = DocumentHelper.createDocument();
//			doc.setXMLEncoding("GBK");
//			Element poi = doc.addElement("POI");
//			String src_id = item.src_id;
//			String guid = item.guid;
//			if (src_id == null) {
//				int splitindex = guid.indexOf("_");
//				if ( splitindex != -1 && !guid.endsWith("_"))
//					src_id= guid.substring(splitindex+1);
//				else
//					src_id= guid;
//			}
//			if ( isbase ) {
//				poi.addElement("SRC_ID").addText("");
//				poi.addElement("GUID").addText(guid);
//			} else {
//				poi.addElement("SRC_ID").addText(src_id);
//				poi.addElement("GUID").addText("");
//			}
//			poi.addElement("SRC_OLD_ID");
//			poi.addElement("SRC_LINKID");
//			poi.addElement("SRC_LINK_TYPE");
//			if (item.name != null) {
//				poi.addElement("SRC_NAME").addText(Common.formatXML(item.name));
//			}else {
//				poi.addElement("SRC_NAME");
//			}
//			if (item.englishname !=null) {
//				poi.addElement("SRC_NAME_ENG").addText(Common.formatXML(item.englishname));
//			} else {
//				poi.addElement("SRC_NAME_ENG");
//			}
//			poi.addElement("SRC_NAME_PY");
//			if (item.address != null) {
//				poi.addElement("SRC_ADDRESS_CHN").addText(Common.formatXML(item.address));
//			} else {
//				poi.addElement("SRC_ADDRESS_CHN");
//			}
//			poi.addElement("SRC_ADDRESS_ENG");
//			poi.addElement("SRC_ADDRESS_PY");
//			poi.addElement("SRC_COORD_SYSTEM").addText("SOGOU");
//			poi.addElement("SRC_X").addText(Common.formatXML(String.valueOf(item.x)));
//			poi.addElement("SRC_Y").addText(Common.formatXML(String.valueOf(item.y)));
//			if ( item.wkt != null ) {	// 支持线、面数据的导出
//				poi.addElement("GEOMETRY").addText(item.wkt);
//			}
//			if (item.phone != null) {
//				poi.addElement("PHONE").addText(Common.formatXML(item.phone));
//			} else {
//				poi.addElement("PHONE");
//			}
//			if (item.postalcode != null) {
//				poi.addElement("POSTCODE").addText(Common.formatXML(item.postalcode));
//			} else {
//				poi.addElement("POSTCODE");
//			}
//			poi.addElement("SRC_TYPECODE");
//			String type = item.bigclass+";;" + item.smallclass;
//			poi.addElement("SRC_TYPE").addText(Common.formatXML(type));
//			if (item.province != null) {
//				poi.addElement("SRC_PROVINCE").addText(Common.formatXML(item.province));
//			} else {
//				poi.addElement("SRC_PROVINCE").addText("");
//			}
//			if (item.city != null) {
//				poi.addElement("SRC_CITY").addText(Common.formatXML(item.city));
//			} else {
//				poi.addElement("SRC_CITY").addText("");
//			}
//			if (item.county != null) {
//				poi.addElement("SRC_COUNTY").addText(Common.formatXML(item.county));
//			} else {
//				poi.addElement("SRC_COUNTY").addText("");
//			}
//			if (item.url != null) {
//				poi.addElement("SRC_URL").addText(Common.formatXML(item.url));
//			} else {
//				poi.addElement("SRC_URL");
//			}
//			if (item.alias != null) {
//				poi.addElement("SRC_ALIAS").addText(Common.formatXML(item.alias));
//			} else {
//				poi.addElement("SRC_ALIAS");
//			}
//			poi.addElement("BIZ_ZONE");
//			if (item.keyword != null) {
//				poi.addElement("KEYWORDS").addText(Common.formatXML(item.keyword));
//			} else {
//				poi.addElement("KEYWORDS");
//			}
//			poi.addElement("EMAIL");
//			poi.addElement("FAX");
//			poi.addElement("PICTURE");
//			poi.addElement("OPEN_TIME");
////			if ( item.mergealias != null) {
////				poi.addElement("NAMEALIAS").addText(Common.formatXML(item.mergealias));
////			} else {
//			poi.addElement("NAMEALIAS");
////			}
//			if (item.tag != null) {
//				poi.addElement("TAG").addText(Common.formatXML(item.tag));
//			} else {
//				poi.addElement("TAG");
//			}
//			poi.addElement("BRANDKN");
//			if (item.flag != null) {
//				poi.addElement("STATUS_FLAG").addText(Common.formatXML(item.flag));
//			} else {
//				poi.addElement("STATUS_FLAG");
//			}
//			poi.addElement("MODIFY_TIME").addText(DateTime.format(item.lastmodify));
//			if (item.laiyuan != null) {
//				poi.addElement("LAI_YUAN").addText(Common.formatXML(item.laiyuan));
//			} else {
//				poi.addElement("LAI_YUAN");
//			}
//			if (item.brief != null) {
//				poi.addElement("INTRODUCTION").addText(Common.formatXML(item.brief));
//			} else {
//				poi.addElement("INTRODUCTION");
//			}
//			if (item.dataid != null) {
//				poi.addElement("DATAID").addText(item.dataid);
//			} else {
//				poi.addElement("DATAID");
//			}
//			if (item.uniqueid != 0) {
//				poi.addElement("UNIQUEID").addText(String.valueOf(item.uniqueid));
//			} else {
//				poi.addElement("UNIQUEID");
//			}
//			Element nav = poi.addElement("NAV");
//			nav.addElement("NAV_CLASS").addText("");
//			nav.addElement("NAV_PID").addText("");
//			nav.addElement("NAV_P_PID").addText("");
//			if ( item.layercode != null) {
//				poi.addElement("LAYERCODE").addText(Common.formatXML(item.layercode));
//			} else {
//				poi.addElement("LAYERCODE");
//			}
//			if ( item.labelname != null ) {
//				poi.addElement("LABELNAME").addText(Common.formatXML(item.labelname));
//			} else {
//				poi.addElement("LABELNAME");
//			}
//			if (item.deep !=null && !item.deep.trim().equals("")) {
//				String additional = item.deep.replace("<?xml version=\"1.0\" encoding=\"GBK\"?>\n", "");
//				additional = additional.replace("\n", " ").replace("\r", " ").replace("\t", " ");
//				CDATA cdata = new DOMCDATA("<additional>" + additional + "</additional>");
//				poi.addElement("DEEP").add(cdata);
//			} else {
//				poi.addElement("DEEP");
//			}
//			poi.addElement("RTI");
//			poi.addElement("START_TIME");
//			poi.addElement("END_TIME");
//			if (item.surrounding_info != null) {
//				poi.addElement("SURROUNDING_INFO").addText(Common.formatXML(item.surrounding_info));
//			} else {
//				poi.addElement("SURROUNDING_INFO").addText("");
//			}
//			if ( isbase ) {
//				poi.addElement("CLUSTER_FLAG").addText("0");
//			}
//			poi.addElement("SHORT_ROAD").addText("");
//
//			return poi.asXML();
//		} catch (Exception e) {
//			throw e;
//		}
//	}

    /*** 修正bbs数据深度信息 */
//	public final static String makeupBbsSource(DataType datatype, String deepstr) {
//		if (datatype == DataType.BBS) {
//			if (deepstr!=null && !deepstr.trim().equals("")) {
//				deepstr = deepstr.replace("<?xml version=\"1.0\" encoding=\"GBK\"?>\n", "");
//				deepstr = deepstr.replace("\n", " ").replace("\r", " ").replace("\t", " ");
//				if ( !deepstr.trim().equals("") )
//					deepstr = "<DEEP><![CDATA[<additional>"+deepstr+"</additional>]]></DEEP>";
//			}
//		}
//		return deepstr;
//	}

    /**
     * 字段补齐
     */
//	public final static void makeuppoi(Poi poi) {
//		if (poi.guid == null || poi.guid.trim().equals("")) {
//			poi.guid = poi.src_id;
//		}
//
//		if ((poi.province == null || poi.province.trim().equals("")
//				|| poi.city == null || poi.city.trim().equals("")
//				|| poi.county == null || poi.county.trim().equals(""))
//				&& poi.x != 0 && poi.y != 0) {
//			double[] xy = new double[2];
//			xy[0] = poi.x;
//			xy[1] = poi.y;
//			String[] region = null;
//			region = Region.find(xy[0], xy[1]);
//			if (region != null) {
//				poi.province = region[0];
//				poi.city = region[1];
//				poi.county = region[2];
//				poi.hidden_folder = "无缝" + poi.city;
//				poi.hidden_memo = poi.hidden_folder + "_POI0";
//				poi.doFlag = DoFlag.HANDLED.toString();
//			}else {
//				poi.doFlag = DoFlag.C_OR_CATEGORY_ERROR.toString();
//				return;		// must return!
//			}
//			String k = poi.bigclass+"--"+poi.smallclass;
//			if(poi.flag!=null && poi.flag.equalsIgnoreCase("A")
//					&& (poi.name==null || poi.name.equals("")
//					|| !typeTable.containsKey(k)
//					|| poi.x == 0 || poi.y == 0 )){
//				poi.doFlag = DoFlag.C_OR_CATEGORY_ERROR.toString();
//			}else {
//				poi.doFlag = DoFlag.HANDLED.toString();
//				return;		// must return!
//			}
//		}
//	}

    /**
     * poi field pad
     * <p> 用sdb库中数据补齐新数据中缺失的字段
     * <p> 不补全<b>dataid</b>和<b>guid</b>
     * <p> <b>uniqueid</b>之所以补全是因为bbs来的数据没有<b>uniqueid</b>
     */
    public final static void padPoiField(Poi xml, Poi onlinePoi) {
        if (xml.name == null) {
            xml.name = onlinePoi.name;
        }
        if (xml.englishname == null) {
            xml.englishname = onlinePoi.englishname;
        }
        if (onlinePoi.uniqueid != 0) {
            xml.uniqueid = onlinePoi.uniqueid;
        }
        if (xml.alias == null || xml.alias.equals("")) {
            xml.alias = onlinePoi.alias;
        }
        if (xml.mergealias == null || xml.mergealias.equals("")) {
            xml.mergealias = onlinePoi.mergealias;
        }
        if (xml.keyword == null || xml.keyword.equals("")) {
            xml.keyword = onlinePoi.keyword;
        }
        if (xml.phone == null) {
            xml.phone = onlinePoi.phone;
        }
        if (xml.address == null) {
            xml.address = onlinePoi.address;
        }
        if (xml.postalcode == null || xml.postalcode.equals("")) {
            xml.postalcode = onlinePoi.postalcode;
        }
        if (xml.url == null) {
            xml.url = onlinePoi.url;
        }
        if (xml.brief == null) {
            xml.brief = onlinePoi.brief;
        }
        if (xml.bigclass == null || xml.bigclass.equals("")) {
            xml.bigclass = onlinePoi.bigclass;
        }
        if (xml.smallclass == null || xml.smallclass.equals("")) {
            xml.smallclass = onlinePoi.smallclass;
        }
        if (xml.source == null || xml.source.equals("")) {
            xml.source = onlinePoi.source;
        }
        if (xml.laiyuan == null || xml.laiyuan.equals("")) {
            xml.laiyuan = onlinePoi.laiyuan;
        }
        if (xml.layercode == null || xml.layercode.equals("")) {
            xml.layercode = onlinePoi.layercode;
        }
        if (xml.labelname == null || xml.labelname.equals("")) {
            xml.labelname = onlinePoi.labelname;
        }
        if (xml.from_x == 0) {
            xml.from_x = onlinePoi.from_x;
        }
        if (xml.to_x == 0) {
            xml.to_x = onlinePoi.to_x;
        }
        if (xml.g_from == 0) {
            xml.g_from = onlinePoi.g_from;
        }
        if (xml.g_to == 0) {
            xml.g_to = onlinePoi.g_to;
        }
        if (xml.directionflag == null || xml.directionflag.equals("")) {
            xml.directionflag = onlinePoi.directionflag;
        }
        if (xml.adjustablerank == 0) {
            xml.adjustablerank = onlinePoi.adjustablerank;
        }
        if (xml.kind == null || xml.kind.equals("")) {
            xml.kind = onlinePoi.kind;
        }
        if (xml.sign == null || xml.sign.equals("")) {
            xml.sign = onlinePoi.sign;
        }
        if (xml.cpid == 0) {
            xml.cpid = onlinePoi.cpid;
        }
        //if (xml.lastmodify == 0) {
        //	xml.lastmodify = onlinePoi.lastmodify;
        //}
        if (xml.modifyname == null || xml.modifyname.equals("")) {
            xml.modifyname = onlinePoi.modifyname;
        }
        if (xml.province == null || xml.province.equals("")) {
            xml.province = onlinePoi.province;
        }
        if (xml.city == null || xml.city.equals("")) {
            xml.city = onlinePoi.city;
        }
        if (xml.county == null || xml.county.equals("")) {
            xml.county = onlinePoi.county;
        }
        if (xml.hidden_folder == null || xml.hidden_folder.equals("")) {
            xml.hidden_folder = onlinePoi.hidden_folder;
        }
        if (xml.hidden_memo == null || xml.hidden_memo.equals("")) {
            xml.hidden_memo = onlinePoi.hidden_memo;
        }
        if (xml.flag == null || xml.flag.equals("")) {
            xml.flag = onlinePoi.flag;
        }
        // if (xml.guid==null && xml.guid.equals("")) {
        // 	 xml.guid = onlinePoi.guid;
        // }
        if (xml.x == 0) {
            xml.x = onlinePoi.x;
        }
        if (xml.y == 0) {
            xml.y = onlinePoi.y;
        }
        if (xml.apply == null || xml.apply.equals("")) {
            xml.apply = onlinePoi.apply;
        }
        if (xml.surrounding_info == null || xml.surrounding_info.equals("")) {
            xml.surrounding_info = onlinePoi.surrounding_info;
        }
        if (xml.entity == null || xml.entity.equals("")) {
            xml.entity = onlinePoi.entity;
        }
        if (xml.credibility == null || xml.credibility.equals("")) {
            xml.credibility = onlinePoi.credibility;
        }
        if (xml.nav_class == null || xml.nav_class.equals("")) {
            xml.nav_class = onlinePoi.nav_class;
        }
        if (xml.nav_pid == null || xml.nav_pid.equals("")) {
            xml.nav_pid = onlinePoi.nav_pid;
        }
        if (xml.nav_p_pid == null || xml.nav_p_pid.equals("")) {
            xml.nav_p_pid = onlinePoi.nav_p_pid;
        }
        if (xml.tag == null || xml.tag.equals("")) {
            xml.tag = onlinePoi.tag;
        }
        if (xml.src_id == null || xml.src_id.equals("")) {
            xml.src_id = onlinePoi.guid;
        }
        if (xml.deep == null || xml.deep.equals("")) {
            xml.deep = onlinePoi.deep;
        }
    }

    /**
     * 解析Poi
     */
    public static Poi parsePoi(String xmlstr) throws Exception {
        Poi poi = new Poi();
        try {
            Document doc = DocumentHelper.parseText(xmlstr);
            doc.setXMLEncoding("GBK");
            Element root = doc.getRootElement();
            poi.src_id = root.elementText("SRC_ID");
            poi.name = root.elementText("SRC_NAME");
            poi.englishname = root.elementText("SRC_NAME_ENG");
            poi.alias = root.elementText("SRC_ALIAS");
            poi.keyword = root.elementText("KEYWORDS");
            poi.dataid = root.elementText("DATAID");
            poi.phone = root.elementText("PHONE");
            poi.address = root.elementText("SRC_ADDRESS_CHN");
            poi.postalcode = root.elementText("POSTCODE");
            poi.url = root.elementText("SRC_URL");
            poi.brief = root.elementText("INTRODUCTION");
            String type = root.elementText("SRC_TYPE");
            String tps[] = type.split(";;");
            if (tps.length > 0) {
                poi.bigclass = tps[0];
                poi.smallclass = tps[1];
            }
            poi.laiyuan = root.elementText("LAI_YUAN");
            poi.layercode = root.elementText("LAYERCODE");
            poi.surrounding_info = root.elementText("SURROUNDING_INFO");
            poi.surrounding_info = split1000Str(poi.surrounding_info, ";");
            poi.from_x = 0;
            poi.to_x = 0;
            poi.g_from = 0;
            poi.g_to = 0;
            poi.adjustablerank = 0;
            poi.kind = root.elementText("SRC_TYPECODE");
            poi.sign = "";
            poi.cpid = 1;
            String modifytime = root.elementText("MODIFY_TIME");
            if (modifytime != null && !modifytime.trim().equals(""))
                poi.lastmodify = DateTime.parse(modifytime);
            else // default timestamp,should not go this!
                poi.lastmodify = System.currentTimeMillis();
            poi.modifyname = "shj";
            poi.province = root.elementText("SRC_PROVINCE");
            poi.city = root.elementText("SRC_CITY");
            poi.county = root.elementText("SRC_COUNTY");
            poi.hidden_folder = "无缝" + poi.city;
            poi.hidden_memo = poi.hidden_folder + "_POI0";
            poi.x = Double.valueOf(root.elementText("SRC_X"));
            poi.y = Double.valueOf(root.elementText("SRC_Y"));
            poi.apply = "";
            poi.source = "";
            try {
                poi.uniqueid = Long.valueOf(root.elementText("UNIQUEID"));
            } catch (Exception e) {
                // format data's UNIQUEID is empty.
                poi.uniqueid = 0;
            }
            poi.credibility = "";
            Element nav = root.element("NAV");
            if (nav != null) {
                poi.nav_class = nav.elementText("NAV_CLASS");
                poi.nav_pid = nav.elementText("NAV_PID");
                poi.nav_p_pid = nav.elementText("NAV_P_PID");
            } else {
                poi.nav_class = "";
                poi.nav_pid = "";
                poi.nav_p_pid = "";
            }
            poi.tag = root.elementText("TAG");
            poi.mergealias = root.elementText("NAMEALIAS");
            poi.guid = root.elementText("GUID");
            String deep = root.element("DEEP").asXML();
            poi.deep = deep.replace("<![CDATA[]]>", "");
            poi.flag = root.elementText("STATUS_FLAG");
            poi.layercode = root.elementText("LAYERCODE");
            poi.labelname = root.elementText("LABELNAME");
            poi.short_road = root.elementText("SHORT_ROAD");


            Document document = parseDeepDocument(poi.deep);


            if(document != null){
                Node node = document.selectSingleNode("/root/additional/data/items");

                String hotelcommentnum = parseEntity(node,"item[@source='ELONG']", "hotelcommentnum");
                poi.hotelcommentnum = hotelcommentnum;

                String commentcount = parseEntity(node, "item[@source='TONGCHENG']","commentcount");
                poi.commentcount = commentcount;

                String recordCount = parseEntity(node,"item[@source='DIANPING']/ReviewList", "RecordCount");
                poi.recordCount = recordCount;

                String avgPrice = parseEntity(node,"item[@source='DIANPING']/Shop", "AvgPrice");
                poi.avgPrice = avgPrice;


                //
                String hotelroomprice = parseEntity(node,"item[@source='ELONG']/minprice", "hotelroomprice");
                poi.minprice = hotelroomprice;


                String price = parseEntity(node,"item[@source='TONGCHENG']", "price");

                poi.price = price;

                String sellingPrice = parseEntity(node,"item[@source='58']/poi", "selling_price");
                if(StringUtils.isNotBlank(sellingPrice) && sellingPrice.contains(".")){
                    int end = sellingPrice.lastIndexOf(".");
                    poi.sellingPrice=sellingPrice.substring(0,end);

                }else{
                    poi.sellingPrice = sellingPrice;
                }

                String hotelrank = parseEntity(node,"item[@source='ELONG']", "hotelrank");
                poi.hotelrank = hotelrank;

                String praise = parseEntity(node,"item[@source='TONGCHENG']", "praise");
                poi.praise = praise;

                String scoremap = parseEntity(node,"item[@source='DIANPING']/Shop", "Scoremap");
                poi.scoremap = scoremap;
            }




            Element structs = root.element("STRUCTS");
            if (structs != null) {
                poi.structs = structs.asXML();
            }
        } catch (Exception e) {
            poi = null;
            e.printStackTrace();
            log.error("parse xml error: " + xmlstr);
            throw e;
        }
        return poi;
    }


    /**
     * 解析id
     * @param document
     * @param field
     * @return: Iterator<Element>
     */
    @SuppressWarnings("unchecked")
    public static String parseEntity(Node document,String nodePath,String field) throws Exception {
        try {
            String result = "0";
            List list = document.selectNodes(nodePath);
            if(list.isEmpty()){
                return result;
            }
            for(Object o:list){

                Element e = (Element) o;
                result=e.element(field).getTextTrim();
                System.out.println(result);
            }
            if(StringUtils.isBlank(result)){
                return  "0";
            }
            if(result.contains(".")){
                return result.substring(0,result.indexOf("."));
            }else{
                return result;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "0";
        }
    }

    /**
     * 超过1000字符的截断
     * @param str 待截取字符串
     * @param regex 分隔符
     * @return: String
     */
    public static String split1000Str(String str, String regex) {
        if (str == null || str.trim().equals("")
                || str.length() <= 1000)
            return str;

        String ret = "";
        boolean flag = false;
        String strarr[] = str.split(regex);
        for (int i = 0; i < strarr.length; i++) {
            ret += strarr[i] + regex;
            while (ret.length() >= 1000) {
                flag = true;
                int index = ret.lastIndexOf(regex);
                if (index == -1) {
                    ret = ret.substring(0, 999);
                } else {
                    ret = ret.substring(0, index);
                }
                if (ret.length() <= 1000) break;
            }
            if (flag) break;
        }
        return ret;
    }

    ///////////////////////////////////xml support/////////////////////////////

    /**
     * 解析深度信息字符串为Element
     * @param deepstr
     * @return: Element
     * @throws DocumentException
     */
    public static Element parseDeepElement(String deepstr) throws DocumentException {
        Element deepelem = null;
        try {
            if (deepstr == null || deepstr.trim().equals("")) {
                return null;
            }

            Document doc = DocumentHelper.parseText(deepstr);
            Element root = doc.getRootElement();
            Iterator<?> iter = root.content().iterator();
            while (iter.hasNext()) {
                Object elem = iter.next();
                if (elem instanceof DefaultCDATA) {
                    CDATA cdata = (CDATA) elem;
                    String cdataxml = "<root>" + cdata.getText() + "</root>";
                    Document deepdoc = DocumentHelper.parseText(cdataxml);
                    deepdoc.setXMLEncoding("GBK");
                    deepelem = deepdoc.getRootElement();
                    break;
                }
            }
        } catch (DocumentException e) {
            throw e;
        }
        return deepelem;
    }


    /**
     * 解析深度信息字符串为Element
     * @param deepstr
     * @return: Element
     * @throws DocumentException
     */
    public static Document parseDeepDocument(String deepstr) throws DocumentException {
        Document deepdoc = null;
        Element deepelem = null;
        try {
            if (deepstr == null || deepstr.trim().equals("")) {
                return null;
            }

            Document doc = DocumentHelper.parseText(deepstr);
            Element root = doc.getRootElement();
            Iterator<?> iter = root.content().iterator();
            while (iter.hasNext()) {
                Object elem = iter.next();
                if (elem instanceof DefaultCDATA) {
                    CDATA cdata = (CDATA) elem;
                    String cdataxml = "<root>" + cdata.getText() + "</root>";
                    deepdoc = DocumentHelper.parseText(cdataxml);
                    deepdoc.setXMLEncoding("GBK");
                    deepelem = deepdoc.getRootElement();
                    break;
                }
            }
        } catch (DocumentException e) {
            throw e;
        }
        return deepdoc;
    }





















    /**
     * 解析id,不包括解析点评id
     * @param elem
     * @param type
     * @return: Iterator<Element>
     */
    @SuppressWarnings("unchecked")
    public static String parseEntityId(Element elem, String type) throws Exception {
        try {
            String id = null;
            if (elem == null) return id;

            Element data = elem.element("data");
            Element items = data.element("items");
            Iterator<Element> iter = items.elements("item").iterator();
            List<String> idlist = new ArrayList<String>();
            while (iter != null && iter.hasNext()) {
                Element e = iter.next();
                if (type.equals("sogou123")) {
                    id = e.elementText("shopid");
                    idlist.add(id);
                } else if (type.equals("meituan")) {
                    id = e.elementText("id");
                    idlist.add(id);
                } else if (type.equals("dianpingTuan")) {
                    Element tuan = e.element("tuan");
                    Element detail = tuan.element("tuandetail");
                    Element businesses = detail.element("businesses");
                    List<Element> businessList = businesses.elements("business");
                    for (Element business : businessList) {
                        id = business.elementText("id");
                        idlist.add(id);
                    }
                } else {
                    // no match,continue
                }
            }
            if (idlist.size() == 1)
                return idlist.get(0);
            else if (idlist.size() == 0)
                return null;
            else {
                Map<String, Integer> map = null;
                map = new HashMap<String, Integer>();
                for (int i = 0; i < idlist.size(); i++) {
                    id = idlist.get(i);
                    Integer num = map.get(id);
                    if (num == null)
                        map.put(id, 1);
                    else
                        map.put(id, num + 1);
                }
                //
                Iterator<String> it = map.keySet().iterator();
                int maxvalue = -1;
                while (it.hasNext()) {
                    String tempid = it.next();
                    int num = map.get(id);
                    if (num >= maxvalue) {
                        maxvalue = num;
                        id = tempid;
                    }
                }
                return id;
            }
        } catch (Exception e) {
            return null;
        }
    }

//	/**
//	 * 解析点评团id
//	 * @param elem
//	 * @return: Iterator<Element>
//	 */
//	public static String parseDianpingId(Element elem) throws Exception{
//		try {
//			String id = null;
//			if (elem == null) return id;
//
//			Element data = elem.element("data");
//			Element items = data.element("items");
//			Iterator<Element> iter = items.elements("item").iterator();
//			while (iter!=null && iter.hasNext()) {
//				Element item = iter.next();
//				String source = item.attributeValue("source");
//				if (source.equals(DataType.DIANPING.echo())) {
//					Element shop = item.element("Shop");
//					id = shop.elementText("ShopID");
//					break;
//				}
//			}
//			return id;
//		} catch (Exception e) {
//			return null;
//		}
//	}

    //	/**
//	 * 从数据库的Entity字段中移除指定大字段
//	 * @param entity 数据库中的大字段
//	 * @param type 如[BBS|MANUAL...]
//	 * @return: String
//	 * @throws DocumentException
//	 */
    @SuppressWarnings("unchecked")
//	public static Element removeDeepFromEntity(Element elem, DataType type) {
//		try {
//			if (elem == null) return elem;
//
//			Element data = elem.element("data");
//			Element items = data.element("items");
//			List<Element> itemList = items.elements("item");
//			Iterator<Element> iter = itemList.iterator();
//			int count = itemList.size();
//			while ( iter.hasNext() ) {
//				Element item = iter.next();
//				String source = item.attributeValue("source");
//				if (  type.echo().equals(source) ) {
//					iter.remove();
//					count--;
//				}
//			}
//			if ( count > 0 )
//				return elem;
//			else
//				return null;
//		} catch (Exception e) {
//			return elem;
//		}
//	}

//	/**
//	 * 从数据库的Entity字段中解析出指定大字段
//	 * @param entity 数据库中的大字段
//	 * @param type 如[BBS|MANUAL...]
//	 * @return: String
//	 * @throws DocumentException
//	 */
//	public static String parseDeepFromEntity(String entity, DataType type) {
//		try {
//			if ( entity == null || entity.trim().equals("") || type == null) {
//				return null;
//			}
//
//			boolean iscontainstype = false;
//			Document doc = DocumentHelper.parseText(entity);
//			Element root = doc.getRootElement();
//			Element items = root.element("items");
//			List<Element> itemList = items.elements("item");
//			Iterator<Element> iter = itemList.iterator();
//			while ( iter.hasNext() ) {
//				Element item = iter.next();
//				String source = item.attributeValue("source");
//				if (  type.echo().equals(source) ) {
//					iscontainstype = true;
//				} else {
//					iter.remove();
//				}
//			}
//			if ( iscontainstype )
//				return root.asXML();
//			else
//				return null;
//		} catch (DocumentException e) {
//			return null;
//		}
//	}

//	/**
//	 * 从数据库的Entity字段中解析出大字段
//	 * @param entity 数据库中的大字段
//	 * @param type 如[BBS|MANUAL...]
//	 * @return: String
//	 * @throws DocumentException
//	 */
//	public static Element parseDeepElementFromEntity(String entity, DataType type) {
//		try {
//			if ( entity == null || entity.trim().equals("") || type == null) {
//				return null;
//			}
//
//			Document doc = DocumentHelper.parseText(entity);
//			Element root = doc.getRootElement();
//			Element items = root.element("items");
//			List<Element> itemList = items.elements("item");
//			Iterator<Element> iter = itemList.iterator();
//			while ( iter.hasNext() ) {
//				Element item = iter.next();
//				String source = item.attributeValue("source");
//				if (  type.echo().equals(source) ) {
//					return item;
//				}
//			}
//			return null;
//		} catch (Exception e) {
//			return null;
//		}
//	}

    /**
     * 更新数据库的Entity字段中指定类型的深度信息
     * @param entity 数据库中的大字段
     * @param type 如[BBS|MANUAL...]
     * @return: String
     * @throws DocumentException
     */
//	public static String updateDeepOfEntity(String entity, DataType type, String newinfo) {
//		try {
//			boolean iscontainstype = false;
//			if ( type == null ) return null;
//			Element elem = parseDeepElementFromEntity(newinfo, type);
//			if ((entity==null || entity.trim().equals("")) && elem==null) {
//				return entity;
//			}
//
//			// entity and elem can't both be null
//			Document doc = null;
//			Element root = null;
//			Element items = null;
//			List<Element> itemList = null;
//			if ( elem != null )  {	// insert
//				try {
//					doc = DocumentHelper.parseText(entity);
//				} catch (Exception e) {
//					doc = DocumentHelper.createDocument();
//					doc.setXMLEncoding("GBK");
//				}
//				root = doc.getRootElement();
//				if ( root == null ) {
//					root = doc.addElement("data");
//				}
//				items = root.element("items");
//				if ( items == null ) {
//					items = root.addElement("items");
//				}
//				itemList = items.elements("item");
//				if ( itemList.size() == 0 ) {
//					items.content().add(elem);
//					return root.asXML();
//				}
//			} else {
//				doc = DocumentHelper.parseText(entity);
//				root = doc.getRootElement();
//				items = root.element("items");
//				itemList = items.elements("item");
//			}
//
//			Iterator<Element> iter = itemList.iterator();
//			while ( iter.hasNext() ) {
//				Element item = iter.next();
//				int index = itemList.indexOf(item);
//				String source = item.attributeValue("source");
//				if (type.echo().equals(source)) {
//					iscontainstype = true;
//					List<Element> content = item.getParent().content();
//					if ( elem != null ) {	// update
//						content.set(index, elem);
//					} else {	// delete
//						content.remove(index);
//					}
//					break;
//				}
//			}
//			if ( !iscontainstype && elem != null) {	// insert
//				items.content().add(elem);
//			}
//			return root.asXML();
//		} catch (Exception e) {
//			return entity;
//		}
//	}

    /**
     * 验证additional有效性
     * @param elem
     * @return: boolean
     */
//	@SuppressWarnings("unchecked")
//	public static boolean valid(Element elem) {
//		try {
//			if (elem == null) return false;
//
//			Element data = elem.element("data");
//			Element items = data.element("items");
//			List<Element> itemList = items.elements("item");
//			for ( Element item : itemList ) {
//				String source = item.attributeValue("source");
//				String lastmodify = item.attributeValue("lastmodify");
//				Element srcElem = item.element("source");
//				if (source==null&&lastmodify==null&&srcElem!=null) {
//					source = srcElem.attributeValue("source");
//					lastmodify = srcElem.attributeValue("lastmodify");
//				}
//
//				if (source == null || source.trim().equals("")
//					||lastmodify == null || lastmodify.trim().equals("")) {
//					return false;
//				}
//			}
//			return true;
//		} catch (Exception e) {
//			return false;
//		}
//	}

    /**
     * 选择1000条记录
     * <p>不是多线程安全的
     */
//	public static Map<String, Poi> selectMap(Map<String, Poi> updateMap) {
//		Map<String, Poi> poiMap = new HashMap<String, Poi>();
//		Iterator<String> iter = updateMap.keySet().iterator();
//		while ( iter.hasNext() ) {
//			String key = iter.next();
//			Poi value = updateMap.get(key);
//			poiMap.put(key, value);
//			iter.remove();
//			if (poiMap.size()>0 && poiMap.size()%1000==0) {
//				return poiMap;
//			}
//		}
//		return poiMap;
//	}

    /**
     * 选择1000条记录
     * <p>不是多线程安全的
     */
    public static Map<String, String> selectStrMap(Map<String, String> updateMap) {
        Map<String, String> map = new HashMap<String, String>();
        Iterator<String> iter = updateMap.keySet().iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            String value = updateMap.get(key);
            map.put(key, value);
            iter.remove();
            if (map.size() > 0 && map.size() % 1000 == 0) {
                return map;
            }
        }
        return map;
    }

    /**
     * 倒叙选择1000条记录
     * <p>不是多线程安全的
     *
     * @param xmlList
     * @param flag A|D|U|null
     * @return: List<Poi>
     */
    public static List<Poi> selectList(List<Poi> xmlList, String flag) {
        List<Poi> poiList = new ArrayList<Poi>();
        for (int i = xmlList.size() - 1; i >= 0; i--) {
            if (flag != null) {
                if (flag.equals(xmlList.get(i).flag))
                    poiList.add(xmlList.remove(i));
            } else {
                poiList.add(xmlList.remove(i));
            }
            if (poiList.size() > 0 && poiList.size() % 1000 == 0) {
                return poiList;
            }
        }
        return poiList;
    }

    /**
     * 倒叙选择1000条记录
     * <p>不是多线程安全的
     */
    public static List<String> selectStrList(List<String> list) {
        List<String> strList = new ArrayList<String>();
        for (int i = list.size() - 1; i >= 0; i--) {
            strList.add(list.remove(i));
            if (strList.size() > 0 && strList.size() % 1000 == 0) {
                return strList;
            }
        }
        return strList;
    }

    /**
     * 倒叙选择1000条记录
     * <p>不是多线程安全的
     */
    public static List<Map<String, Object>> selectMapList(List<Map<String, Object>> list) {
        List<Map<String, Object>> objList = new ArrayList<Map<String, Object>>();
        for (int i = list.size() - 1; i >= 0; i--) {
            objList.add(list.remove(i));
            if (objList.size() > 0 && objList.size() % 1000 == 0) {
                return objList;
            }
        }
        return objList;
    }

    /**
     * 转为标准XML,入库使用
     * @param elem
     * @throws DocumentException
     * @return: String
     */
    public static String toXML(Element elem) throws DocumentException {
        try {
            if (elem == null) return "";

            Element data = elem.element("data");
            Document xmldoc = DocumentHelper.parseText(data.asXML());
            xmldoc.setXMLEncoding("GBK");
            return xmldoc.asXML();
        } catch (DocumentException e) {
            throw e;
        }
    }

    /*** 清除poi数据的flag标签  */
    public static void clearFlag(List<Poi> poiList) {
        for (Poi poi : poiList) poi.flag = null;
    }

    /*** 过滤poiList,过滤掉uniqueid不等于0的数据 */
    public static void fliterPoiList(List<Poi> poiList) {
        Iterator<Poi> iter = poiList.iterator();
        while (iter.hasNext()) {
            Poi poi = iter.next();
            if (poi.uniqueid != 0)
                iter.remove();
        }
    }

    /**
     * 找到名称相同的最近的点
     * @param dest 目标点
     * @param poiList 同名poi列表
     * @param delta 单位为米
     * @return: List<Poi>
     */
    public static Poi findNearestPoi(Poi dest, List<Poi> poiList, double delta) {
        List<Poi> nearbyList = findNearbyPois(dest, poiList, delta);
        int minindex = -1;
        double mindistance = Double.POSITIVE_INFINITY;
        for (int i = 0; i < nearbyList.size(); i++) {
            Poi poi = nearbyList.get(i);
            double x0 = poi.x;
            double y0 = poi.y;
            double x1 = dest.x;
            double y1 = dest.y;
            String coords = x0 + "," + y0 + "," + x1 + "," + y1;
            double distance = CheckUtil.getLen(coords);
            if (distance < mindistance) {
                mindistance = distance;
                minindex = i;
            }
        }
        return (minindex != -1) ? nearbyList.get(minindex) : null;
    }

    /**
     * 找到名称相同的附近的点
     * @param dest 目标点
     * @param poiList 同名poi列表
     * @param delta 单位为米
     * @return: List<Poi>
     */
    public static List<Poi> findNearbyPois(Poi dest, List<Poi> poiList,
                                           double delta) {
        List<Poi> targetList = new ArrayList<Poi>();
        for (Poi poi : poiList) {
            double x0 = poi.x;
            double y0 = poi.y;
            double x1 = dest.x;
            double y1 = dest.y;
            String coords = x0 + "," + y0 + "," + x1 + "," + y1;
            double distance = CheckUtil.getLen(coords);
            if (distance <= delta) { // lt 50 mi
                targetList.add(poi);
            }
        }
        return targetList;
    }

    /**
     * 获取大小类表
     * @Title: getTypeTable
     * @return: Hashtable<String,String>
     */
//	public static Hashtable<String, String> getTypeTable() {
//		HSSFWorkbook wb = null;
//		Hashtable<String, String> typeTable = new Hashtable<String, String>();
//		try {
//			String fileName = Config.PATH + "config/config_type_table.xls";
//			InputStream is = new FileInputStream(fileName);
//			POIFSFileSystem fs = new POIFSFileSystem(is);
//			wb = new HSSFWorkbook(fs);
//			HSSFSheet sheet = wb.getSheetAt(0);
//			int firstRow = 1;
//			int lastRow = sheet.getLastRowNum();
//			String bigClass = null;
//			String smallClass = null;
//			for (int r = firstRow; r <= lastRow; r++) {
//				HSSFRow row = sheet.getRow(r);
//				if (row == null) {
//					continue;
//				}
//				HSSFCell bClass = row.getCell(1);
//				HSSFCell sClass = row.getCell(2);
//				if (bClass != null && !bClass.getStringCellValue().equals("")
//					&&(sClass==null||sClass.getStringCellValue().equals(""))) {
//					bigClass = bClass.getStringCellValue().trim();
//					continue;
//				}
//				if (bigClass!=null && sClass!=null
//					&&!sClass.getStringCellValue().trim().equals("")){
//					smallClass = sClass.getStringCellValue().trim();
//					typeTable.put(bigClass + "--" + smallClass, "1".intern());
//				}
//			}
//			wb.close();
//		} catch (Exception e) {
//			log.error("Read config_type_table.xml error!");
//			e.printStackTrace(System.err);
//		} finally {
//			if(wb!=null)
//				try {
//					wb.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//		}
//		return typeTable;
//	}

    /**
     * 获取大小类映射表
     * @Title: getCommunityTable
     * @throws FileNotFoundException
     * @throws IOException
     * @return: Hashtable<String,String>
     */
//	public static Hashtable<String, String> getCommunityTable(){
//		HSSFWorkbook wb = null;
//		Hashtable<String, String> communityTable = new Hashtable<String, String>();
//		try {
//			String fileName = Config.PATH + "config/config_community_type_table.xls";
//			InputStream is = new FileInputStream(fileName);
//			POIFSFileSystem fs = new POIFSFileSystem(is);
//			wb = new HSSFWorkbook(fs);
//			HSSFSheet sheet = wb.getSheetAt(0);
//			int firstRow = 1;
//			int lastRow = sheet.getLastRowNum();
//			for (int r = firstRow; r <= lastRow; r++) {
//				HSSFRow row = sheet.getRow(r);
//				if (row == null) {
//					continue;
//				}
//				HSSFCell bbsBClass = row.getCell(1);
//				if (bbsBClass == null || bbsBClass.getStringCellValue().equals("")
//						|| bbsBClass.getStringCellValue().equals("大类")) {
//					continue;
//				}
//				String bbsBC = row.getCell(1).getStringCellValue().trim();
//				String bbsSC = row.getCell(2).getStringCellValue().trim();
//				String go2BC = row.getCell(5).getStringCellValue().trim();
//				String go2SC = row.getCell(6).getStringCellValue().trim();
//				communityTable.put(bbsBC + "--" + bbsSC, go2BC + "--" + go2SC);
//			}
//			wb.close();
//		} catch (Exception e) {
//			log.error("Read config_community_type_table.xls error!");
//			e.printStackTrace(System.err);
//		} finally {
//			if(wb!=null)
//				try {
//					wb.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//		}
//		return communityTable;
//	}

    /**
     * 读取excel数据
     * <p><b> 默认会把第一行当做表头</p>
     * @param fileName excel文件名,接受xls格式
     * @param index sheet索引,从0开始
     * @return: List<Map<String, Object>>
     */
    public static List<Map<String, Object>> readXlsExcel(String fileName, int index)
            throws IOException {
        HSSFWorkbook wb = null;
        List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
        try {
            InputStream is = new FileInputStream(fileName);
            POIFSFileSystem fs = new POIFSFileSystem(is);
            wb = new HSSFWorkbook(fs);
            HSSFSheet sheet = wb.getSheetAt(index);
            HSSFRow header = sheet.getRow(0);
            sheet.removeRow(header);

            Iterator<Row> rowIter = sheet.rowIterator();
            while (rowIter.hasNext()) {
                Row row = rowIter.next();
                Map<String, Object> m = new HashMap<String, Object>();
                Iterator<Cell> hIter = header.cellIterator();
                while (hIter.hasNext()) {
                    Cell hCell = hIter.next();
                    int hIndex = hCell.getColumnIndex();
                    String hName = hCell.getStringCellValue();

                    Object value = "";
                    Cell cell = row.getCell(hIndex);
                    if (cell != null) {
                        switch (cell.getCellType()) {
                            case Cell.CELL_TYPE_NUMERIC:
                                cell.setCellType(Cell.CELL_TYPE_STRING);
                                value = cell.getStringCellValue();
                                break;
                            case Cell.CELL_TYPE_BOOLEAN:
                                value = cell.getBooleanCellValue();
                                break;
                            case Cell.CELL_TYPE_ERROR:
                            case Cell.CELL_TYPE_FORMULA:
                            case Cell.CELL_TYPE_BLANK:
                            case Cell.CELL_TYPE_STRING:
                                value = cell.getStringCellValue();
                                break;
                        }
                    }
                    m.put(hName.toUpperCase(), value);
                }
                dataList.add(m);
            }
            wb.close();
        } catch (IOException e) {
            log.error("Read " + fileName + " error!", e);
        } finally {
            if (wb != null)
                try {
                    wb.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
        return dataList;
    }

    /**
     * 读取商量excel数据
     * <p><b> 默认会把第一行当做表头</p>
     * @param fileName excel文件名,接受xlsx格式
     * @param index sheet索引,从0开始
     * @return: List<Map<String, Object>>
     */
    public static List<Map<String, Object>> readXlsxExcel(String fileName, int index)
            throws IOException, InvalidFormatException {
        XSSFWorkbook wb = null;
        List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
        try {
            InputStream is = new FileInputStream(fileName);
            OPCPackage opcPackage = OPCPackage.open(is);
            wb = new XSSFWorkbook(opcPackage);
            XSSFSheet sheet = wb.getSheetAt(index);
            XSSFRow header = sheet.getRow(0);

            boolean excludeHeader = true;// 首行作为map键
            Iterator<Row> rowIter = sheet.rowIterator();
            while (rowIter.hasNext()) {
                Row row = rowIter.next();
                if (excludeHeader) {
                    excludeHeader = false;
                    continue;
                }
                Map<String, Object> m = new HashMap<String, Object>();
                Iterator<Cell> hIter = header.cellIterator();
                while (hIter.hasNext()) {
                    Cell hCell = hIter.next();
                    int hIndex = hCell.getColumnIndex();
                    String hName = hCell.getStringCellValue();

                    Object value = "";
                    Cell cell = row.getCell(hIndex);
                    if (cell != null) {
                        switch (cell.getCellType()) {
                            case Cell.CELL_TYPE_NUMERIC:
                                cell.setCellType(Cell.CELL_TYPE_STRING);
                                value = cell.getStringCellValue();
                                break;
                            case Cell.CELL_TYPE_BOOLEAN:
                                value = cell.getBooleanCellValue();
                                break;
                            case Cell.CELL_TYPE_ERROR:
                            case Cell.CELL_TYPE_FORMULA:
                            case Cell.CELL_TYPE_BLANK:
                            case Cell.CELL_TYPE_STRING:
                                value = cell.getStringCellValue();
                                break;
                        }
                    }
                    m.put(hName.toUpperCase(), value);
                }
                dataList.add(m);
            }
            wb.close();
        } catch (IOException e) {
            log.error("Read " + fileName + " error!", e);
        } finally {
            if (wb != null)
                try {
                    wb.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
        return dataList;
    }

    /**
     * 读取大量excel数据
     * @param fileName excel文件名,接受xlsx格式
     * @param sheetName sheet名称
     * @param minColumns 最小列数
     * @return: List<String[]>
     */
//	public static List<String[]> readXlsxExcel(String path, String sheetName,
//			int minColumns) throws Exception {
//		return XLSXCovertCSVReader.readerExcel(path, sheetName, minColumns);
//	}

    /**
     * 去除首尾引号
     * @param str
     * @return: String
     */
    public final static String trim(String str) {
        if (str == null || str.equals(""))
            return str;
        if (str.startsWith("\""))
            str = str.substring(1);
        if (str.endsWith("\""))
            str = str.substring(0, str.length() - 1);
        return str.trim();
    }


    public static void main(String[] args) {


        String str="18736.0元/";

        int end = str.lastIndexOf(".");
        String ssellingPrice=str.substring(0,end);
        System.out.println(ssellingPrice);



        String path = "D:\\structure\\test";

        try {

            BufferedReader bufferedReader = FileHandler.getReader(path, "gb18030");
            BufferedWriter writer = new BufferedWriter(new FileWriter("D:\\structure\\xm.txt"));
            int count = 0;
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                Poi poi = parsePoi(line);

                Joiner joiner = Joiner.on("\t");

                String logStr = joiner.join(new String[]{poi.name, poi.minprice, poi.hotelrank, poi
                        .hotelcommentnum, poi.price, poi.praise, poi.commentcount, poi.sellingPrice, poi.scoremap,
                        poi.recordCount,poi.avgPrice
                });


                System.out.println(logStr);
            }

            writer.flush();
            writer.close();
            System.out.println("finish");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
