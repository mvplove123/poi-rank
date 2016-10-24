package com.map.util;

import com.map.model.Rank;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2016/4/1.
 * 分类阀值
 */
public class CategoryThreshold {

    public static Map<String, Rank> getCategoryRank() {

        Map<String, Rank> cateRank = Maps.newHashMap();

        cateRank.put("lvYouJingDian", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("binGuanFanDian", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("yiLiaoWeiSheng", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("fangDiChan", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("xueXiaoKeYan", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("canYinFuWu", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("xiuXianYuLe", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("jinRongYinHang", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("changGuanHuiSuo", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("gongSiQiYe", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("youZhengDianXin", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("zhengFuJiGuan", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("qiCheFuWu", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("gouWuChangSuo", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("jiaoTongChuXing", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("diMing", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("xinWenMeiTi", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("tiYuChangGuan", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        cateRank.put("qiTa", new Rank(new Double[]{0.01, 0.05, 0.20, 0.70,}, new Integer[]{ 5, 4, 3, 2, 1}));
        return cateRank;
    }


    public static Map<String,Integer> getCategoryGps(){
        Map<String, Integer> categoryGps = Maps.newHashMap();
        categoryGps.put("地名-村庄",300);
        categoryGps.put("地名-地名",500);
        categoryGps.put("地名-其它",null);
        categoryGps.put("地名-区县",null);
        categoryGps.put("地名-乡镇",1000);
        categoryGps.put("旅游景点-5A4A景点",700);

        categoryGps.put("房地产-别墅",500);
        categoryGps.put("房地产-居民小区",500);
        categoryGps.put("房地产-楼号",500);
        categoryGps.put("房地产-楼盘",500);
        categoryGps.put("交通出行-大型火车站",500);
        categoryGps.put("交通出行-航站楼",500);
        categoryGps.put("交通出行-火车站",500);
        categoryGps.put("交通出行-机场",500);
        categoryGps.put("旅游景点-1-3A景点",500);
        categoryGps.put("旅游景点-知名景点",500);
        categoryGps.put("其它-大型化工厂",500);
        categoryGps.put("其它-大型热电厂",500);
        categoryGps.put("其它-大型制药厂",500);
        categoryGps.put("其它-垃圾处理厂",500);
        categoryGps.put("其它-垃圾填埋场",500);
        categoryGps.put("其它-污水处理厂",500);
        categoryGps.put("汽车服务-驾校",500);
        categoryGps.put("汽车服务-检测场",500);
        categoryGps.put("体育场馆-大型体育场馆",500);
        categoryGps.put("学校科研-知名大学",500);

        categoryGps.put("房地产-公寓",300);

        categoryGps.put("场馆会所-大型博物馆",200);
        categoryGps.put("场馆会所-大型展览馆",200);
        categoryGps.put("房地产-高档楼盘",200);
        categoryGps.put("公司企业-工厂",200);
        categoryGps.put("公司企业-知名工厂",200);
        categoryGps.put("交通出行-长途客运站",200);
        categoryGps.put("交通出行-地铁站",200);
        categoryGps.put("旅游景点-度假村",200);
        categoryGps.put("旅游景点-公园",200);
        categoryGps.put("旅游景点-景点",200);
        categoryGps.put("旅游景点-绿地点",200);
        categoryGps.put("宾馆饭店-4-5星级",100);
        categoryGps.put("宾馆饭店-酒店式公寓",100);
        categoryGps.put("宾馆饭店-普通",100);
        categoryGps.put("宾馆饭店-其它",100);
        categoryGps.put("宾馆饭店-其它星级",100);
        categoryGps.put("宾馆饭店-招待所",100);
        categoryGps.put("场馆会所-博物馆",100);
        categoryGps.put("场馆会所-大型图书馆",100);
        categoryGps.put("场馆会所-展览馆",100);
        categoryGps.put("地名-水系点",100);
        categoryGps.put("房地产-高档写字楼",100);
        categoryGps.put("房地产-其它",100);
        categoryGps.put("房地产-写字楼",100);
        categoryGps.put("购物场所-大型商场",100);
        categoryGps.put("购物场所-电脑城",100);
        categoryGps.put("购物场所-电器城",100);
        categoryGps.put("购物场所-服装市场",100);
        categoryGps.put("购物场所-花鸟市场",100);
        categoryGps.put("购物场所-家居市场",100);
        categoryGps.put("购物场所-建材市场",100);
        categoryGps.put("购物场所-旧货市场",100);
        categoryGps.put("购物场所-农贸市场",100);
        categoryGps.put("购物场所-批发市场",100);
        categoryGps.put("旅游景点-教堂",100);
        categoryGps.put("旅游景点-其它",100);
        categoryGps.put("汽车服务-停车场",100);
        categoryGps.put("汽车服务-修理厂",100);
        categoryGps.put("体育场馆-健身场所",100);
        categoryGps.put("体育场馆-体育场馆",100);
        categoryGps.put("新闻媒体-电视台",100);
        categoryGps.put("学校科研-大学",100);
        categoryGps.put("学校科研-大专",100);
        categoryGps.put("学校科研-一般大学",100);
        categoryGps.put("医疗卫生-二级医院",100);
        categoryGps.put("医疗卫生-三级医院",100);
        categoryGps.put("政府机关-政府驻地",100);

        categoryGps.put("宾馆饭店-楼号",50);
        categoryGps.put("餐饮服务-楼号",50);
        categoryGps.put("场馆会所-大门",50);
        categoryGps.put("场馆会所-俱乐部",50);
        categoryGps.put("场馆会所-楼号",50);
        categoryGps.put("场馆会所-其它",50);
        categoryGps.put("场馆会所-图书馆",50);
        categoryGps.put("地名-大门",50);
        categoryGps.put("地名-楼号",50);
        categoryGps.put("房地产-大门",50);
        categoryGps.put("公司企业-大门",50);
        categoryGps.put("公司企业-公司",50);
        categoryGps.put("公司企业-楼号",50);
        categoryGps.put("公司企业-其它",50);
        categoryGps.put("公司企业-知名公司",50);
        categoryGps.put("购物场所-大门",50);
        categoryGps.put("购物场所-大型超市",50);
        categoryGps.put("购物场所-其它",50);
        categoryGps.put("购物场所-一般商场",50);
        categoryGps.put("购物场所-专卖店",50);
        categoryGps.put("交通出行-大门",50);
        categoryGps.put("交通出行-立交桥",50);
        categoryGps.put("旅游景点-大门",50);
        categoryGps.put("其它-大门",50);
        categoryGps.put("其它-积水点",50);
        categoryGps.put("其它-楼号",50);
        categoryGps.put("其它-其它",50);
        categoryGps.put("汽车服务-4S店",50);
        categoryGps.put("汽车服务-加气站",50);
        categoryGps.put("汽车服务-加油站",50);
        categoryGps.put("汽车服务-其它",50);
        categoryGps.put("汽车服务-专卖店",50);
        categoryGps.put("体育场馆-楼号",50);
        categoryGps.put("体育场馆-其它",50);
        categoryGps.put("体育场馆-游泳馆",50);
        categoryGps.put("新闻媒体-广播",50);
        categoryGps.put("新闻媒体-其它",50);
        categoryGps.put("新闻媒体-艺术团体",50);
        categoryGps.put("休闲娱乐-KTV",50);
        categoryGps.put("休闲娱乐-歌舞厅",50);
        categoryGps.put("休闲娱乐-楼号",50);
        categoryGps.put("休闲娱乐-洗浴中心",50);
        categoryGps.put("休闲娱乐-夜总会",50);
        categoryGps.put("休闲娱乐-影剧院",50);
        categoryGps.put("休闲娱乐-娱乐城",50);
        categoryGps.put("学校科研-科研院所",50);
        categoryGps.put("学校科研-楼号",50);
        categoryGps.put("学校科研-其它",50);
        categoryGps.put("学校科研-研究生院",50);
        categoryGps.put("学校科研-一般小学",50);
        categoryGps.put("学校科研-一般中学",50);
        categoryGps.put("学校科研-知名小学",50);
        categoryGps.put("医疗卫生-一般医院",50);
        categoryGps.put("医疗卫生-一级医院",50);
        categoryGps.put("政府机关-事业单位",50);
        categoryGps.put("政府机关-政府机关",50);
        categoryGps.put("政府机关-主要政府机关",50);

        categoryGps.put("宾馆饭店-大门",30);
        categoryGps.put("餐饮服务-茶馆",30);
        categoryGps.put("餐饮服务-大门",30);
        categoryGps.put("餐饮服务-酒吧",30);
        categoryGps.put("餐饮服务-咖啡馆",30);
        categoryGps.put("餐饮服务-快餐小吃",30);
        categoryGps.put("餐饮服务-冷饮",30);
        categoryGps.put("餐饮服务-面包甜点",30);
        categoryGps.put("餐饮服务-其它",30);
        categoryGps.put("餐饮服务-一般西餐",30);
        categoryGps.put("餐饮服务-一般中餐",30);
        categoryGps.put("餐饮服务-一般综合",30);
        categoryGps.put("餐饮服务-异国风味",30);
        categoryGps.put("餐饮服务-知名中餐",30);
        categoryGps.put("餐饮服务-知名综合",30);
        categoryGps.put("购物场所-礼品店",30);
        categoryGps.put("购物场所-书店",30);
        categoryGps.put("购物场所-一般超市",30);
        categoryGps.put("交通出行-码头",30);
        categoryGps.put("交通出行-其它",30);
        categoryGps.put("交通出行-桥梁",30);
        categoryGps.put("金融银行-分理处和储蓄所",30);
        categoryGps.put("金融银行-银行",30);
        categoryGps.put("金融银行-证券",30);
        categoryGps.put("金融银行-支行",30);
        categoryGps.put("金融银行-总部",30);
        categoryGps.put("新闻媒体-报社",30);
        categoryGps.put("新闻媒体-出版社",30);
        categoryGps.put("学校科研-一般幼儿园",30);
        categoryGps.put("学校科研-知名幼儿园",30);
        categoryGps.put("学校科研-知名中学",30);
        categoryGps.put("学校科研-中专",30);
        categoryGps.put("医疗卫生-防疫站",30);
        categoryGps.put("医疗卫生-楼号",30);
        categoryGps.put("医疗卫生-其它",30);
        categoryGps.put("邮政电信-邮局",30);

        categoryGps.put("公司企业-火车票代售处",20);
        categoryGps.put("购物场所-鲜花店",20);
        categoryGps.put("购物场所-眼镜店",20);
        categoryGps.put("购物场所-音像店",20);
        categoryGps.put("交通出行-地铁站出入口",20);
        categoryGps.put("交通出行-服务区",20);
        categoryGps.put("交通出行-高速公路出口",20);
        categoryGps.put("交通出行-高速公路入口",20);
        categoryGps.put("交通出行-公交车站",20);
        categoryGps.put("交通出行-红绿灯",20);
        categoryGps.put("交通出行-落客区",20);
        categoryGps.put("交通出行-收费站",20);
        categoryGps.put("金融银行-基金",20);
        categoryGps.put("金融银行-楼号",20);
        categoryGps.put("金融银行-门址",20);
        categoryGps.put("金融银行-期货",20);
        categoryGps.put("金融银行-其它",20);
        categoryGps.put("金融银行-信托",20);
        categoryGps.put("金融银行-资产管理",20);
        categoryGps.put("金融银行-租赁",20);
        categoryGps.put("汽车服务-充电桩",20);
        categoryGps.put("汽车服务-大门",20);
        categoryGps.put("体育场馆-大门",20);
        categoryGps.put("新闻媒体-大门",20);
        categoryGps.put("新闻媒体-杂志社",20);
        categoryGps.put("休闲娱乐-大门",20);
        categoryGps.put("休闲娱乐-其它",20);
        categoryGps.put("休闲娱乐-网吧",20);
        categoryGps.put("学校科研-大门",20);
        categoryGps.put("医疗卫生-宠物医院",20);
        categoryGps.put("医疗卫生-大门",20);
        categoryGps.put("医疗卫生-药店",20);
        categoryGps.put("医疗卫生-诊所",20);
        categoryGps.put("邮政电信-大门",20);
        categoryGps.put("邮政电信-电信",20);
        categoryGps.put("邮政电信-联通",20);
        categoryGps.put("邮政电信-其它",20);
        categoryGps.put("邮政电信-铁通",20);
        categoryGps.put("邮政电信-移动",20);
        categoryGps.put("政府机关-大门",20);
        categoryGps.put("政府机关-楼号",20);
        categoryGps.put("政府机关-其它",20);

        categoryGps.put("金融银行-保险",10);
        categoryGps.put("金融银行-大门",10);

        categoryGps.put("购物场所-楼号",5);
        categoryGps.put("金融银行-ATM",5);

        return categoryGps;

    }



    public static Map<String, String> getSpecialCity() {

        Map<String, String> specialCity = Maps.newHashMap();
        specialCity.put("抚州市", "fuzhoushi1");
        specialCity.put("宿州市", "suzhoushi1");
        specialCity.put("台州市", "taizhoushi1");
        specialCity.put("宜春市", "yichunshi1");
        specialCity.put("玉林市", "yulinshi1");
        specialCity.put("吕梁市", "lvliangshi");

        return specialCity;
    }

}
