package com.map.model;

import org.apache.hadoop.thirdparty.guava.common.base.Joiner;

/**
 * Created by admin on 2016/6/13.
 * 特征值阀值
 */
public class FeatureThreshold {


    private String parentCategory;

    private String subCategory;

    private String category; // 大类打分

    private String tag;

    private String matchCount;

    private String grade;

    private String comment;

    private String price;


    private String area;

    private String leafCount;

    private String doorCount;

    private String parkCount;

    private String innerCount;

    private String ticketCount;

    public String getParentCategory() {
        return parentCategory;
    }

    public void setParentCategory(String parentCategory) {
        this.parentCategory = parentCategory;
    }

    public String getSubCategory() {
        return subCategory;
    }

    public void setSubCategory(String subCategory) {
        this.subCategory = subCategory;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getMatchCount() {
        return matchCount;
    }

    public void setMatchCount(String matchCount) {
        this.matchCount = matchCount;
    }

    public String getGrade() {
        return grade;
    }

    public void setGrade(String grade) {
        this.grade = grade;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getLeafCount() {
        return leafCount;
    }

    public void setLeafCount(String leafCount) {
        this.leafCount = leafCount;
    }

    public String getDoorCount() {
        return doorCount;
    }

    public void setDoorCount(String doorCount) {
        this.doorCount = doorCount;
    }

    public String getParkCount() {
        return parkCount;
    }

    public void setParkCount(String parkCount) {
        this.parkCount = parkCount;
    }

    public String getInnerCount() {
        return innerCount;
    }

    public void setInnerCount(String innerCount) {
        this.innerCount = innerCount;
    }

    public String getTicketCount() {
        return ticketCount;
    }

    public void setTicketCount(String ticketCount) {
        this.ticketCount = ticketCount;
    }


    @Override//weight 权重
    public String toString() {


        Joiner joiner = Joiner.on("\t");
        String featureStr = joiner.join(new String[]{parentCategory,subCategory,category,
                tag,matchCount,grade,comment, price,
                area,leafCount, doorCount, parkCount
                ,innerCount,ticketCount});

        return featureStr;
    }

    //特征阀值
    public String toStringRank() {

        Joiner joiner = Joiner.on("\t");
        String newCategory=category;
        if(category.contains(".")){
            newCategory=category.substring(0,category.indexOf("."));
        }

        String featureStr = joiner.join(new String[]{parentCategory,subCategory,newCategory,
                tag,matchCount,grade,comment, price,
                area,leafCount, doorCount, parkCount
                ,innerCount,ticketCount});

        return featureStr;
    }

}
