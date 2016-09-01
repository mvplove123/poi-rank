package com.map.model;

import java.util.Map;

/**
 * Created by admin on 2016/6/14.
 */
public class FeatureRank {


    /**
     * 分类值
     */
    private String categoryValue;

    /**
     * 标签值
     */
    private String tagValue;

    /**
     * 区间范围值
     */
    private Map<String,Rank> scopeValue;

    public String getCategoryValue() {
        return categoryValue;
    }

    public void setCategoryValue(String categoryValue) {
        this.categoryValue = categoryValue;
    }

    public String getTagValue() {
        return tagValue;
    }

    public void setTagValue(String tagValue) {
        this.tagValue = tagValue;
    }

    public Map<String, Rank> getScopeValue() {
        return scopeValue;
    }

    public void setScopeValue(Map<String, Rank> scopeValue) {
        this.scopeValue = scopeValue;
    }
}
