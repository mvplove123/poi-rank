package com.map.model;

/**
 * Created by admin on 2016/4/1.
 */
public class Rank {

    /**
     * 范围区间
     */
    private Integer[] categoryThreshold;

    /**
     * 范围取值
     */
    private Integer[] thresholdValue;


    private Double[] rankThreshold;

    private Integer[] rankThresholdValue;


    public Integer[] getCategoryThreshold() {
        return categoryThreshold;
    }


    public Rank(Integer[] categoryThreshold, Integer[] thresholdValue) {
        this.categoryThreshold = categoryThreshold;
        this.thresholdValue = thresholdValue;
    }

    public Rank(Double[] rankThreshold, Integer[] rankThresholdValue) {
        this.rankThreshold = rankThreshold;
        this.rankThresholdValue = rankThresholdValue;
    }

    public void setCategoryThreshold(Integer[] categoryThreshold) {
        this.categoryThreshold = categoryThreshold;
    }

    public Integer[] getThresholdValue() {
        return thresholdValue;
    }

    public void setThresholdValue(Integer[] thresholdValue) {
        this.thresholdValue = thresholdValue;
    }



    public Double[] getRankThreshold() {
        return rankThreshold;
    }

    public void setRankThreshold(Double[] rankThreshold) {
        this.rankThreshold = rankThreshold;
    }

    public Integer[] getRankThresholdValue() {
        return rankThresholdValue;
    }

    public void setRankThresholdValue(Integer[] rankThresholdValue) {
        this.rankThresholdValue = rankThresholdValue;
    }
}
