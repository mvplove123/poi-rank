package com.map.util;

/**
 * Created by admin on 2016/1/12.
 */
public enum SimilarityEnum {
    IDENTICAL, SUFFIX, PREFIX, INFIX, ONENULL, BOTHNULL, SIMILAR, DIFFERENT, NULL;

    public boolean in(SimilarityEnum ...sims){

        for(SimilarityEnum sim : sims){
            if(sim == this){
                return true;
            }
        }
        return false;
    }

    public boolean isSubstring(){
        return this.in(SUFFIX,PREFIX,INFIX);
    }

    public boolean hasNull(){
        return this.in(ONENULL,BOTHNULL);
    }

    public boolean isSimilar(){
        return this.in(IDENTICAL,SIMILAR);
    }

    public boolean isNoDifference(){
        return this.in(IDENTICAL,BOTHNULL);
    }
}
