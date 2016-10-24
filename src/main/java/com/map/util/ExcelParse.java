package com.map.util;

import com.map.model.FeatureThreshold;
import org.apache.commons.lang3.StringUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

/**
 * Created by admin on 2016/6/13.
 */
public class ExcelParse {


    public static void main(String[] args) {

        try {
            List<FeatureThreshold> result = ExcelHandler.readExcel("/search/odin/taoyongbo/rank/input/poi-rank.xlsx");
            OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream
                    ("/search/odin/taoyongbo/rank/result/poi-threshold.txt",
                            false), "GB18030");

            OutputStreamWriter writer1 = new OutputStreamWriter(new FileOutputStream
                    ("/search/odin/taoyongbo/rank/result/poi-weight.txt",
                            false), "GB18030");

            for (FeatureThreshold featureThreshold : result) {

                if (StringUtils.isBlank(featureThreshold.getSubCategory())) {
                    writer1.write(featureThreshold.toString() + "\n");
                } else {
                    writer.write(featureThreshold.toStringRank() + "\n");
                }
            }

            writer.flush();
            writer1.flush();
            writer.close();
            writer1.close();
            System.out.println("parse excel finished");

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
