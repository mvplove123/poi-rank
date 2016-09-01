package com.map.main;

import java.math.BigDecimal;

/**
 * Created by admin on 2016/3/14.
 */
public class Test {

    public static void main(String[] args) {





            String str1 = "1.2866499597E7";
            BigDecimal bigDecimal = new BigDecimal(str1);

            double va =Double.valueOf(str1);
            String str2 = bigDecimal.toString();

            System.out.println(str2);



    }
}
