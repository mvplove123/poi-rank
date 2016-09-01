package com.map.util;

import org.apache.commons.lang3.StringUtils;

public class CharNormalizer {

    public static int baseHalf(int ch) {

        switch (ch) {
        case '“':
            return '"';
        case '”':
            return '"';
        case '‘':
            return '\'';
        case '’':
            return '\'';
        case '。':
            return '.';
        case '，':
            return ',';
        case '；':
            return ';';
        case '：':
            return ':';
        case '？':
            return '?';
        case '！':
            return '!';
        case '—':
            return '-';
        case '～':
            return '~';
        case '（':
            return '(';
        case '）':
            return ')';
        case '《':
            return '<';
        case '》':
            return '>';
        }

        if (Character.isWhitespace(ch))
            return ' ';
        if (ch > '\uFF00' && ch < '\uFF5F')
            return ch - 65248;

        return ch;
    }


    public static String char2String(String str){


        if(StringUtils.isEmpty(str)){
            return "";
        }

        char[] k = new char[str.length()];

        for(int i=0 ; i< str.length();i++){
            k[i] = (char)CharNormalizer.baseHalf(str.charAt(i));
        }

        return String.valueOf(k);

    }


    public static void main(String[] args) {
        System.out.println((char) baseHalf('今'));
//        for (int i = '\uFF00' + 1, n = '\uFF5F'; i < n; i++) {
//            System.out.println(((char) i) + ":" + (char) (i - 65248));
//        }
    }

}
