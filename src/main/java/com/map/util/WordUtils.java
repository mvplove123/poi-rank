package com.map.util;

import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by admin on 2016/1/12.
 */
public class WordUtils {


    private static final Pattern alphabeticNumericPat = Pattern.compile("[a-zA-Z0-9]{1,}");

    private static Comparator<String> lenthDescComparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            if (o1.length() > o2.length()) {
                return -1;
            } else if (o1.length() < o2.length()) {
                return 1;
            }
            return 0;
        }
    };
    /**
     * 将字符串中的全角符号转换为半角符号，
     * 包括将中文空格转换为英文空格，中文横线转换成英文连字符
     *
     * @param value
     *            要转换的字串
     * @return 转换后的字串
     */
    public static String convertFullWidth2HalfWidth(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() == 0) {
            return "";
        }

        char c[] = value.toCharArray();
        for (int i = 0; i < c.length; i++) {
            if ((c[i] == '\u3000') || (c[i] == '\u00A0')) {
                c[i] = ' ';
            } else if ((c[i] == '\u2014')
                    || (c[i] == '\u2015')
                    || (c[i] == '•')
                    || (c[i] == '・')
                    ) {
                c[i] = '-';
            } else if (c[i] > '\uFF00' && c[i] < '\uFF5F') {
                c[i] = (char) (c[i] - 65248);
            }
        }

        return new String(c);
    }

    /**
     * 将字符串中的中文空格转换为英文空格
     *
     * @param value
     *            要转换的字串
     * @return 转换后的字串
     */
    public static String convertChineseSpace2Space(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() == 0) {
            return "";
        }

        char c[] = value.toCharArray();
        for (int i = 0; i < c.length; i++) {
            if ((c[i] == '\u3000') || (c[i] == '\u00A0')) {
                c[i] = ' ';
            }
        }
        return new String(c);
    }

    /**
     * 去掉字串开始和结尾的空白字符，包括java的whiteSpace、\u00A0和全角空格等
     *
     * @param value
     *            要处理的值
     * @return 处理后的字串
     */
    public static String trimWhiteSpace(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() == 0) {
            return "";
        }

        char c[] = value.toCharArray();
        int len = c.length;
        int start = 0;
        for (int i = 0; i < len; i++) {
            char ch = c[i];
            if ((!Character.isWhitespace(ch)) && (ch != '\u00A0')
                    && (ch != '\u3000')) {
                start = i;
                break;
            }
        }
        int end = len - 1;
        for (int i = len - 1; i >= 0; i--) {
            char ch = c[i];
            if ((!Character.isWhitespace(ch)) && (ch != '\u00A0')
                    && (ch != '\u3000')) {
                end = i;
                break;
            }
        }

        return new String(c, start, end - start + 1);
    }


    /**
     * 将中文的"零一二三..."等数字转换成"0123..."
     *
     * @param value
     *            要转换的字串
     * @return 转换后的字串
     */
    public static String convertUppercaseNumber2Number(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() == 0) {
            return "";
        }

        return StringUtils.replaceEach(value, new String[] {
                        "零", "一", "二", "三", "四", "五", "六", "七", "八", "九" },
                new String[] { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9" });
    }

    /**
     * 将中文大写的"零壹贰叁肆伍陆柒捌玖拾..."等数字转换成"0123..."
     *
     * @param value
     *            要转换的字串
     * @return 转换后的字串
     */
    public static String convertChineseUppercaseNumber2Number(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() == 0) {
            return "";
        }

        return StringUtils.replaceEach(value, new String[]{
                        "零", "壹", "贰", "叁", "肆", "伍", "陆", "柒", "捌", "玖"},
                new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"});
    }

    /**
     * 将取值中连续的空白字符转换为一个英文空格
     *
     * @param value
     *            要处理的值
     * @return 转换后的值
     */
    public static String convertWhiteSpace(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder(value.length());

        char[] c = value.toCharArray();
        boolean gotWhiteSpace = false;
        for (char ch : c) {
            if (Character.isWhitespace(ch)) {
                if (gotWhiteSpace) {
                    /* do nothing */
                } else {
                    gotWhiteSpace = true;
                    sb.append(' ');
                }
            } else {
                gotWhiteSpace = false;
                sb.append(ch);
            }
        }

        return sb.toString();
    }

    /**
     * 判断一个字符是否为中文字符（不含标点符号）
     *
     * @param ch
     *            要判断的字符
     * @return 如果为中文字符，返回true
     */
    public static boolean isChineseChar(char ch) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(ch);
        return ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A;
    }

    public static boolean isEnglish(char ch){
        return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z');
    }

    /**
     * 判断一个字符是否为英文标点或中文标点
     *
     * @param ch
     *            要判断的字符
     * @return 如果为标点，返回true
     */
    public static boolean isPunctuationChar(char ch) {
        if ((ch > '\u0020') && (ch <= '\u002F')) {
            return true;
        }
        if ((ch >= '\u003A') && (ch <= '\u0040')) {
            return true;
        }
        if ((ch >= '\u005B') && (ch <= '\u0060')) {
            return true;
        }
        if ((ch >= '\u007B') && (ch <= '\u007E')) {
            return true;
        }
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(ch);
        return ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
                || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS;
    }

    /**
     * 获取字符串相似性
     *
     * @param str1
     * @param str2
     * @return
     */
    public static SimilarityEnum getSimilarity(String str1, String str2) {
        if (StringUtils.isBlank(str1) && StringUtils.isBlank(str2)) {
            return SimilarityEnum.BOTHNULL; // both null
        }
        if (StringUtils.isBlank(str1) || StringUtils.isBlank(str2)) {
            return SimilarityEnum.ONENULL; //one null
        }
        if (str1.equals(str2)) {
            return SimilarityEnum.IDENTICAL; // identical
        }
        if (str1.endsWith(str2) || str2.endsWith(str1)) {
            return SimilarityEnum.SUFFIX; // is suffix
        }
        if (str1.startsWith(str2) || str2.startsWith(str1)) {
            return SimilarityEnum.PREFIX; // is prefix
        }
        if (str1.indexOf(str2) != -1 || str2.indexOf(str1) != -1) {
            return SimilarityEnum.INFIX; // is substring
        }
        return SimilarityEnum.DIFFERENT;
    }



    /**
     * 获取字符串长度差
     *
     * @param str1
     * @param str2
     * @return
     */
    public static int getLengthDiff(String str1, String str2) {
        if (str1 == null || str2 == null) {
            throw new IllegalArgumentException("parameters should not be null");
        }
        return Math.abs(str1.length() - str2.length());
    }


    /**
     * 全不为空
     *
     * @param strings
     * @return
     */
    public static boolean noneBlank(String... strings) {
        for (String string : strings) {
            if (StringUtils.isBlank(string)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 不全为空
     *
     * @param strings
     * @return
     */
    public static boolean notAllBlank(String... strings) {
        for (String string : strings) {
            if (!StringUtils.isBlank(string)) {
                return true;
            }
        }
        return false;
    }


    /**
     * 获取两个字符串的最长公共子串
     *
     * @param str1
     * @param str2
     * @return
     */
    public static List<String> lcs(String str1, String str2) {
        List<String> resultList = new ArrayList<String>();

        int maxLength = 0;
        int[][] table = new int[str1.length()][str2.length()];

        for (int i = 0; i < str1.length(); i++) {
            for (int j = 0; j < str2.length(); j++) {
                if (str1.charAt(i) == str2.charAt(j)) {
                    if (i == 0 || j == 0) {
                        table[i][j] = 1;
                    } else {
                        table[i][j] = table[i - 1][j - 1] + 1;
                    }

                    // Found longer substring. Clear the resultList. Will be adding the substring in following section.
                    if (table[i][j] > maxLength) {
                        maxLength = table[i][j];
                        resultList.clear();
                    }

                    // Found another substring witch has length of "maxLength"
                    if (table[i][j] == maxLength) {
                        resultList.add(str1.substring(i - maxLength + 1, i + 1));
                    }
                }
            }
        }

        return resultList;
    }

    /**
     * 获取最长公共子串长度
     *
     * @param str1
     * @param str2
     * @return
     */
    public static int getLcsLength(String str1, String str2) {
        List<String> list = lcs(str1, str2);
        if (list.size() == 0) {
            return 0;
        }
        return list.get(0).length();
    }

    /**
     * 找出所有公共子串
     *
     * @param str1
     * @param str2
     * @return
     */
    public static Set<String> getCommonSubstrings(String str1, String str2) {
        return getCommonSubstrings(str1, str2, 1);
    }


    /**
     *
     * @param strs
     * @param descending
     * @return
     */
    public static List<String> sortByLength(Collection<String> strs, boolean descending) {
        LinkedList<String> list = new LinkedList<String>(strs);
        Collections.sort(list, lenthDescComparator);
        if (!descending) {
            Collections.reverse(list);
        }
        return list;
    }
    /**
     * 找出所有的公共子串(length >= threshold)
     *
     * @param str1
     * @param str2
     * @param threshold minimal substring length (included)
     * @return
     */
    public static Set<String> getCommonSubstrings(String str1, String str2, int threshold) {
        Set<String> set = new HashSet<String>();

        int[][] table = new int[str1.length()][str2.length()];

        for (int i = 0; i < str1.length(); i++) {
            for (int j = 0; j < str2.length(); j++) {
                if (str1.charAt(i) == str2.charAt(j)) {
                    if (i == 0 || j == 0) {
                        table[i][j] = 1;
                    } else {
                        table[i][j] = table[i - 1][j - 1] + 1;

                        if (i == str1.length() - 1 || j == str2.length() - 1) {
                            if (table[i][j] >= threshold) {
                                set.add(str1.substring(i - table[i][j] + 1, i + 1));
                            }
                        }
                    }
                } else if (i >= threshold && j >= threshold) {
                    int len = table[i - 1][j - 1];
                    if (len >= threshold) {
                        set.add(str1.substring(i - len, i));
                    }
                }
            }
        }
        return set;
    }

//    /**
//     * 找出所有的公共子串(pre-tokenized)
//     *
//     * @param str1
//     * @param str2
//     * @param threshold
//     * @return
//     */
//    public static Set<String> getCommonTokenizedSubstrings(String str1, String str2, int threshold) {
//        if (!noneBlank(str1, str2)) {
//            return Collections.emptySet();
//        }
//        List<Token> tokens1 = tokenizer.tokenize(str1);
//        List<Token> tokens2 = tokenizer.tokenize(str2);
//        Set<String> set = new HashSet<String>();
//
//        int[][] table = new int[tokens1.size()][tokens2.size()];
//
//        for (int i = 0; i < tokens1.size(); i++) {
//            for (int j = 0; j < tokens2.size(); j++) {
//                if (tokens1.get(i).getValue().equals(tokens2.get(j).getValue())) {
//                    if (i == 0 || j == 0) {
//                        table[i][j] = 1;
//                    } else {
//                        table[i][j] = table[i - 1][j - 1] + 1;
//
//                        if (i == tokens1.size() - 1 || j == tokens2.size() - 1) {
//                            if (table[i][j] >= threshold) {
//                                StringBuilder sb = new StringBuilder();
//                                for (int a = i - table[i][j] + 1; a < i + 1; a++) {
//                                    sb.append(tokens1.get(a).getValue());
//                                }
//                                set.add(sb.toString());
//                            }
//                        }
//                    }
//                } else if (i >= threshold && j >= threshold) {
//                    int len = table[i - 1][j - 1];
//                    if (len >= threshold) {
//                        StringBuilder sb = new StringBuilder();
//                        for (int a = i - len; a < i; a++) {
//                            sb.append(tokens1.get(a).getValue());
//                        }
//                        set.add(sb.toString());
//                    }
//                }
//            }
//        }
//        return set;
//    }



//    public static String getCommonTail(String str1, String str2) {
//        if (!noneBlank(str1, str2)) {
//            return "";
//        }
//        List<Token> tokens1 = tokenizer.tokenize(str1);
//        List<Token> tokens2 = tokenizer.tokenize(str2);
//
//        LinkedList<String> stack = new LinkedList<String>();
//        for (int i = 1; i <= Math.min(tokens1.size(), tokens2.size()); i++) {
//            Token t1 = tokens1.get(tokens1.size() - i);
//            Token t2 = tokens2.get(tokens2.size() - i);
//            if (!t1.getValue().equals(t2.getValue())) {
//                break;
//            }
//            stack.push(t1.getValue());
//        }
//        if (stack.size() > 0) {
//            StringBuilder sb = new StringBuilder();
//            while (stack.size() > 0) {
//                sb.append(stack.pop());
//            }
//            return sb.toString();
//        }
//        return "";
//    }



    /**
     * 获取两个字符串不同的部分
     *
     * @param str1
     * @param str2
     * @return
     */
    //FIXME
    public static MutablePair<List<String>, List<String>> getDiffList(String str1, String str2) {
        MutablePair<List<String>, List<String>> pair = new MutablePair<List<String>, List<String>>();
        Set<String> subs = getCommonSubstrings(str1, str2, 2);
        List<String> list = sortByLength(subs, true);
        pair.setLeft(removeWords(str1, list));
        pair.setRight(removeWords(str2, list));
        return pair;
    }

    public static List<String> removeWords(String str, List<String> words) {
        for (String word : words) {
            str = StringUtils.replace(str, word, "\t", 5);
        }
        String[] arr = str.split("\t");

        LinkedList<String> list = new LinkedList<String>();
        if (arr.length > 0) {
            for (String item : arr) {
                if (!StringUtils.isBlank(item)) {
                    list.add(item);
                }
            }
        }
        return list;
    }

    /**
     * get the shorter string
     *
     * @param str1
     * @param str2
     * @return
     */
    public static String getShorterOne(String str1, String str2) {
        return str1.length() < str2.length() ? str1 : str2;
    }

    /**
     * get the longer string
     *
     * @param str1
     * @param str2
     * @return
     */
    public static String getLongerOne(String str1, String str2) {
        return str1.length() > str2.length() ? str1 : str2;
    }


    /**
     * contains alphabetic or number(sequential only)
     *
     * @param str
     * @return
     */
    public static boolean containsSequentialAlphabeticNumeric(String str) {
        if (StringUtils.isBlank(str)) {
            return false;
        }
        return alphabeticNumericPat.matcher(str).find();
    }




    /**
     * 规则化
     * @param str
     */
    public static String normalize(String str) {

        String normalizeStr="";
        if (StringUtils.isEmpty(str)) {
            return "";
        }
        //全角转半角
        normalizeStr = WordUtils.convertFullWidth2HalfWidth(str.toUpperCase());

        //繁体转简体
        normalizeStr = ChineseConverter.traditional2Simplified(normalizeStr);

        //中文字符转英文字符
        normalizeStr = CharNormalizer.char2String(normalizeStr);


        return normalizeStr;

    }




    public static void main(String[] args) {

        String str1 = "《abccadeｊ資料庫";
        String str2 = "dgcadde";



        ChineseConverter.traditional2Simplified(str1);

        String result  =ChineseConverter.traditional2Simplified(convertFullWidth2HalfWidth(str1.toUpperCase()));


        CharNormalizer.char2String(result);


        System.out.println(str1+"转化"+CharNormalizer.char2String(result));



    }



}
