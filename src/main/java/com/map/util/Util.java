/**
 * Copyright (C), 2010-2015, Beijing Sogo Co., Ltd.
 *
 * @Title: Util.java
 * @Package: com.sogou.map.hadoop.util
 * @author: huajin.shen
 * @date: 2015年7月17日 上午9:30:02
 * @version: v1.0
 */
package com.map.util;

import com.map.main.ClusterCenterMain;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: Util
 * @author: taoyongbo
 * @date: 2015年7月20日 上午9:30:02
 * @version: 1.0
 */
public class Util {

    protected static final Logger logger = LoggerFactory.getLogger(Util.class);

    public static String getGBKString(Text text)
            throws UnsupportedEncodingException {
        return new String(text.getBytes(), 0, text.getLength(), "GB18030");
    }

    public static String formatBrackets(String str) {
        if (str == null || str.trim().equals("")) return str;
        return str.replace("（", "(").replace("）", ")");
    }


    public static boolean match(String pattern, String text) {
        List<Integer> matches = new ArrayList<Integer>();
        int m = text.length();
        int n = pattern.length();
        Map<Character, Integer> rightMostIndexes = preprocessForBadCharacterShift(pattern);
        int alignedAt = 0;
        while (alignedAt + (n - 1) < m) {
            for (int indexInPattern = n - 1; indexInPattern >= 0; indexInPattern--) {
                int indexInText = alignedAt + indexInPattern;
                char x = text.charAt(indexInText);
                char y = pattern.charAt(indexInPattern);
                if (indexInText >= m) break;
                if (x != y) {
                    Integer r = rightMostIndexes.get(x);
                    if (r == null) {
                        alignedAt = indexInText + 1;
                    } else {
                        int shift = indexInText - (alignedAt + r);
                        alignedAt += shift > 0 ? shift : 1;
                    }
                    break;
                } else if (indexInPattern == 0) {
                    matches.add(alignedAt);
                    alignedAt++;
                }
            }
        }


        if (matches.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }


    private static Map<Character, Integer> preprocessForBadCharacterShift(
            String pattern) {
        Map<Character, Integer> map = new HashMap<Character, Integer>();
        for (int i = pattern.length() - 1; i >= 0; i--) {
            char c = pattern.charAt(i);
            if (!map.containsKey(c)) map.put(c, i);
        }
        return map;
    }


    /**
     * 得到一个目录(不包括子目录)下的所有名字匹配上pattern的文件名
     *
     * @param fs
     * @param folderPath
     * @param pattern    用于匹配文件名的正则
     * @return
     * @throws IOException
     */
    public static List<Path> getFilesUnderFolder(FileSystem fs, Path folderPath, String pattern) throws IOException {
        List<Path> paths = new ArrayList<Path>();
        if (fs.exists(folderPath)) {
            FileStatus[] fileStatuses = fs.listStatus(folderPath);


            if (fileStatuses.length > 0) {
                for (FileStatus f : fileStatuses) {
                    try {
                         showDir(fs,f,paths);
//                        logger.info("fileStatus.length:" + filePath.toString());
//                        paths.add(filePath);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        logger.info("路径length:"+paths.size());

        return paths;
    }


    private static void showDir(FileSystem fileSystem,FileStatus fs,List<Path> paths) throws Exception {
        Path path = fs.getPath();
        // 如果是目录
        if (fs.isDir()) {
            FileStatus[] f = fileSystem.listStatus(path);
            if (f.length > 0) {
                for (FileStatus fileStatus : f) {
                    showDir(fileSystem,fileStatus,paths);
                }
            }
        }else {

            String pathStr = path.toString();
            if(!pathStr.contains("_SUCCESS")){
                paths.add(path);
            }

        }
    }


}
