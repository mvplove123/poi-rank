/**
 *
 */
package com.map.util;

import com.map.model.FeatureThreshold;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public class ExcelHandler {


    private static  String pCategory="";
    /**
     * read the Excel file
     * @param path the path of the Excel file
     * @return
     * @throws IOException
     */
    public static List<FeatureThreshold> readExcel(String path) throws IOException {
        if (path == null || Constants.EMPTY.equals(path)) {
            return null;
        } else {
            String postfix = getPostfix(path);
            if (!Constants.EMPTY.equals(postfix)) {
                if (Constants.OFFICE_EXCEL_2003_POSTFIX.equals(postfix)) {
                    return readXls(path);
                } else if (Constants.OFFICE_EXCEL_2010_POSTFIX.equals(postfix)) {
                    return readXlsx(path);
                }
            } else {
                System.out.println(path + Constants.NOT_EXCEL_FILE);
            }
        }
        return null;
    }

    /**
     * Read the Excel 2010
     * @param path the path of the excel file
     * @return
     * @throws IOException
     */
    public static List<FeatureThreshold> readXlsx(String path) throws IOException {
        System.out.println(Constants.PROCESSING + path);
        InputStream is = new FileInputStream(path);
        XSSFWorkbook xssfWorkbook = new XSSFWorkbook(is);
        // Read the Sheet
        for (int numSheet = 0; numSheet < xssfWorkbook.getNumberOfSheets(); numSheet++) {
            XSSFSheet xssfSheet = xssfWorkbook.getSheetAt(numSheet);
            if (xssfSheet == null) {
                continue;
            }

            String parentCategory=" ";
            pCategory=parentCategory;

            List<FeatureThreshold> featureThresholdList = Lists.newArrayList();
            // Read the Row
            for (int rowNum = 2; rowNum <= xssfSheet.getLastRowNum(); rowNum++) {
                XSSFRow xssfRow = xssfSheet.getRow(rowNum);
                if (xssfRow != null) {

                    String subCategory=" ";
                    String category=" ";
                    String tag=" ";
                    String matchCount=" ";
                    String grade=" ";
                    String comment=" ";
                    String price=" ";
                    String area=" ";
                    String leafCount=" ";
                    String doorCount=" ";
                    String parkCount=" ";
                    String innerCount=" ";
                    String ticketCount=" ";


                    FeatureThreshold featureThreshold = new FeatureThreshold();
                    XSSFCell xparentCategory = xssfRow.getCell(0);
                    XSSFCell xsubCategory = xssfRow.getCell(1);
                    XSSFCell xcategory = xssfRow.getCell(2);
                    XSSFCell xtag = xssfRow.getCell(3);
                    XSSFCell xmatchCount = xssfRow.getCell(4);
                    XSSFCell xgrade = xssfRow.getCell(5);
                    XSSFCell xcomment = xssfRow.getCell(6);
                    XSSFCell xprice = xssfRow.getCell(7);
                    XSSFCell xarea = xssfRow.getCell(8);
                    XSSFCell xleafCount = xssfRow.getCell(9);
                    XSSFCell xdoorCount = xssfRow.getCell(10);
                    XSSFCell xparkCount = xssfRow.getCell(11);
                    XSSFCell xinnerCount = xssfRow.getCell(12);
                    XSSFCell xticketCount = xssfRow.getCell(13);


                    //获取权重行
                    if(xsubCategory !=null && StringUtils.isBlank(getValue(xsubCategory))){
                        parentCategory = getValue(xparentCategory);
                        subCategory=getValue(xsubCategory);
                        category=getValue(xcategory);
                        tag=getValue(xtag);
                        matchCount=getValue(xmatchCount);
                        grade=getValue(xgrade);
                        comment=getValue(xcomment);
                        price=getValue(xprice);
                        area=getValue(xarea);
                        leafCount=getValue(xleafCount);
                        doorCount=getValue(xdoorCount);
                        parkCount=getValue(xparkCount);
                        innerCount=getValue(xinnerCount);
                        ticketCount=getValue(xticketCount);
                    }else{

                        //获取具体值
                        if(xparentCategory !=null && StringUtils.isNotBlank(getValue(xparentCategory))){
                            parentCategory = getValue(xparentCategory);
                        }
                        if(xsubCategory !=null && StringUtils.isNotBlank(getValue(xsubCategory))){
                            subCategory=getValue(xsubCategory);
                        }
                        if(xcategory !=null && StringUtils.isNotBlank(getValue(xcategory))){
                            category=getValue(xcategory);
                        }
                        if(xtag !=null && StringUtils.isNotBlank(getValue(xtag))){
                            tag=getValue(xtag);
                        }
                        if(xmatchCount !=null && StringUtils.isNotBlank(getValue(xmatchCount))){

                            matchCount=getFormatValue(getValue(xmatchCount));
                        }
                        if(xgrade !=null && StringUtils.isNotBlank(getValue(xgrade))){
                            grade=getFormatValue(getValue(xgrade));
                        }
                        if(xcomment !=null && StringUtils.isNotBlank(getValue(xcomment))){
                            comment=getFormatValue(getValue(xcomment));
                        }
                        if(xprice !=null && StringUtils.isNotBlank(getValue(xprice))){
                            price=getFormatValue(getValue(xprice));
                        }
                        if(xarea !=null && StringUtils.isNotBlank(getValue(xarea))){
                            area=getFormatValue(getValue(xarea));
                        }
                        if(xleafCount !=null && StringUtils.isNotBlank(getValue(xleafCount))){
                            leafCount=getFormatValue(getValue(xleafCount));
                        }
                        if(xdoorCount !=null && StringUtils.isNotBlank(getValue(xdoorCount))){
                            doorCount=getFormatValue(getValue(xdoorCount));
                        }
                        if(xparkCount !=null && StringUtils.isNotBlank(getValue(xparkCount))){
                            parkCount=getFormatValue(getValue(xparkCount));
                        }
                        if(xinnerCount !=null && StringUtils.isNotBlank(getValue(xinnerCount))){
                            innerCount=getFormatValue(getValue(xinnerCount));
                        }
                        if(xticketCount !=null && StringUtils.isNotBlank(getValue(xticketCount))){
                            ticketCount=getFormatValue(getValue(xticketCount));
                        }


                    }

                    featureThreshold.setParentCategory(parentCategory);
                    featureThreshold.setSubCategory(subCategory);
                    featureThreshold.setCategory(category);
                    featureThreshold.setArea(area);
                    featureThreshold.setComment(comment);
                    featureThreshold.setDoorCount(doorCount);
                    featureThreshold.setGrade(grade);
                    featureThreshold.setInnerCount(innerCount);
                    featureThreshold.setLeafCount(leafCount);
                    featureThreshold.setMatchCount(matchCount);
                    featureThreshold.setParkCount(parkCount);
                    featureThreshold.setTag(tag);
                    featureThreshold.setTicketCount(ticketCount);
                    featureThreshold.setPrice(price);

                    featureThresholdList.add(featureThreshold);



                }
            }
            return featureThresholdList;
        }
        return null;
    }

    /**
     * Read the Excel 2003-2007
     * @param path the path of the Excel
     * @return
     * @throws IOException
     */
    public static List<FeatureThreshold> readXls(String path) throws IOException {
        System.out.println(Constants.PROCESSING + path);
        InputStream is = new FileInputStream(path);
        HSSFWorkbook hssfWorkbook = new HSSFWorkbook(is);
        // Read the Sheet
        for (int numSheet = 0; numSheet < hssfWorkbook.getNumberOfSheets(); numSheet++) {
            HSSFSheet hssfSheet = hssfWorkbook.getSheetAt(numSheet);
            if (hssfSheet == null) {
                continue;
            }
            // Read the Row
            for (int rowNum = 1; rowNum <= hssfSheet.getLastRowNum(); rowNum++) {
                HSSFRow hssfRow = hssfSheet.getRow(rowNum);
                if (hssfRow != null) {
//                    student = new Student();
//                    HSSFCell no = hssfRow.getCell(0);
//                    HSSFCell name = hssfRow.getCell(1);
//                    HSSFCell age = hssfRow.getCell(2);
//                    HSSFCell score = hssfRow.getCell(3);
//                    student.setNo(getValue(no));
//                    student.setName(getValue(name));
//                    student.setAge(getValue(age));
//                    student.setScore(Float.valueOf(getValue(score)));
//                    list.add(student);
                }
            }
        }
        return null;
    }


    private static String getFormatValue(String value){

        if(StringUtils.isBlank(value)){
            return null;
        }
        TreeSet<String> level = new TreeSet(new MyComparator());
        TreeSet<String> score = new TreeSet(new MyComparator());
        String[] a = value.trim().split("\\|");
        for(String str : a){
            String[] b = str.split(":");
            String b0=b[0];
            String b1=b[1];
                String[] c = b0.split("-");
                for(String str2 : c){
                    level.add(str2);
                }
            score.add(b1);
        }

        Joiner joiner = Joiner.on(",");




        String levelstr = joiner.join(level);
        score.add("0");

        String scorestr = joiner.join(score);
        String result = levelstr+"|"+scorestr;

        if(level.size()>5 || score.size()>6 || level.size() == score.size()){
            System.out.println(pCategory +result);
        }

        return result;

    }


    public static class MyComparator implements Comparator<String> {

        public int compare(String f1,String f2) {

            if(!NumberUtils.isNumber(f1) || !NumberUtils.isNumber(f2)){
                return -1;
            }


            if (Integer.valueOf(f1) > Integer.valueOf(f2))
            {
                return 1;
            }
            else if (Integer.valueOf(f1).equals(Integer.valueOf(f2)) )
            {
                return 0;
            }
            else
            {
                return -1;
            }
        }
    }



    @SuppressWarnings("static-access")
    private static String getValue(XSSFCell xssfRow) {
        if (xssfRow.getCellType() == xssfRow.CELL_TYPE_BOOLEAN) {
            return String.valueOf(xssfRow.getBooleanCellValue());
        } else if (xssfRow.getCellType() == xssfRow.CELL_TYPE_NUMERIC) {
            return String.valueOf(xssfRow.getNumericCellValue());
        } else {
            return String.valueOf(xssfRow.getStringCellValue());
        }
    }

    @SuppressWarnings("static-access")
    private static String getValue(HSSFCell hssfCell) {
        if (hssfCell.getCellType() == hssfCell.CELL_TYPE_BOOLEAN) {
            return String.valueOf(hssfCell.getBooleanCellValue());
        } else if (hssfCell.getCellType() == hssfCell.CELL_TYPE_NUMERIC) {
            return String.valueOf(hssfCell.getNumericCellValue());
        } else {
            return String.valueOf(hssfCell.getStringCellValue());
        }
    }


    /**
     * get postfix of the path
     * @param path
     * @return
     */
    public static String getPostfix(String path) {
        if (path == null || Constants.EMPTY.equals(path.trim())) {
            return Constants.EMPTY;
        }
        if (path.contains(Constants.POINT)) {
            return path.substring(path.lastIndexOf(Constants.POINT) + 1, path.length());
        }
        return Constants.EMPTY;
    }


//    public static void string2Csv(List<String[]> strList,String path){
//        try{
//
//            OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(path), Charset.forName("GBK"));
//            CSVWriter csvWriter = new CSVWriter(out, ',');
//            csvWriter.writeAll(strList);
//            csvWriter.close();
//        }catch(IOException e){
//            System.out.println(e);
//        }
//    }






}