/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.map.model;

//import data.BoundingBox;

import java.util.ArrayList;
import java.util.List;

public class CellCut {
    double DIAMETER;//比例尺，设置只是对map的个数有影响
    double EXPAND; //与当前格子相临N米，距离半径

    public CellCut(double DIAMETER,double EXPAND)
    {
        this.DIAMETER=DIAMETER;
        this.EXPAND=EXPAND;
    }

    public CellCut()
    {
        this.DIAMETER=1000.0;
        this.EXPAND=500.0;
    }


    ////计算当前点所在的格子编号
    public String getCurrentCell(String sx,String sy)
    {
        double x=Double.parseDouble(sx);
        double y=Double.parseDouble(sy);
        x=x/DIAMETER;
        int ax=(int)x;

        y=y/DIAMETER;
        int ay=(int)y;

        return ax+"|"+ay;
    }

    public String getCurrentCell(double sx,double sy)
    {
        double x=sx;
        double y=sy;
        x=x/DIAMETER;
        int ax=(int)x;

        y=y/DIAMETER;
        int ay=(int)y;

        return ax+"|"+ay;
    }

    ////计算与当前格子相邻的8各格子的编号
    public List getNeberCell(String sx, String sy) {
        double x = Double.parseDouble(sx);
        double y = Double.parseDouble(sy);
        x = x / DIAMETER;
        int ax = (int) x;

        y = y / DIAMETER;
        int ay = (int) y;

        String[] temp = new String[9];
        if (ax - 1 >= 0) {
            temp[1] = (ax - 1) + "|" + (ay + 1);
            temp[4] = (ax - 1) + "|" + ay;
            if (ay - 1 > 0) {
                temp[6] = (ax - 1) + "|" + (ay - 1);
            }
        }
        if (ay - 1 > 0) {
            temp[7] = ax + "|" + (ay - 1);
            temp[8] = ax + 1 + "|" + (ay - 1);
        }

        temp[2] = ax + "|" + (ay + 1);
        temp[3] = (ax + 1) + "|" + (ay + 1);
        temp[0] = ax + "|" + ay;
        temp[5] = (ax + 1) + "|" + ay;

        List<String> outList = new ArrayList();
        for (int i = 0; i < 9; i++) {
            if (!outList.contains(temp[i])) {
                outList.add(temp[i]);
            }
        }

        return outList;
    }


    ////计算与当前格子相交N米格子的编号
    public List getCrossCell(String sx,String sy)
    {
        double r=EXPAND;
        double x=Double.parseDouble(sx);
        double y=Double.parseDouble(sy);

        double xl=x-r;
        double xh=x+r;
        double yl=y-r;
        double yh=y+r;

        x=x/DIAMETER;

        y=y/DIAMETER;
        int ax=(int)x;
        int ay=(int)y;

        String curCel;
        curCel=ax+"|"+ay;

        xl=xl/DIAMETER;
        int axl=(int)xl;
        yl=yl/DIAMETER;
        int ayl=(int)yl;

        xh=xh/DIAMETER;
        int axh=(int)xh;
        yh=yh/DIAMETER;
        int ayh=(int)yh;

        List<String> outList=new ArrayList();//vector<string>out;


        for(int i=axl;i<=axh;i++){
            for(int j=ayl;j<=ayh;j++){


                String temp = i+"|"+j;
                if(!outList.contains(temp)){
                    outList.add(temp);
                }
            }
        }




//        String []temp=new String[4];
//        temp[0]=axl+"|"+ayh;
//        temp[1]=axh+"|"+ayh;
//        temp[2]=axl+"|"+ayl;
//        temp[3]=axh+"|"+ayl;
//
//
//
//
//
//        outList.add(curCel);
//        for(int i=0;i<4;i++)
//        {
//            if(!outList.contains(temp[i]) &&!temp[i].trim().equals(""))
//            {
//                outList.add(temp[i]);
//            }
//        }

        return outList;
    }

//       ////获取区域内所有格子的编号
//    public List getCellsByBox(BoundingBox bbox)
//    {
//        String left=getCurrentCell(bbox.MinLongitude,bbox.MinLatitude);
//        String right=getCurrentCell(bbox.MaxLongitude,bbox.MaxLatitude);
//        System.out.println(left);
//        System.out.println(right);
//
//        double x=bbox.MinLongitude;
//        double y=bbox.MinLatitude;
//        x=x/DIAMETER;
//        int axmin=(int)x;
//        y=y/DIAMETER;
//        int aymin=(int)y;
//
//        x=bbox.MaxLongitude;
//        y=bbox.MaxLatitude;
//        x=x/DIAMETER;
//        int axmax=(int)x;
//        y=y/DIAMETER;
//        int aymax=(int)y;
//
//
//        List<String> outList=new ArrayList();
//        for(int i=axmin;i<=axmax;i++)
//        {
//            for(int j=aymin;j<=aymax;j++)
//            {
//               StringBuffer buffer=new StringBuffer();
//               buffer.append(i).append("|").append(j);
//               outList.add(buffer.toString());
//            }
//        }
//
//        return outList;
//    }

//           ////获取区域内所有格子的编号
//    public List getCellsByBoxExpand(BoundingBox bbox)
//    {
//        String left=getCurrentCell(bbox.MinLongitude,bbox.MinLatitude);
//        String right=getCurrentCell(bbox.MaxLongitude,bbox.MaxLatitude);
//        System.out.println(left);
//        System.out.println(right);
//        
//        double x=bbox.MinLongitude;
//        double y=bbox.MinLatitude;
//        x=x/DIAMETER;
//        int axmin=(int)x;
//        if(axmin-1>0)
//        {
//            axmin--;
//        }
//        y=y/DIAMETER;
//        int aymin=(int)y;
//        if(aymin-1>0)
//        {
//            aymin--;
//        }
//        
//        x=bbox.MaxLongitude;
//        y=bbox.MaxLatitude;
//        x=x/DIAMETER;
//        int axmax=(int)x+1;
//        y=y/DIAMETER;
//        int aymax=(int)y+1;
//        
//        
//        List<String> outList=new ArrayList();
//        for(int i=axmin;i<=axmax;i++)
//        {
//            for(int j=aymin;j<=aymax;j++)
//            {
//               StringBuffer buffer=new StringBuffer();
//               buffer.append(i).append("|").append(j);
//               outList.add(buffer.toString());
//            }
//        }
//        
//        return outList;
//    }

    public static void main(String[] args) {
        CellCut c=new CellCut(2000,50);
//        BoundingBox bbox=new BoundingBox();
//        bbox.MaxLatitude=4114806.4712998173;
//        bbox.MaxLongitude=12655953.338503184;
//        bbox.MinLatitude=4109358.1220662734;
//        bbox.MinLongitude=12654427.838622805;
//        List<String> outList= c.getCellsByBox(bbox);

//        Map<String,String> map = new HashMap<String,String>();
//
//        List<Poi> poiList = new ArrayList<Poi>();
//        try {
//            BufferedReader reader = FileHandler.getReader("D:\\testresult1.txt","utf-8");
//            String line = "";
//            while ((line= reader.readLine()) != null){
//
//                Poi poi = new Poi();
//                String[] result = line.split("\t");
//                poi.setName(result[0]);
//
//                String pointstr = result[1];
//                String[] re = pointstr.split(",");
//                poi.setPoint(result[1]);
//                poiList.add(poi);
//
//                String out=c.getCurrentCell(re[0],re[1]);
//                map.put(result[0],out);
//
//
//            }
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }





        String out=c.getCurrentCell("1.29501200181877E7","4837005.00542882");
        System.out.println(out);

        List<String> outList=c.getCrossCell("1.29594119928106E7","4847111.33093773");
                System.out.println("list1~~~~~~~~~~~~~~~~~");

        for(String s:outList)
        {
            System.out.println(s);
        }

//        System.out.println("~~~~~~~~~~~~~~~~~");
//        String out1=c.getCurrentCell("1.29497894357781E7","4836997.29291283");
//        System.out.println(out1);
//        List<String> outList1=c.getCrossCell("1.29593126278016E7","4846851.87400383");
//        System.out.println("~~~~~~~~~~~~~~~~~list2");
//
//        for(String s:outList1)
//        {
//            System.out.println(s);
//        }


    }
}
