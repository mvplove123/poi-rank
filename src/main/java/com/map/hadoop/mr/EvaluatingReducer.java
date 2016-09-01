package com.map.hadoop.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.apache.hadoop.thirdparty.guava.common.collect.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * poi Evaluating reducer 评测样本抽测
 */
public class EvaluatingReducer extends Reducer<Text, Text, Text, Text> {

    MultipleOutputs outputs = null;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {


        List<String> poiList = Lists.newArrayList();

        Set<Integer> rankset = Sets.newHashSet();
        Multimap<Integer,String> poiMap = ArrayListMultimap.create();

        for (Text val : values) {
            String poi = val.toString();

            String[] poifields = poi.split("\t");

            String subcategory= poifields[5];
            Integer rank = Integer.valueOf(poifields[17]);

            if(!subcategory.equals("大门")){


                poiMap.put(rank,poi);
                rankset.add(rank);
            }
        }


        if(!rankset.isEmpty() && rankset.size()>2){


            String keys = key.toString();

            String[] keyFileds = keys.split("-");


            String city = keyFileds[0];
            String category = keyFileds[1];
            String outPath =city + "/" + category + "/evaluation";

//        Integer[] rankArray =  (Integer[])rankset.toArray();


            List<Integer> rankArray = new ArrayList<Integer>(rankset);




            int max = rankArray.size();
            int min = 0;


            List<String> noEqualList = Lists.newArrayList();
            List<String> equalList = Lists.newArrayList();

            List<String> noEqualVerifyList = Lists.newArrayList();
            List<String> equalVerifyList = Lists.newArrayList();

            int noEqualCount=0;
            int equalCount=0;
            int totalCount=0;
            do{

                int num1 = (int) (Math.random() * (max - min)) + min;
                int num2 = (int) (Math.random() * (max - min)) + min;


                int first = rankArray.get(num1);
                int second = rankArray.get(num2);

                if(Math.abs(first-second)>=2){

                    getResult(noEqualList,noEqualVerifyList,first,second, poiMap,noEqualCount);
                }else{
                    getResult(equalList,equalVerifyList,first,second, poiMap,equalCount);

                }

                totalCount++;
            }while(totalCount<100);


            //输出
            if (!noEqualList.isEmpty()) {

                String noEqualFilePath = city + "/" + category + "/noEqualEvaluation";
                outExlortFile(noEqualList,noEqualFilePath);

            }

            if(!equalList.isEmpty()){
                String equalFilePath = city + "/" + category + "/equalEvaluation";
                outExlortFile(equalList,equalFilePath);
            }
        }






    }



    private void getResult(List<String> outList,List<String> poiVerifyList,int first,int second,Multimap<Integer,String>
            poiMap,int count){
        List<String> firstString = (List<String>)poiMap.get(first);
        List<String> secondString = (List<String>)poiMap.get(second);


        int firstStringNum = firstString.size();
        int secondStringNum = secondString.size();
        int minNum = 0;
        int firstnum = (int) (Math.random() * (firstStringNum - minNum)) + minNum;
        int secondnum = (int) (Math.random() * (secondStringNum - minNum)) + minNum;


        String poi0 = firstString.get(firstnum);
        String poi1 = secondString.get(secondnum);

        if(!poiVerifyList.contains(poi0) || !poiVerifyList.contains(poi1)){


            Joiner joiner = Joiner.on("\n");
            String mapValue = joiner.join(new String[]{poi0, poi1," "});
            outList.add(mapValue);

            poiVerifyList.add(poi0);
            poiVerifyList.add(poi1);
            count++;
        }
    }


    private void outExlortFile(List<String>poiList,String outPath){
        int i=poiList.size();

        try{
            if(i>5){
                for(int j=0;j<5;j++){
                    outputs.write(NullWritable.get(), new Text(poiList.get(j)), outPath);
                }
            }else{
                for(int j=0;j<i;j++){
                    outputs.write(NullWritable.get(), new Text(poiList.get(j)), outPath);
                }

            }
        }catch (Exception e){

            System.out.println(e.getMessage());
        }


    }





    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }

}
