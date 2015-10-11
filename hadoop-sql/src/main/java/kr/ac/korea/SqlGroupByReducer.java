package kr.ac.korea;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by ideapad on 2015-10-11.
 */
public class SqlGroupByReducer extends Reducer<LongWritable, LineItem, Text, Text> {
    private long rowNum = 1;

    @Override
    protected void reduce(LongWritable key, Iterable<LineItem> values, Context context) throws IOException, InterruptedException {
        Map<Long, Map<String, Object>> lnMap = new HashMap<Long, Map<String, Object>>();

        for(LineItem item : values){
            long lineNumber = item.getLinenumber();
            /**
             * lineNumber로 두번째 group by를 해주는것과 같다.
             */
            if(lnMap.containsKey(lineNumber)){
                Map<String, Object> lnResultMap = lnMap.get(lineNumber);

                /**
                 * Quantity Sum 계산.
                 */
                double sumQuantity = (Double) lnResultMap.get("sumQuantity");
                sumQuantity = sumQuantity + item.getQuantity();
                lnResultMap.put("sumQuantity", sumQuantity);

                /**
                 * Discount의 Max 값 계산.
                 */
                double maxDiscount = (Double) lnResultMap.get("maxDiscount");
                if(maxDiscount < item.getDiscount()){
                    maxDiscount = item.getDiscount();
                    lnResultMap.put("maxDiscount", maxDiscount);
                }

                /**
                 * Tax의 Avg 값 계산.
                 * - Avg를 계산하기 위해 tax 를 list에 담고, list의 size로 나누어 준다.
                 */
                List<Double> taxList = (List)lnResultMap.get("taxList");
                taxList.add(item.getTax());
            }else{
                Map<String, Object> lnResultMap = new HashMap<String, Object>();
                lnResultMap.put("sumQuantity", item.getQuantity());
                lnResultMap.put("maxDiscount", item.getDiscount());

                List<Double> taxList = new ArrayList<Double>();
                taxList.add(item.getTax());
                lnResultMap.put("taxList", taxList);

                lnMap.put(lineNumber, lnResultMap);
            }
        }

        /**
         * 결과를 출력 한다.
         */
        for (Map.Entry<Long, Map<String, Object>> entry : lnMap.entrySet()){
            long lnMapKey = entry.getKey();
            Map<String, Object> lnMapValue = entry.getValue();

            /**
             * Tax의 경우, list에 담긴 값들을 sum 한후, list의 size 값으로 나누어서 평균을 구한다.
             */
            List<Double> taxList = (List)lnMapValue.get("taxList");

            double sumTax = 0;
            for(double tax : taxList){
                sumTax += tax;
            }

            double avgTax = sumTax / taxList.size();

            System.out.println("row:" + rowNum + "\t" + key + "\t\t\t" + lnMapKey + "\t\t\t" +
                    lnMapValue.get("sumQuantity") + "\t" + lnMapValue.get("maxDiscount") + "\t" + avgTax);

            context.write(new Text("row:" + rowNum + "\t"),
                    new Text(key.get() + "\t\t\t" + lnMapKey + "\t\t\t" + lnMapValue.get("sumQuantity") + "\t" +
                            lnMapValue.get("maxDiscount") + "\t" + avgTax));
            rowNum++;
        }
    }
}
