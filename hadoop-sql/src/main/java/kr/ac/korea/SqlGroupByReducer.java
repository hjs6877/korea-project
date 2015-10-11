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
             * lineNumber�� �ι�° group by�� ���ִ°Ͱ� ����.
             */
            if(lnMap.containsKey(lineNumber)){
                Map<String, Object> lnResultMap = lnMap.get(lineNumber);

                /**
                 * Quantity Sum ���.
                 */
                double sumQuantity = (Double) lnResultMap.get("sumQuantity");
                sumQuantity = sumQuantity + item.getQuantity();
                lnResultMap.put("sumQuantity", sumQuantity);

                /**
                 * Discount�� Max �� ���.
                 */
                double maxDiscount = (Double) lnResultMap.get("maxDiscount");
                if(maxDiscount < item.getDiscount()){
                    maxDiscount = item.getDiscount();
                    lnResultMap.put("maxDiscount", maxDiscount);
                }

                /**
                 * Tax�� Avg �� ���.
                 * - Avg�� ����ϱ� ���� tax �� list�� ���, list�� size�� ������ �ش�.
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
         * ����� ��� �Ѵ�.
         */
        for (Map.Entry<Long, Map<String, Object>> entry : lnMap.entrySet()){
            long lnMapKey = entry.getKey();
            Map<String, Object> lnMapValue = entry.getValue();

            /**
             * Tax�� ���, list�� ��� ������ sum ����, list�� size ������ ����� ����� ���Ѵ�.
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
