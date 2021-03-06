package kr.ac.korea;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ideapad on 2015-10-11.
 *
 *
 */
public class SqlGroupByMapper extends Mapper<LongWritable, Text, LongWritable, LineItem/**Mapper의 출력 value를 LineItem 객체로 지정 */> {
    LongWritable orderKeyLong = new LongWritable();
    LineItem lineItem = new LineItem();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lineItems = value.toString().split("\\|");

//        System.out.println("lineItems[0]orderkey: " + lineItems[0]);
//        System.out.println("lineItems[3]linenumber: " + lineItems[3]);
//        System.out.println("lineItems[4]quantity: " + lineItems[4]);
//        System.out.println("lineItems[6]discount: " + lineItems[6]);
//        System.out.println("lineItems[7]: " + lineItems[7]);

        long orderKey     = Long.parseLong(lineItems[0]);
        long linenumber   = Long.parseLong(lineItems[3]);
        double quantitiy    = Double.parseDouble(lineItems[4]);
        double discount     = Double.parseDouble(lineItems[6]);
        double tax          = Double.parseDouble(lineItems[7]);


        /**
         * orderKey를 Mapper의 출력 key로 설정.
         */
        orderKeyLong.set(orderKey);


        /**
         * separated된 값들을 LineItem 객체에 담는다.
         */
        lineItem.set(orderKey, linenumber, quantitiy, discount, tax);

        /**
         * orderkey로 그룹핑되므로, sql에서 orderkey로 첫번째 group by를 하는것과 같다.
         */
        context.write(orderKeyLong, lineItem);
    }
}
