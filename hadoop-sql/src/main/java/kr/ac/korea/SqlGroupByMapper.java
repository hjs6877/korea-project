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
public class SqlGroupByMapper extends Mapper<LongWritable, Text, LongWritable, LineItem/**Mapper�� ��� value�� LineItem ��ü�� ���� */> {
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
         * orderKey�� Mapper�� ��� key�� ����.
         */
        orderKeyLong.set(orderKey);


        /**
         * separated�� ������ LineItem ��ü�� ��´�.
         */
        lineItem.set(orderKey, linenumber, quantitiy, discount, tax);

        /**
         * orderkey�� �׷��εǹǷ�, sql���� orderkey�� ù��° group by�� �ϴ°Ͱ� ����.
         */
        context.write(orderKeyLong, lineItem);
    }
}
