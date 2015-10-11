package kr.ac.korea;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ideapad on 2015-10-11.
 */
public class SqlGroupByDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
            System.exit(-1);
        }


        Job job = new Job();
        job.setJarByClass(SqlGroupByDriver.class);
        job.setJobName("Sql Group By Job");
        job.setMapperClass(SqlGroupByMapper.class);
        //job.setCombinerClass(SqlGroupByReducer.class);
        job.setReducerClass(SqlGroupByReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);

        /**
         * Mapper의 출력을 LineItem 클래스로 지정. LineItem은 WritableComparable의 구현체임.
         */
        job.setMapOutputValueClass(LineItem.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
