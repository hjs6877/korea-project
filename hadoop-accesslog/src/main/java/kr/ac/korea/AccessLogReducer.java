package kr.ac.korea;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by ideapad on 2015-09-17.
 */
public class AccessLogReducer extends Reducer<Text, Text, Text, Text> {
//    private IntWritable result = new IntWritable();
    private Text path = new Text();
    private Text count = new Text();
    private String pathStr = "Path: ";
    private String countStr = "\nCOUNT: ";
    private String uniqueIpStr = "The number of unique client IP: ";



    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Path�� ������ ��ü IP �Ǽ��� �ߺ��� IP�� ���͸� �� Unique�� IP�Ǽ��� ����Ѵ�.
        HashMap<String, Integer> resultMap = this.getResultMap(values);
        int countTotal = resultMap.get("countTotal");
        int countUnique = resultMap.get("countUnique");


        String pathFull = pathStr.concat(key.toString());

        String totalCountFull = countStr.concat(String.valueOf(countTotal));
        String uniqueIpFull = uniqueIpStr.concat(String.valueOf(countUnique));
        String countFull = totalCountFull.concat("\n").concat(uniqueIpFull).concat("\n\n");


        path.set(pathFull);
        count.set(countFull);


        context.write(path, count);
    }

    /**
     * �ش� Path�� ��ü IP �Ǽ��� Unique�� IP �Ǽ��� ���.
     *
     * @param values
     * @return
     */
    private HashMap<String, Integer> getResultMap(Iterable<Text> values) {
        HashMap<String, Integer> resultMap = new HashMap<String, Integer>();
        HashMap<String, Boolean> uniqueIpMap = new HashMap<String, Boolean>();

        int countTotal = 0;
        int countUnique = 0;

        /**
         * 1. Iterable�� �ݺ� ��ȸ
         * 2. IP�� Map�� �����Ѵٸ� unique���� �����Ƿ� ���͸��Ѵ�.
         * 3. ��ü�Ǽ��� ������ �������� 1�� �����ؼ� ��ü�Ǽ��� ����Ѵ�.
         */
        for(Text val : values){
            String ip = val.toString();
            if(uniqueIpMap.get(ip) == null){
                countUnique = countUnique + 1;
                uniqueIpMap.put(ip, true);
            }
            countTotal = countTotal + 1;
        }

        resultMap.put("countTotal", countTotal);
        resultMap.put("countUnique", countUnique);

        return resultMap;
    }
}
