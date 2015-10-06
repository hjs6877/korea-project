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
        // Path에 접근한 전체 IP 건수와 중복된 IP를 필터링 한 Unique한 IP건수를 계산한다.
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
     * 해당 Path의 전체 IP 건수와 Unique한 IP 건수를 계산.
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
         * 1. Iterable을 반복 순회
         * 2. IP가 Map에 존재한다면 unique하지 않으므로 필터링한다.
         * 3. 전체건수는 루프를 돌때마다 1씩 증가해서 전체건수를 계산한다.
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
