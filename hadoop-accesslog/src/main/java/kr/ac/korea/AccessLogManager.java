package kr.ac.korea;

/**
 * Created by ideapad on 2015-09-17.
 */
public class AccessLogManager {
    public static String getAccessPath(String line){
        int start = line.indexOf("GET") != -1 ? line.indexOf("GET")+3 : (line.indexOf("OPTIONS") != -1
                ? line.indexOf("OPTIONS")+7 : (line.indexOf("POST") != -1 ? line.indexOf("POST")+4 : 0));
        int end = line.indexOf("HTTP") != -1 ?line.indexOf("HTTP")-1 : 0;
        String accessPath = line.substring(start, end).trim();
        return accessPath;
    }

    public static String getAccessIp(String line){
        String ip = line.substring(0,line.indexOf("- -")-1);

        return ip;
    }
}
