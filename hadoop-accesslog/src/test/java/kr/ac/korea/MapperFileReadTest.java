package kr.ac.korea;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by ideapad on 2015-09-17.
 */
public class MapperFileReadTest {
    private static FileReader reader;
    private static BufferedReader br;
    @BeforeClass
    public static void loadFile() throws FileNotFoundException {
        reader =   new FileReader("F:\\3_private_project\\1_project\\2_korea\\2_201502\\korea-project\\hadoop-accesslog\\src\\test\\java\\kr\\ac\\korea\\access_log");
        br = new BufferedReader(reader);
    }
    @Test
    public void getReadPathTest() throws IOException {
        String line = "";
        int i = 1;
        while((line = br.readLine()) != null){
            System.out.println("line: " + i++);
            int start = line.indexOf("GET") != -1 ? line.indexOf("GET")+3 : (line.indexOf("OPTIONS") != -1
                    ? line.indexOf("OPTIONS")+7 : (line.indexOf("POST") != -1 ? line.indexOf("POST")+4 : 0));
            int end = line.indexOf("HTTP") != -1 ?line.indexOf("HTTP")-1 : 0;
            System.out.println("start: " + start);
            System.out.println("#" + line.substring(start, end).trim() + "#");
        }
        assertEquals("","");
    }

    @Test
    public void getIpTest() throws IOException {
        String line = "";
        while((line = br.readLine()) != null){
            System.out.println("#" + line.substring(0,line.indexOf("- -")-1) + "#");
        }
        assertEquals("","");
    }
    public static void endUp() throws IOException {
        br.close();
        reader.close();
    }
}
