package mylogstest;

import mylogs.Log;
import mylogs.ReadCSV;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ReadCSVTest {
    @Test
    public void readFromCSV() throws Exception {
        List<Log> logs = ReadCSV.readFromCSV("data.txt");
        List<Log> test = new ArrayList<>();
        test.add(new Log("http://www.bbb.com.ua/","127.0.0.0","2017/11/01 21:12:00",100));
        test.add(new Log("http://www.ccc.com.ua/","128.0.0.3","2017/11/02 16:12:00",200));
        assertEquals(logs.equals(test),true);
    }

    @Test
    public void convertFromCSVToJSON() throws Exception {
        String strJSON = ReadCSV.convertFromCSVToJSON("data.txt");
        String test = "[{\"url\":\"http://www.bbb.com.ua/\",\"ip\":\"127.0.0.0\",\"timeStamp\":\"2017/11/01 21:12:00\",\"timeSpent\":100},{\"url\":\"http://www.ccc.com.ua/\",\"ip\":\"128.0.0.3\",\"timeStamp\":\"2017/11/02 16:12:00\",\"timeSpent\":200}]";
        assertEquals(strJSON.equals(test),true);
    }


}