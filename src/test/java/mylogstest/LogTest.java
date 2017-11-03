package mylogstest;

import mylogs.Log;
import org.bson.Document;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LogTest {
    @Test
    public void getIp() throws Exception {
        Log.getCollection();
        List<String> list = Log.getIp("http://www.pravda.com.ua/");
        List<String> test = new ArrayList<>();
        test.add("127.0.0.0");
        test.add("164.0.0.12");
        assertEquals(list.equals(test), true);
    }

    @Test
    public void getURL() throws Exception {
        Log.getCollection();
        List<String> list = Log.getURL("127.0.0.1");
        List<String> test = new ArrayList<>();
        test.add("https://uk.wikipedia.org/");
        assertEquals(list.equals(test), true);
    }

    @Test
    public void getURLByTime() throws Exception {
        Log.getCollection();
        List<String> list = Log.getURL("2017/11/01 20:17:43","2017/11/01 20:18:00");
        List<String> test = new ArrayList<>();
        test.add("https://uk.wikipedia.org/");
        assertEquals(list.equals(test), true);
    }


    @Test
    public void ainsertDocument(){
       Log.getCollection();
       Log log = new Log("http://kkk.kiev.ua/","164.0.0.12","2017/11/02 15:32:00",100);log.insertDocument();
       log.insertDocument();
       Document doc = Log.logsCollection.find(and(eq("url","http://kkk.kiev.ua/"),eq("ip","164.0.0.12"))).first();
       assertEquals(doc.isEmpty(),false);
    }

    @Test
    public void bupdateDocument(){
        Log.getCollection();
        Log log = new Log("http://kkkk.kiev.ua/","164.0.0.12","2017/11/02 15:32:00",90);
        log.updateDocument("http://kkk.kiev.ua/");
        Document doc = Log.logsCollection.find(and(eq("url","http://kkkk.kiev.ua/"),eq("ip","164.0.0.12"))).first();
        assertEquals(doc.isEmpty(),false);
    }

    @Test
    public void cdeleteDocument() {
        Log.getCollection();
        Log log = new Log("http://kkkk.kiev.ua/","164.0.0.12","2017/11/02 15:32:00",90);
        log.deleteDocument();
        Document doc = Log.logsCollection.find(and(eq("url","http://kkkk.kiev.ua/"),eq("ip","164.0.0.12"))).first();
        assertEquals(doc,null);
    }

    //@Test
    //public void mapReduceURLByTime(){
       // HashMap<String,String> dict = mylogs.Log.mapReduceURLByTime();
        //HashMap<String,String> test = new HashMap<String,String>();
        //test.put("http://dynamo.kiev.ua/","930.0");
        //test.put("http://www.pravda.com.ua/","50.0");
        //test.put("https://docs.mongodb.com/","700.0");
        //test.put("https://stackoverflow.com/","280.0");
        //test.put("https://uk.wikipedia.org/" ,"60.0");
        //assertEquals(dict.equals(test),true);
    //}


}