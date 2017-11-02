import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static org.junit.Assert.*;

public class LogTest {
    @Test
    public void getIp() throws Exception {
        Log.GetCollection();
        List<String> list = Log.GetIp("http://www.pravda.com.ua/");
        List<String> test = new ArrayList<>();
        test.add("127.0.0.0");
        assertEquals(list.equals(test), true);
    }

    @Test
    public void getURL() throws Exception {
        Log.GetCollection();
        List<String> list = Log.GetURL("127.0.0.1");
        List<String> test = new ArrayList<>();
        test.add("https://uk.wikipedia.org/");
        assertEquals(list.equals(test), true);
    }

    @Test
    public void getURLByTime() throws Exception {
        Log.GetCollection();
        List<String> list = Log.GetURL("2017/11/01 20:17:43","2017/11/01 20:18:00");
        List<String> test = new ArrayList<>();
        test.add("https://uk.wikipedia.org/");
        assertEquals(list.equals(test), true);
    }


    @Test
    public void insertDocument(){
        Log.GetCollection();
        Log log = new Log("www/dynamo","164.0.0.12","2017/11/02 15:32:00",90);
        log.InsertDocument();
        Document doc = Log.logsCollection.find(and(eq("url","www/dynamo"),eq("ip","164.0.0.12"))).first();
        assertEquals(doc.isEmpty(),false);
    }

    @Test
    public void updateDocument(){
        Log.GetCollection();
        Log log = new Log("http://dynamo.kiev.ua/","164.0.0.12","2017/11/02 15:32:00",90);
        log.UpdateDocument("www/dynamo");
        Document doc = Log.logsCollection.find(and(eq("url","http://dynamo.kiev.ua/"),eq("ip","164.0.0.12"))).first();
        assertEquals(doc.isEmpty(),false);
    }

    @Test
    public void deleteDocument() {
        Log.GetCollection();
        Log log = new Log("http://kkkk.kiev.ua/","164.0.0.12","2017/11/02 15:32:00",100);
        log.InsertDocument();
        log.DeleteDocument();
        Document doc = Log.logsCollection.find(and(eq("url","http://kkkk.kiev.ua/"),eq("ip","164.0.0.12"))).first();
        assertEquals(doc,null);
    }


}