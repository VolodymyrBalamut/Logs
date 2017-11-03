package mylogs;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import static com.mongodb.client.model.Filters.*;

public class Log {
    public String url;
    public String ip;
    public String timeStamp;
    public Integer timeSpent;

    public static MongoCollection<Document> logsCollection;

    public Log(String url,String ip,String timeStamp,int timeSpent){
        this.url = url;
        this.ip = ip;
        this.timeStamp = timeStamp;
        this.timeSpent = timeSpent;
    }
    @Override
    public final int hashCode(){
        int hash = 0;
        hash += url.hashCode();
        hash += ip.hashCode();
        hash *= timeSpent;
        hash -= timeStamp.hashCode();
        return hash;
    }
    @Override
    public final boolean equals(Object obj){
        if (obj instanceof Log)
        {
            if (obj == null)
            {return false;}

            if (this == obj)
            {return true;}

            if (this.hashCode() == obj.hashCode())
            {return true;}
        }
        return false;
    }
    @Override
    public String toString() {
        return "mylogs.Log [url=" + this.url + ", ip=" + this.ip + ", timeStamp=" + this.timeStamp + ", timeSpent=" + this.timeSpent+ "]";
    }


    public static void getCollection(){
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        MongoDatabase database = mongoClient.getDatabase("log");
        MongoCollection<Document> collection = database.getCollection("logs");
        logsCollection = collection;
    }

    public void insertDocument(){
        DateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        try {
            Date date1 = format.parse(this.timeStamp);
            //Date date2 = format.parse(this.timeSpent);
            Document doc = new Document("url", this.url)
                    .append("ip", this.ip)
                    .append("timeStamp",date1)
                    .append("timeSpent",this.timeSpent);
            logsCollection.insertOne(doc);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
    public void updateDocument(String urlOld){
        DateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        try{
            Date date1 = format.parse(this.timeStamp);
            //Date date2 = format.parse(this.timeSpent);
            Document doc = new Document("url", this.url)
                    .append("ip", this.ip)
                    .append("timeStamp",date1)
                    .append("timeSpent",this.timeSpent);
            logsCollection.updateOne(eq("url", urlOld), new Document("$set", doc));
        }catch (ParseException e) {
            e.printStackTrace();
        }
    }
    public void deleteDocument(){
        Document doc = logsCollection.find(and(eq("url", this.url),eq("ip",this.ip))).first();
        if(!doc.isEmpty()) {
            logsCollection.deleteOne(doc);
        }
    }

    public static List<String> getIp(String url){
        List<String> listIP = new ArrayList<>();
        FindIterable<Document> coll = logsCollection.find(eq("url", url)).sort(Sorts.ascending("ip"));
        for(Document doc : coll){
            System.out.println("IP: " + doc.getString("ip"));
            listIP.add(doc.getString("ip"));
        }
        return  listIP;
    }
    public static List<String> getURL(String timeStart, String timeEnd){
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        List<String> listURL = new ArrayList<>();
        try {
            Date date1 = format.parse(timeStart);
            Date date2 = format.parse(timeEnd);
            FindIterable<Document> coll = logsCollection.find(and(gte("timeStamp", date1), lt("timeStamp", date2)));
            for(Document doc : coll){
                System.out.println("URL: " + doc.getString("url"));
                listURL.add(doc.getString("url"));
            }
        } catch (ParseException e) {
             e.printStackTrace();
        }
        return listURL;

    }
    public static List<String> getURL(String ip){
        List<String> listURL = new ArrayList<>();
        FindIterable<Document> coll = logsCollection.find(eq("ip", ip)).sort(Sorts.ascending("url"));
        for(Document doc : coll){
            System.out.println("URL: " + doc.getString("url"));
            listURL.add(doc.getString("url"));
        }
        return listURL;
    }


    public static HashMap<String,String> mapReduceURLByTime(){
        DBCollection collection = connectForMapReduce();
        HashMap<String,String> dictionary = new HashMap<>();

        String map ="function () { emit(this.url,this.timeSpent); }";
        String reduce = "function (key, timeSpent) { var total =0;"
                + "for(var i =0; i<timeSpent.length;i++){"
                +"total = total +timeSpent[i]; } "
                + "return total; }";
        MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce,
                null, MapReduceCommand.OutputType.INLINE, null);

        MapReduceOutput out = collection.mapReduce(cmd);
        dictionary = convertToDictionary(out);
        return dictionary;
    }

    public static HashMap<String,String> mapReduceURLByCount(){
        DBCollection collection = connectForMapReduce();
        HashMap<String,String> dictionary = new HashMap<>();
        String map ="function () { var count =1; emit(this.url,count); }";
        String reduce = "function (key, count) { var total =0;"
                + "for(var i =0; i<count.length;i++){"
                +"total = total + 1; } "
                + "return total; }";
        MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce,
                null, MapReduceCommand.OutputType.INLINE, null);

        MapReduceOutput out = collection.mapReduce(cmd);
        dictionary = convertToDictionary(out);
        return dictionary;
    }
    public static HashMap<String,String> mapReduceURLByURLAndDay(){
        DBCollection collection = connectForMapReduce();
        HashMap<String,String> dictionary = new HashMap<>();
        String map ="function () { var count =1;"+
                "var day = (this.timeStamp.getFullYear() + \"-\" + (this.timeStamp.getMonth()+1) + \"-\" +this.timeStamp.getDate());"+
                //"var day = 1;"+
                "emit({day:day,url:this.url},count); }";
        String reduce = "function (key, count) { var total =0;"
                + "for(var i =0; i<count.length;i++){"
                +"total = total + 1; } "
                + "return total; }";
        MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce,
                null, MapReduceCommand.OutputType.INLINE, null);

        MapReduceOutput out = collection.mapReduce(cmd);
        dictionary = convertToDictionary(out);
        return dictionary;
    }
    public static HashMap<String,String> mapReduceIPByCountTimeSpent(){
        DBCollection collection = connectForMapReduce();
        HashMap<String,String> dictionary = new HashMap<>();
        String map ="function () { var count =1; emit(this.ip,{count:count,timeSpent:this.timeSpent}); }";
        String reduce = "function (key, values) { var total =0; var timeSpentTotal =0;"
                + "for(var i =0; i<values.length;i++){"
                +"total = total + 1; timeSpentTotal = timeSpentTotal + values[i].timeSpent; } "
                + "return {total,timeSpentTotal}; }";
        MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce,
                null, MapReduceCommand.OutputType.INLINE, null);

        MapReduceOutput out = collection.mapReduce(cmd);
        dictionary = convertToDictionary(out);
        return dictionary;
    }


    private static DBCollection connectForMapReduce(){
        MongoClient mongo = null;
        try {
            mongo = new MongoClient("localhost", 27017);
        } catch (Exception e) {
            e.printStackTrace();
        }
        DB db = mongo.getDB("log");
        DBCollection collection = db.getCollection("logs");
        return collection;
    }

    private static HashMap<String,String> convertToDictionary(MapReduceOutput out){
        HashMap<String,String> dictionary =new HashMap<>();
        for (DBObject o : out.results()) {
            System.out.println(o.toString());
            Map omap = o.toMap();
            String url ="";
            String total = "";
            boolean flag = false;
            for (Object key : omap.keySet()) {
                if(!flag){
                    url = omap.get(key).toString();
                }
                else{
                    total = omap.get(key).toString();
                }
                flag = true;
            }
            dictionary.put(url,total);
        }

        System.out.println("Done");
        return dictionary;
    }
    public static void main( String args[] ) throws FileNotFoundException, UnsupportedEncodingException {
        mapReduceURLByTime();
        mapReduceURLByCount();
        mapReduceURLByURLAndDay();
        mapReduceIPByCountTimeSpent();
    }
}
