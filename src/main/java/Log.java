import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;


import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.io.*;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;


import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;

import com.mongodb.client.result.UpdateResult;

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
        return "Log [url=" + this.url + ", ip=" + this.ip + ", timeStamp=" + this.timeStamp + ", timeSpent=" + this.timeSpent+ "]";
    }


    public static void GetCollection(){
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        MongoDatabase database = mongoClient.getDatabase("log");
        MongoCollection<Document> collection = database.getCollection("logs");
        logsCollection = collection;
    }

    public void InsertDocument(){
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
    public void UpdateDocument(String urlOld){
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
    public void DeleteDocument(){
        Document doc = logsCollection.find(and(eq("url", this.url),eq("ip",this.ip))).first();
        if(!doc.isEmpty()) {
            logsCollection.deleteOne(doc);
        }
    }

    public static List<String> GetIp(String url){
        List<String> listIP = new ArrayList<>();
        FindIterable<Document> coll = logsCollection.find(eq("url", url)).sort(Sorts.ascending("ip"));
        for(Document doc : coll){
            System.out.println("IP: " + doc.getString("ip"));
            listIP.add(doc.getString("ip"));
        }
        return  listIP;
    }
    public static List<String> GetURL(String timeStart,String timeEnd){
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
        finally {
            return listURL;
        }

    }
    public static List<String> GetURL(String ip){
        List<String> listURL = new ArrayList<>();
        FindIterable<Document> coll = logsCollection.find(eq("ip", ip)).sort(Sorts.ascending("url"));
        for(Document doc : coll){
            System.out.println("URL: " + doc.getString("url"));
            listURL.add(doc.getString("url"));
        }
        return listURL;
    }


    public static List<String> MapReduceURLByTime(){
        MongoClient mongo = null;
        try {
            mongo = new MongoClient("localhost", 27017);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        DB db = mongo.getDB("log");
        DBCollection collection = db.getCollection("logs");
        String map ="function () { emit('url','timeSpent'); }";
        String reduce = "function (key, values) { return Array.sum(values); }";
        MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce,
                null, MapReduceCommand.OutputType.INLINE, null);

        MapReduceOutput out = collection.mapReduce(cmd);

        for (DBObject o : out.results()) {
            System.out.println(o.toString());
        }
        System.out.println("Done");

        return null;

    }


    public static void main( String args[] ) throws FileNotFoundException, UnsupportedEncodingException {

        //GetCollection();

        //ReadCSV.ConvertFromCSVToJSON("data.txt");
        MapReduceURLByTime();
        //GetDate();

    }
}
