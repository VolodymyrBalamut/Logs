import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;

import com.mongodb.client.result.UpdateResult;

public class Log {
    public  String url;
    public  String ip;
    public String timeStamp;
    public String timeSpent;

    public static MongoCollection<Document> logsCollection;
    public static HashMap<String,Log> logsMap;
    public static Block<Document> printBlock = new Block<Document>() {
        @Override
        public void apply(final Document document) {
            System.out.println(document.toJson());
        }
    };

    public Log(String url,String ip,String timeStamp,String timeSpent){
        this.url = url;
        this.ip = ip;
        this.timeStamp = timeStamp;
        this.timeSpent = timeSpent;
    }
    @Override public String toString() {
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
            Date date2 = format.parse(this.timeSpent);
            Document doc = new Document("url", this.url)
                    .append("ip", this.ip)
                    .append("timeStamp",date1)
                    .append("timeSpent",date2);
            logsCollection.insertOne(doc);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
    public void UpdateDocument(String urlOld){
        DateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        try{
            Date date1 = format.parse(this.timeStamp);
            Date date2 = format.parse(this.timeSpent);
            Document doc = new Document("url", this.url)
                    .append("ip", this.ip)
                    .append("timeStamp",date1)
                    .append("timeSpent",date2);
            logsCollection.updateOne(eq("url", urlOld), new Document("$set", doc));
        }catch (ParseException e) {
            e.printStackTrace();
        }
    }
    public void DeleteDocument(){
        logsCollection.deleteOne(eq("url", this.url));
    }
    public static long GetCountOfDocument(){
        return logsCollection.count();
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
    public static void GetURL(String timeStamp,String timeSpent){
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        try {
            Date date1 = format.parse(timeStamp);
            Date date2 = format.parse(timeSpent);
            //logsCollection.find(and(gte("timeStamp", timeStamp), lt("timeSpent", timeSpent)))
              //      .sort(Sorts.descending("url")).forEach(printBlock);
            FindIterable<Document> coll = logsCollection.find(and(gte("timeStamp", date1), lt("timeSpent", date2)));
            for(Document doc : coll){
                System.out.println("URL: " + doc.getString("url"));
            }
        } catch (ParseException e) {
             e.printStackTrace();
        }
    }
    public static List<String> GetURL(String ip){
        //logsCollection.find(eq("ip", ip)).sort(Sorts.ascending("url")).forEach(printBlock);
        List<String> listURL = new ArrayList<>();
        FindIterable<Document> coll = logsCollection.find(eq("ip", ip)).sort(Sorts.ascending("url"));
        for(Document doc : coll){
            System.out.println("URL: " + doc.getString("url"));
            listURL.add(doc.getString("url"));
        }
        return listURL;
    }
    public static void GetDate(){
        Document doc = logsCollection.find().first();
        Date date = doc.getDate("timeStamp");
        SimpleDateFormat formattedDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        formattedDate.setTimeZone(TimeZone.getTimeZone("GMT+02:00"));
        System.out.println(formattedDate.format(date));
    }



    public static void main( String args[] ) throws FileNotFoundException, UnsupportedEncodingException {

        GetCollection();

        ReadCSV.ConvertFromCSVToJSON("data.txt");
        //InsertDocument("http://www.aaa.com.ua/","127.0.0.0","2017/11/01 21:12:00","2017/11/01 21:15:00");
        //UpdateDocument("http://www.pravda.com.ua/","https://stackoverflow.com/","127.0.0.0","2017/11/01 21:12:00","2017/11/01 21:15:00");
        //DeleteDocument("http://www.pravda.com.ua/");
        //Log log1 = new Log("http://www.pravda.com.ua/","127.0.0.0",);
        //log1.InsertDocument();

       // log1.UpdateDocument("John");
        //log1.DeleteDocument("Bogdan");
        //Document myDoc = logsCollection.find().first();
        //System.out.println(myDoc.toJson());

        //Get
        //GetIp("https://stackoverflow.com/");
        //GetURL("127.0.0.0");
       // GetURL("2017/11/01 21:12:00","2017/11/01 21:15:00");
        //GetDate();
        //List<String> listIP = GetIp("http://www.pravda.com.ua/");
        //MongoCursor<Document> cursor = logsCollection.find().iterator();
        //try {
          //  while (cursor.hasNext()) {
           //     System.out.println(cursor.next().toJson());
          //  }
      //  } finally {
         //   cursor.close();
        //}

        //System.out.println("Count of document in collection: "+GetCountOfDocument());
    }
}
