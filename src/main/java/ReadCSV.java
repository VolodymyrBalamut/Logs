import com.fasterxml.jackson.databind.ObjectMapper;

import java.awt.print.Book;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class ReadCSV {

    public static List readFromCSV(String fileName) {
        List<Log> logs = new ArrayList<>();
        Path pathToFile = Paths.get(fileName);

        try (BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.US_ASCII)) {
            // read the first line from the text file
            String line = br.readLine();
            // loop until all lines are read
            while (line != null) {
                // use string.split to load a string array with the values from
                // each line of // the file, using a comma as the delimiter
                String[] attributes = line.split(",");
                Log log = createLog(attributes);
                // adding book into ArrayList
                logs.add(log);
                // read next line before looping
                // if end of file reached, line would be null
                line = br.readLine();
            }
        } catch (IOException ioe)  {
            ioe.printStackTrace();
        }

        return logs;
    }

    private static Log createLog(String[] metadata) {
        String url = metadata[0];
        String ip = metadata[1];
        String timeStamp = metadata[2];
        String timeSpent = metadata[3];
        // create and return book of this metadata
        return new Log(url,ip,timeStamp,Integer.parseInt(timeSpent));
    }

    public static String ConvertFromCSVToJSON(String fileName){
        List<Log> logs = ReadCSV.readFromCSV("data.txt");
        String jsonInString = "";
        try{
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(new File("file.json"),logs);
            jsonInString = mapper.writeValueAsString(logs);
        }
        catch (IOException ioex){
            ioex.printStackTrace();
        }
        finally {
            return jsonInString;
        }
    }

    public static void main( String args[] ){
        List<Log> logs = readFromCSV("data.txt");

        // let's print all the person read from CSV file
        for (Log b : logs) {
            System.out.println(b);
        }
    }
}


