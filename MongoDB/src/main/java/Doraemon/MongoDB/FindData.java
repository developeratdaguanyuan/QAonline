package Doraemon.MongoDB;

import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class FindData {
  private static MongoClient mongoClient = new MongoClient("localhost", 27017);
  private static MongoDatabase mongoDatabase = mongoClient.getDatabase("test");
  
  public static boolean connectDB(String db_name) {
    mongoClient = new MongoClient("localhost", 27017);
    if (mongoClient == null) {
      return false;
    }
    mongoDatabase = mongoClient.getDatabase(db_name);
    if (mongoDatabase == null) {
      return false;
    }
    return true;
  }
  
  public static List<Double> findData(String ct_name, int id) {
    MongoCollection<Document> collection = mongoDatabase.getCollection(ct_name);
    if (collection == null) {
      return null;
    }
    
    FindIterable<Document> iterable = collection.find(new Document("id", id));
    return (List<Double>)iterable.first().get("embedding");
  }
  
  public static void main(String[] args) {
    connectDB("test");
    List<Double> result = findData("collection", 3);
    for (Double val : result) {
      System.out.println(val + " ");
    }
    System.out.println();
  }
}
