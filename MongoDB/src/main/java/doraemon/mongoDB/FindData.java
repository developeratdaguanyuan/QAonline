package doraemon.mongoDB;

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
  
  public static Data findData(String ct_name, int id) {
    MongoCollection<Document> collection = mongoDatabase.getCollection(ct_name);
    if (collection == null) {
      return null;
    }
    
    FindIterable<Document> iterable = collection.find(new Document("id", id));
    if(iterable.first() == null) {
      return null;
    }
    
    Data result = new Data();
    result.setId(id);
    result.setMid((String)iterable.first().get("mid"));
    result.setEmbedding((List<Double>)iterable.first().get("embedding"));
    return result;
  }
  
  public static void main(String[] args) {
    connectDB("freebase");
    Data result = findData("entityEmbedding", 0);
    if (result == null) {
      System.out.println("it is empty");
      return;
    }
    for (Double val : result.getEmbedding()) {
      System.out.println(val);
    }
    System.out.println();
    return;
  }
}
