package doraemon.mongoDB;

import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import doraemon.mongoDBDataStructure.EmbeddingData;
import doraemon.mongoDBDataStructure.MidNameData;

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
  
  public static EmbeddingData findDataByMID(String ct_name, String mid) {
    MongoCollection<Document> collection = mongoDatabase.getCollection(ct_name);
    if (collection == null) {
      return null;
    }
    
    FindIterable<Document> iterable = collection.find(new Document("mid", mid));
    if(iterable.first() == null) {
      return null;
    }
    
    EmbeddingData result = new EmbeddingData();
    result.setId((Integer)iterable.first().get("id"));
    result.setMid(mid);
    result.setEmbedding((List<Double>)iterable.first().get("embedding"));
    return result;
  }
  
  public static EmbeddingData findDataByID(String ct_name, int id) {
    MongoCollection<Document> collection = mongoDatabase.getCollection(ct_name);
    if (collection == null) {
      return null;
    }
    
    FindIterable<Document> iterable = collection.find(new Document("id", id));
    if(iterable.first() == null) {
      return null;
    }
    
    EmbeddingData result = new EmbeddingData();
    result.setId(id);
    result.setMid((String)iterable.first().get("mid"));
    result.setEmbedding((List<Double>)iterable.first().get("embedding"));
    return result;
  }
  
  public static MidNameData findNameByMID(String ct_name, String mid) {
    MongoCollection<Document> collection = mongoDatabase.getCollection(ct_name);
    if (collection == null) {
      return null;
    }
    
    FindIterable<Document> iterable = collection.find(new Document("mid", mid));
    if(iterable.first() == null) {
      return null;
    }
    
    MidNameData result = new MidNameData();
    result.setMid((String)iterable.first().get("mid"));
    result.setName((String)iterable.first().get("name"));

    return result;
  }
  
  public static void main(String[] args) {
    connectDB("freebase");
    
    EmbeddingData id_result = findDataByID("entityEmbedding", 1);
    if (id_result == null) {
      System.out.println("it is empty");
    } else {
      for (Double val : id_result.getEmbedding()) {
        System.out.println(val);
      }
      System.out.println(id_result.getMid());
      System.out.println();
    }
    
    EmbeddingData mid_result = findDataByMID("entityEmbedding", "/m/06_fxzj");
    if (mid_result == null) {
      System.out.println("it is empty");
    } else {
      //System.out.println(mid_result.getId());
      for (Double val : id_result.getEmbedding()) {
        System.out.println(val);
      }
      System.out.println(id_result.getMid());
      System.out.println();
    }
    
    MidNameData name_result = findNameByMID("entityMidName", "/american_football/football_player/footballdb_id");
    if (name_result == null) {
      System.out.println("it is empty");
    } else {
      System.out.println(name_result.getName());
    }
    
    return;
  }
}
