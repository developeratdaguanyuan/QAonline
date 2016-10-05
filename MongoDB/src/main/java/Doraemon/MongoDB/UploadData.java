package Doraemon.MongoDB;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * create database and upload
 *
 */
public class UploadData {
  public static String ENTITY_EMBEDDING = "./data/entity_embedding";
  public static String RELATION_EMBEDDING = "./data/relation_embedding";
  public static String ENTITY_MAP = "./data/FB5M-entity-id.txt";
  public static String RELATION_MAP = "./data/FB5M-relation-id.txt";
  
  public static Map<String, Integer> entity_id_map = new HashMap<String, Integer>();
  public static Map<String, Integer> relation_id_map = new HashMap<String, Integer>();
  
  public static void loadDict(String path, Map<String, Integer> id_map) {

    
  }
  
  
  public static void updateFreebaseEmbedding(String db_name, String ct_name) {
    try {
      MongoClient mongoClient = new MongoClient("localhost", 27017);

      MongoDatabase mongoDatabase = mongoClient.getDatabase(db_name);
      MongoCollection<Document> collection = mongoDatabase.getCollection(ct_name);
      if (collection == null) {
        mongoDatabase.createCollection(ct_name);
        collection = mongoDatabase.getCollection(ct_name);
      }
      List<Integer> array = new LinkedList<Integer>();
      array.add(0);
      array.add(1);
      array.add(2);
      Document document = new Document("word", "MongoDB");
      document.append("array", array);
      List<Document> documents = new ArrayList<Document>();  
      documents.add(document);  
      collection.insertMany(documents);
      System.out.println("Connect to database successfully");

    } catch (Exception e) {
      System.err.println(e.getClass().getName() + ": " + e.getMessage());
    }
  }
  
  public static void main(String[] args) {
    updateFreebaseEmbedding("test", "collection");
    
  }
}
