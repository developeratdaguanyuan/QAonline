package doraemon.mongoDB;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * create database and upload.
 * data format: <mid, id, embedding>
 *
 */
public class UploadData {
  private static String ENTITY_EMBEDDING = "./data/entity_embedding.txt";
  private static String RELATION_EMBEDDING = "./data/relation_embedding.txt";
  private static String ENTITY_MAP = "./data/FB5M-entity-id.txt";
  public static String RELATION_MAP = "./data/FB5M-relation-id.txt";
  
  public static Map<Integer, String> entity_id_map = new HashMap<Integer, String>();
  public static Map<Integer, String> relation_id_map = new HashMap<Integer, String>();
  
  public static void loadDict(String path, Map<Integer, String> id_map) throws IOException {
    File fin = new File(path);
    BufferedReader reader = new BufferedReader(new FileReader(fin));
    String line = null;
    while ((line = reader.readLine()) != null) {
      String[] tokens = line.trim().split("\t");
      String id_m = tokens[0].trim();
      String id_d = tokens[1].trim();
      id_map.put(Integer.valueOf(id_d), id_m);
    }
    reader.close();
  }
  
  public static void updateFreebaseEmbedding(
      String db_name, String ct_name, String embed_file_path, Map<Integer, String> id_map) {
    try {
      MongoClient mongoClient = new MongoClient("localhost", 27017);

      MongoDatabase mongoDatabase = mongoClient.getDatabase(db_name);
      MongoCollection<Document> collection = mongoDatabase.getCollection(ct_name);
      if (collection == null) {
        mongoDatabase.createCollection(ct_name);
        collection = mongoDatabase.getCollection(ct_name);
      }

      List<Double> array = new LinkedList<Double>();
      File file = new File(embed_file_path);
      BufferedReader reader = new BufferedReader(new FileReader(file));
      int i = 0;
      String line = null;
      while ((line = reader.readLine()) != null) {
        array.add(Double.valueOf(line.trim()));
        if (array.size() == 256) {
          if (id_map.containsKey(i)) {
            Document document = new Document("mid", id_map.get(i));
            document.append("id", i);
            document.append("embedding", array);
            collection.insertOne(document);
          }
          ++i;
          array.clear();
        }
      }
      
      reader.close();
    } catch (Exception e) {
      System.err.println(e.getClass().getName() + ": " + e.getMessage());
    }
  }
  
  public static void main(String[] args) throws IOException {
    loadDict(ENTITY_MAP, entity_id_map);
    loadDict(RELATION_MAP, relation_id_map);
    updateFreebaseEmbedding("freebase", "entityEmbedding", ENTITY_EMBEDDING, entity_id_map);
    updateFreebaseEmbedding("freebase", "relationEmbedding", RELATION_EMBEDDING, relation_id_map);
  }
}
