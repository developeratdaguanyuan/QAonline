package Doraemon.MongoDB;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
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
 * create database and upload
 *
 */
public class UploadData {
  public static String ENTITY_EMBEDDING = "./data/entity_embedding";
  public static String RELATION_EMBEDDING = "./data/relation_embedding";
  public static String ENTITY_MAP = "./data/FB5M-entity-id.txt";
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
      String db_name, String ct_name, String embed_file_path) {
    try {
      MongoClient mongoClient = new MongoClient("localhost", 27017);

      MongoDatabase mongoDatabase = mongoClient.getDatabase(db_name);
      MongoCollection<Document> collection = mongoDatabase.getCollection(ct_name);
      if (collection == null) {
        mongoDatabase.createCollection(ct_name);
        collection = mongoDatabase.getCollection(ct_name);
      }

      File file = new File(embed_file_path);
      FileInputStream fin = new FileInputStream(file);
      BufferedInputStream bin = new BufferedInputStream(fin);
      DataInputStream din = new DataInputStream(bin);
      int count = (int) (file.length() / 4);
      for (int i = 0; i < count / 256; i++) {
        System.out.println(i);
        List<Double> array = new LinkedList<Double>();
        for (int j = 0; j < 256; j++) {
          array.add((double)din.readFloat());
        }
        Document document = new Document("mid", entity_id_map.get(i + 1));
        document.append("id", i + 1);
        document.append("embedding", array);
        collection.insertOne(document);
      }
    } catch (Exception e) {
      System.err.println(e.getClass().getName() + ": " + e.getMessage());
    }
  }
  
  public static void main(String[] args) throws IOException {
    loadDict(ENTITY_MAP, entity_id_map);
    updateFreebaseEmbedding("test", "collection", ENTITY_EMBEDDING);
  }
}
