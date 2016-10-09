package doraemon.mongoDBDataStructure;

import java.util.List;

public class EmbeddingData {
  private String mid;
  private int id;
  private List<Double> embedding;
  
  public void setMid(String m) {
    mid = m;
  };
  
  public void setId(int i) {
    id = i;
  }
  
  public void setEmbedding(List<Double> e) {
    embedding = e;
  }
  
  public String getMid() {
    return mid;
  }
  
  public int getId() {
    return id;
  }
  
  public List<Double> getEmbedding() {
    return embedding;
  }
}
