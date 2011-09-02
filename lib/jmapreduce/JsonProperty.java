package org.fingertap.jmapreduce;

import java.util.HashMap;
import java.net.URLDecoder;
import java.lang.reflect.Type;
import java.io.UnsupportedEncodingException;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JsonProperty {
  private static Gson gson = new Gson();
  private static Type mapType = new TypeToken<HashMap<String, String>>() {}.getType();
  private static Type arrayType = new TypeToken<String[]>() {}.getType();
  
  public static HashMap parse(String json) throws UnsupportedEncodingException {
    return gson.fromJson(URLDecoder.decode(json, "UTF-8"), mapType);
  }
  
  public static String arrayToJson(String[] array) {
    return gson.toJson(array);
  }
  
  public static String hashToJson(HashMap<String,String> hash) {
    return gson.toJson(hash);
  }
  
  public static String[] arrayFromJson(String json) throws UnsupportedEncodingException {
    return gson.fromJson(json, arrayType);
  }
  
  public static HashMap hashFromJson(String json) throws UnsupportedEncodingException {
    return gson.fromJson(json, mapType);
  }
}