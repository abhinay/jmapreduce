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
  
  public static HashMap parse(String json) throws UnsupportedEncodingException {
    return gson.fromJson(URLDecoder.decode(json, "UTF-8"), mapType);
  }
}