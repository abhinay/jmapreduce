package org.fingertap.jmapreduce;

import java.lang.ClassNotFoundException;
import java.io.UnsupportedEncodingException;

import java.net.URLEncoder;
import java.net.URLDecoder;

import org.msgpack.MessagePack;

public class ValuePacker {
  private static String delimiter = Character.toString('\u0000');
  
  public static String pack(Object value) throws UnsupportedEncodingException {
    if (value instanceof String)
      return value.toString();
    
    String raw = new String(MessagePack.pack(value));
    return value.getClass().getName() + delimiter + URLEncoder.encode(raw, "UTF-8");
  }
  
  public static Object unpack(String value) throws ClassNotFoundException, UnsupportedEncodingException {
    if (value.indexOf(delimiter) == -1)
      return value;
    
    String[] tokens = value.split(delimiter);
    String raw = URLDecoder.decode(tokens[1], "UTF-8");
    return MessagePack.unpack(raw.getBytes(), Class.forName(tokens[0]));
  }
}