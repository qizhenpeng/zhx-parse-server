package com.techvalley.parse.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;


public class ParserServerConfig {
	  public static Configuration HDFS_CONF = new Configuration();

	  public static String HBASE_TABLE_NAME;
	  public static String HBASE_FAMILY;
	  public static int ABSTRACT_SUM = 10;
	  public static String HDFS_BASE_PATH;

	  static
	  {
		Properties prop =  new  Properties();    
	    InputStream in = ParserServerConfig.class.getClassLoader().getResourceAsStream("hdfs.properties");    
	    try {
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}    
	    HDFS_CONF.set("dfs.nameservices", prop.getProperty("dfs.nameservices").trim());
	    HDFS_CONF.set("fs.defaultFS", prop.getProperty("fs.defaultFS").trim());
	    HBASE_TABLE_NAME = prop.getProperty("hbase.table.name").trim();
	    HBASE_FAMILY = prop.getProperty("hbase.family").trim();
	    HDFS_BASE_PATH = prop.getProperty("hdfs.base.path").trim();
	  }
	  
	  public static void main(String[] args) {
		System.out.println(HDFS_CONF.get("fs.defaultFS"));
		System.out.println(HBASE_TABLE_NAME);
		System.out.println(HBASE_FAMILY);
		System.out.println(HDFS_BASE_PATH);
	}
}
