package com.techvalley.parse.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 单例的线程安全的Map；
 * @author ling
 *
 */
public class StatusSingle {
	private static Log log = LogFactory.getLog(StatusSingle.class);
	public static ConcurrentHashMap<String, Future<ParseParameter>> STATUS_MAP 
	                                                                = new ConcurrentHashMap<String, Future<ParseParameter>>();

	public void submit(String key, Future<ParseParameter> fu){
		log.debug("Put the key=>" + key + "value=>" + fu.toString());
		
		log.info("the key=>" + key + " add to the Status_Map, it size=>" + STATUS_MAP.size());
		System.out.println("the key=>" + key + " add to the Status_Map, it size=>" + STATUS_MAP.size());
		STATUS_MAP.put(key, fu);
	}
	
	public ParseParameter getStatus(String key){		
		Map<String, String> data = new HashMap<String, String>();
		data.put("key", key);
		
		//验证key是否已经存在其中；
		if(!STATUS_MAP.containsKey(key)){
			return this.par("500", "the key=>" + key + "not found.", data);
		}
		
		Future<ParseParameter> fu = STATUS_MAP.get(key);
		//如果可以存在；
		if(!fu.isDone()){//如果当前的文件还没有解析完成；
			return null; 
		}else{
			ParseParameter par = null;
			try {
				par = fu.get();
			} catch (InterruptedException | ExecutionException e) {
				log.warn("the key=>" + key + " has been Interrupted or Executed!", e);
				return this.par("500", "the key=>" + key + " has been Interrupted or Executed!", data);
			}
			STATUS_MAP.remove(key); //删除查询；
			return par;
		}
		
	}
	
	private ParseParameter par(String status, String message, Map<String, String> data){
		ParseParameter par = null;
		if (status != null) {
			par = new ParseParameter();
			par.setStatus(status);
			par.setMsg(message);
			par.setData(data);
		}
		return par;
	}
}
