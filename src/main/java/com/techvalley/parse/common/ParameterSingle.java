package com.techvalley.parse.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ParameterSingle{
	
	private static Log log = LogFactory.getLog(ParameterSingle.class);
	
	public static Map<String, Future<ParseParameter>> SINGLE_PARAM;
	public static int MAX_PARAM_SIZE = 10000; //最大10000，初始值；
	
	static{
		//初始化；
		SINGLE_PARAM = new HashMap<String, Future<ParseParameter>>();
	}
	
	public static synchronized void submit(String key, Future<ParseParameter> fu){
		//SINGLE_PARAM = SINGLE_PARAM == null ? new HashMap<String, Future<ParseParameter>>() : SINGLE_PARAM;
	   
		//赋值；
		if(SINGLE_PARAM == null){
		   SINGLE_PARAM = new HashMap<String, Future<ParseParameter>>();
	   }
		
		//Note：判断是否达到了最大值，如果达到了。就删除其中一个已经成功的值；??????????
		if(SINGLE_PARAM.size() >= MAX_PARAM_SIZE){
			for(String k : SINGLE_PARAM.keySet()){
				Future<ParseParameter> fut = SINGLE_PARAM.get(k);
				try {
					if(fut.isDone() && fut.get().getStatus().contains("200")){
						SINGLE_PARAM.remove(k);
						break;
					}
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		
		//加入到单例中；
		SINGLE_PARAM.put(key, fu);
	}
	
	public static synchronized ParseParameter getStatus(String key){
		ParseParameter par = null;
		
		Future<ParseParameter> fu = SINGLE_PARAM.get(key);
		if(fu == null){ //处理由于Map中的Size超过了最大限制时，将它清除的情况；Note:???????????
			par = new ParseParameter();
			par.setStatus("500");
			par.setMsg("not found the key or out of max size;");
			return par;
		}else{
			
			if (fu.isDone()) {  //如果任务完成或者失败。
				//处理fu不为空的时候的情况；
				try {
					par = fu.get();
				} catch (InterruptedException e) {

					par = new ParseParameter();
					par.setStatus("500");
					par.setMsg(e.getMessage());
					par.setData(null);

					log.warn("the key [" + key + "] has been interrupted !", e);
					return par;
				} catch (ExecutionException e) {
					par = new ParseParameter();
					par.setStatus("500");
					par.setMsg(e.getMessage());
					par.setData(null);

					log.warn("the key [" + key
							+ "] has exception in excute the programe !", e);
					return par;
				}
				//SINGLE_PARAM.remove(key);
				return par;
			}else{ //如果任务没有完成；
				//什么都不做,返回空值；
				return par;
			} //end if(fu.isDone());
			
		}  //end if(fu == null);

	}
}
