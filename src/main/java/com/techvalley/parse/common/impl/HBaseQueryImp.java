package com.techvalley.parse.common.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

import com.techvalley.parse.common.HBaseQuery;
import com.techvalley.search.exception.RowKeyNullException;
import com.techvalley.search.hbase.ado.DataTable;
import com.techvalley.search.hbase.core.HBaseHelper;
import com.techvalley.search.hbase.utils.Column;

public class HBaseQueryImp implements HBaseQuery{

	private Log log = LogFactory.getLog(HBaseQueryImp.class);
	private HBaseHelper<String> helper;
	
	public HBaseQueryImp(){
		helper = new HBaseHelper<String>();
	}
	public Map<String, String> hbaseQuery(String tableName, String uuid) throws IOException{
	
		log.info("Begin to Query the data with uuid=>" + uuid);
		
		Map<String, String> maps = new HashMap<String, String>();
		
		//验空，并返回
		if(uuid == null || "".equals(uuid)){
			return maps;
		}
		
		//查询数据；
		try {			
			DataTable<String> dt = helper.getRowResult(tableName, uuid);
			Map<String, List<Column>> columns = dt.getRowMap();
			for(String key: columns.keySet()){
				for(Column col : columns.get(key)){
					log.info("quality=>" + col.getQulifier() + " value=>" + Bytes.toString(col.getValue()));
					maps.put(col.getQulifier(), Bytes.toString(col.getValue()));
				}
			}
			
		} catch (RowKeyNullException e) {
			log.warn("Not Found the key, key=>" + uuid);
			e.printStackTrace();
			return maps;
		}
		
		log.info("Successly, Query the data with uuid=>" + uuid + " Query Result Size=>" + maps.size());
		return maps;
	}
}
