package com.techvalley.parse.common.impl;

import java.io.File;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.tika.exception.TikaException;

import com.techvalley.parse.KeyMetadata;
import com.techvalley.parse.Suffix;
import com.techvalley.parse.TikaMeta;
import com.techvalley.parse.TikaMetaCon;
import com.techvalley.parse.TikaUtil;
import com.techvalley.parse.common.ParseParameter;
import com.techvalley.parse.common.ParserServer;
import com.techvalley.parse.common.ParserServerConfig;
import com.techvalley.parse.common.SolrWrite;
import com.techvalley.search.hbase.core.HBaseHelper;
import com.techvalley.search.hbase.utils.ColumnFamily;

/**
 * 
 * @author ling
 *
 */
public class ParserServerImp implements ParserServer{

	private static Log log = LogFactory.getLog(ParserServerImp.class);
	
	private Configuration conf = ParserServerConfig.HDFS_CONF;
	
	/**
	 * 瑙ｆ瀽鏂囦欢鏁版嵁锛�
	 * 鍐欏叆鍒癏Base鍜孲olr涓紱
	 */
	@Override
	public ParseParameter parserHBaseSolr(Path path, Map<String, Object> keys,
			int absSum, SolrWrite sw, String...hbase){
		
		//鍒涘缓ParseParameter锛�
		ParseParameter par = new ParseParameter();
		Map<String, String> data = new HashMap<String, String>();
		data.put("key", keys.get("UUID").toString());
		data.put("path", path.toUri().toString());
		
		//瑙ｆ瀽鏁版嵁锛�
		TikaMetaCon metaCon = null;
		try {
			metaCon = TikaUtil.parse(path, conf, keys, absSum);
		} catch (AccessControlException e) {		
			//璁剧疆鏉冮檺寮傚父锛�
			log.warn("the path=>" + path.toUri().toString() + " don't have permission to read the file!",e);
			e.printStackTrace();
			return this.par("500", e.getMessage(), data);
		} catch (IOException e) {			
			//璁剧疆IO寮傚父锛�
			log.warn("the [" + path.toUri().toString() + "] don't have found !", e);
			e.printStackTrace();
			return this.par("200", "IOException" + e.getMessage(), data);
		} catch (TikaException e) {		
			//璁剧疆Tika瑙ｆ瀽寮傚父锛�
            log.warn("tika parse the [" + path.toUri().toString() + "] exception",e);
            e.printStackTrace();
		    return this.par("500", e.getMessage(), data);
		}
		
		//灏嗚В鏋愮殑鍏冩暟鎹啓鍏ュ埌doc涓紱
		Map<String, String> doc = new HashMap<String, String>(); //瀛樺偍鍐欏叆鍒皊olr涓殑鏁版嵁锛�
		Map<String, String> meta = this.parseMeta(metaCon);
		
		//灏嗚В鏋愮殑鍏冩暟鎹姞鍏�_m鍐欏叆鍒癲oc涓�鐩殑鏄彲浠ュ拰鍔ㄦ�solr瀛楁鍖归厤锛�
		for(String key : meta.keySet()){
			doc.put(key + "_m", meta.get(key));
		}
		
		//灏嗚В鏋愮殑鏁版嵁鍐欏叆鍒癏Base涓紱
		try {
			this.writeToHBase(hbase[0], hbase[1], hbase[2], meta);
		} catch (IOException e) {
			
			//璁剧疆Hbase鍐欏叆閿欒锛�
			data.put("rowKey", hbase[1]);		
			log.warn("the path [" + path.toUri().toString() + "]" + " and the row key [" + hbase[1] + "]" + " write the hbase failed !", e);
			e.printStackTrace();
			return this.par("500", e.getMessage(), data);
		} //Note:hbase蹇呴』鍚湁涓変釜鍙傛暟锛�
		
		//鍐欏叆鏁版嵁鍒癝olr锛�
		keys.remove("tn"); //闄ゅ幓娴佹按鍙凤紱
		for(String key : keys.keySet()){
			doc.put(key, keys.get(key).toString());
		}
		
		//鍐欏叆path_m锛�rowKey_m锛�
		doc.put("rowKey_m", doc.get("rowKey"));
		doc.remove("rowKey");
		doc.remove("path");
		
		try {
			sw.write(doc);
		} catch (SolrServerException e) {
			
			//璁剧疆Solr鏈嶅姟鍣ㄥ紓甯革紱
			log.warn("the [" + path.toUri().toString() + "] not write the data to the Solr!",e);
		    e.printStackTrace();
			return this.par("500", e.getMessage(), data);
			
		} catch (IOException e) {
			
			//璁剧疆IO寮傚父锛�
			log.warn("the [" + path.toUri().toString() + "] IOExcpetion!",e);
		    e.printStackTrace();
			return this.par("500", e.getMessage(), data);
		} 
		
		//璁剧疆杩斿洖鍊糚arameter锛�
		par.setStatus("200");
		par.setMsg("success");
		par.setData(data); //鎴愬姛鏃跺�杩斿洖绌虹殑data鍊硷紱
		
		log.info("=> parse the key=>" + keys.get("UUID") + "success!");
		return par;
	}

	@Override
	public ParseParameter parserAndSolr(File file, Map<String, Object> keys,
			int absSum, SolrWrite sw) {

		return null;
	}

	/**
	 * 鍐欏叆鏁版嵁鍒癏Base涓紱
	 * @param tableName
	 * @param rowKey
	 * @param familyName
	 * @param mapMeta
	 * @throws IOException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void writeToHBase(String tableName, String rowKey, String familyName, Map<String, String> mapMeta) throws IOException{
		HBaseHelper hp = new HBaseHelper();
		
		//鍔犲叆鏁版嵁鍒板垪绨囦腑锛�
		List<ColumnFamily> lists = new ArrayList<ColumnFamily>();
		for(String key : mapMeta.keySet()){
			String value = mapMeta.get(key);
			if (value != null && !value.trim().equals("")) {
				ColumnFamily<String> col = new ColumnFamily<String>();
				col.setFamily(familyName);
				col.setQualifier(key);
				col.setValue(value); //Note : ??
				lists.add(col);
			}
		}
		
		//鎻掑叆鏁版嵁锛�
		hp.insert(tableName, rowKey, lists);
	}
	
	/**
	 * 瑙ｆ瀽鏂囦欢鐨勬暟鎹埌Hbase涓紱
	 * @param meta
	 * @return
	 */
	private Map<String, String> parseMeta(TikaMetaCon meta){
		
		//璇诲彇鏂囨。鐨勭被鍨嬶紱
		String doc = null;
		switch(meta.getSuffix()){
		    case OFFICE: doc = "office"; break;
		    case PDF : doc = "pdf"; break;
		    case TXT : doc = "txt"; break;
		    default : doc = "other" ;
		}
		
		//瑙ｆ瀽鏁版嵁骞跺啓鍏ュ埌Map涓紱
		Map<String, String> in = new HashMap<String, String>();
		in.put("doc", doc); //璁剧疆鏂囨。绫诲瀷锛�
		in.put("content", meta.getContent());
		in.put("path", meta.getPath());
		TikaMeta tm = meta.getMeta();
		if(meta.getSuffix() == Suffix.OFFICE || meta.getSuffix() == Suffix.PDF){ //濡傛灉鏂囦欢涓篛ffice PDF鏂囦欢锛�
					
			//Office PDF 鍏辨湁鐨勫睘鎬ч儴鍒嗭紱
			in.put(KeyMetadata.AUTHOR.getValue(), tm.getValue(KeyMetadata.AUTHOR));
			in.put(KeyMetadata.CONTENT_TYPE.getValue(), tm.getValue(KeyMetadata.CONTENT_TYPE));
			in.put(KeyMetadata.CREATION_DATE.getValue(), tm.getValue(KeyMetadata.CREATION_DATE));
			in.put(KeyMetadata.LAST_MODIFIED.getValue(), tm.getValue(KeyMetadata.LAST_MODIFIED));
			in.put(KeyMetadata.LAST_SAVEDATE.getValue(), tm.getValue(KeyMetadata.LAST_SAVEDATE));
			in.put(KeyMetadata.X_PARSED_BY.getValue(), tm.getValue(KeyMetadata.X_PARSED_BY));
			in.put(KeyMetadata.CREATOR.getValue(), tm.getValue(KeyMetadata.CREATOR));
			in.put(KeyMetadata.DATE.getValue(), tm.getValue(KeyMetadata.DATE));
			in.put(KeyMetadata.DC_CREATOR.getValue(), tm.getValue(KeyMetadata.DC_CREATOR));
			in.put(KeyMetadata.DC_FORMAT.getValue(), tm.getValue(KeyMetadata.DC_FORMAT));
			in.put(KeyMetadata.DCTERMS_CREATED.getValue(), tm.getValue(KeyMetadata.DCTERMS_CREATED));
			in.put(KeyMetadata.DCTERMS_MODIFIED.getValue(), tm.getValue(KeyMetadata.DCTERMS_MODIFIED));
			in.put(KeyMetadata.META_AUTHOR.getValue(), tm.getValue(KeyMetadata.META_AUTHOR));
			in.put(KeyMetadata.META_CREATION_DATE.getValue(), tm.getValue(KeyMetadata.META_CREATION_DATE));
			in.put(KeyMetadata.META_SAVE_DATE.getValue(), tm.getValue(KeyMetadata.META_SAVE_DATE));
			in.put(KeyMetadata.MODIFIED.getValue(), tm.getValue(KeyMetadata.MODIFIED));
			in.put(KeyMetadata.XMP_TPG_NPAGES.getValue(), tm.getValue(KeyMetadata.XMP_TPG_NPAGES));
			if(meta.getSuffix() == Suffix.PDF){
				
				//PDF 绉佹湁鐨勫睘鎬э紱
				in.put(KeyMetadata.PDF_CREATED.getValue(), tm.getValue(KeyMetadata.PDF_CREATED));
				in.put(KeyMetadata.PDF_PDF_VERSION.getValue(), tm.getValue(KeyMetadata.PDF_PDF_VERSION));
				in.put(KeyMetadata.PDF_ENCRYPTED.getValue(), tm.getValue(KeyMetadata.PDF_ENCRYPTED));
				in.put(KeyMetadata.PDF_PRODUCER.getValue(), tm.getValue(KeyMetadata.PDF_PRODUCER));
				in.put(KeyMetadata.PDF_XMP_CREATOR_TOOL.getValue(), tm.getValue(KeyMetadata.PDF_XMP_CREATOR_TOOL));
			}else if(meta.getSuffix() == Suffix.OFFICE){  //end if(meta.getSuffix()==Suffix.PDF)
				in.put(KeyMetadata.OFFICE_APPLICATION_NAME.getValue(), tm.getValue(KeyMetadata.OFFICE_APPLICATION_NAME));
				in.put(KeyMetadata.OFFICE_APPLICATION_VERSION.getValue(), tm.getValue(KeyMetadata.OFFICE_APPLICATION_VERSION));
				in.put(KeyMetadata.OFFICE_CATEGORY.getValue(), tm.getValue(KeyMetadata.OFFICE_CATEGORY));
				in.put(KeyMetadata.OFFICE_CHARACTER_COUNT_WITHSPACES.getValue(), tm.getValue(KeyMetadata.OFFICE_CHARACTER_COUNT_WITHSPACES));
				in.put(KeyMetadata.OFFICE_COMMENTS.getValue(), tm.getValue(KeyMetadata.OFFICE_COMMENTS));
				in.put(KeyMetadata.OFFICE_COMPANY.getValue(), tm.getValue(KeyMetadata.OFFICE_COMPANY));
				in.put(KeyMetadata.OFFICE_CONTENT_ENCODING.getValue(), tm.getValue(KeyMetadata.OFFICE_CONTENT_ENCODING));
				in.put(KeyMetadata.OFFICE_EDIT_TIME.getValue(), tm.getValue(KeyMetadata.OFFICE_EDIT_TIME));
				in.put(KeyMetadata.OFFICE_KEY_WORDS.getValue(), tm.getValue(KeyMetadata.OFFICE_KEY_WORDS));
				in.put(KeyMetadata.OFFICE_LAST_AUTHOR.getValue(), tm.getValue(KeyMetadata.OFFICE_LAST_AUTHOR));
				in.put(KeyMetadata.OFFICE_LINE_COUNT.getValue(), tm.getValue(KeyMetadata.OFFICE_LINE_COUNT));
				in.put(KeyMetadata.OFFICE_MANAGER.getValue(), tm.getValue(KeyMetadata.OFFICE_MANAGER));
				in.put(KeyMetadata.OFFICE_PAGE_COUNT.getValue(), tm.getValue(KeyMetadata.OFFICE_PAGE_COUNT));
				in.put(KeyMetadata.OFFICE_PARAGRAPH_COUNT.getValue(), tm.getValue(KeyMetadata.OFFICE_PARAGRAPH_COUNT));
				in.put(KeyMetadata.OFFICE_PRESENTATION_FORMAT.getValue(), tm.getValue(KeyMetadata.OFFICE_PRESENTATION_FORMAT));
				in.put(KeyMetadata.OFFICE_REVISION_NUMBER.getValue(), tm.getValue(KeyMetadata.OFFICE_REVISION_NUMBER));
				in.put(KeyMetadata.OFFICE_SLIDE_COUNT.getValue(), tm.getValue(KeyMetadata.OFFICE_SLIDE_COUNT));
				in.put(KeyMetadata.OFFICE_TEMPLATE.getValue(), tm.getValue(KeyMetadata.OFFICE_TEMPLATE));
				in.put(KeyMetadata.OFFICE_TOTA_LTIME.getValue(), tm.getValue(KeyMetadata.OFFICE_TOTA_LTIME));
				in.put(KeyMetadata.OFFICE_WORD_COUNT.getValue(), tm.getValue(KeyMetadata.OFFICE_WORD_COUNT));
				in.put(KeyMetadata.OFFICE_X_PARSEDBY.getValue(), tm.getValue(KeyMetadata.OFFICE_X_PARSEDBY));
				in.put(KeyMetadata.OFFICE_COMMENT.getValue(), tm.getValue(KeyMetadata.OFFICE_COMMENT));
				in.put(KeyMetadata.OFFICE_CP_CATEGORY.getValue(), tm.getValue(KeyMetadata.OFFICE_CP_CATEGORY));
				in.put(KeyMetadata.OFFICE_CP_REVISION.getValue(), tm.getValue(KeyMetadata.OFFICE_CP_REVISION));
				in.put(KeyMetadata.OFFICE_CP_SUBJECT.getValue(), tm.getValue(KeyMetadata.OFFICE_CP_SUBJECT));
				in.put(KeyMetadata.OFFICE_CUSTOM_KSOPRODUCTBUILDVER.getValue(), tm.getValue(KeyMetadata.OFFICE_CUSTOM_KSOPRODUCTBUILDVER));
				in.put(KeyMetadata.OFFICE_DC_PUBLISHER.getValue(), tm.getValue(KeyMetadata.OFFICE_DC_PUBLISHER));
				in.put(KeyMetadata.OFFICE_DC_SUBJECT.getValue(), tm.getValue(KeyMetadata.OFFICE_DC_SUBJECT));
				in.put(KeyMetadata.OFFICE_DC_TITLE.getValue(), tm.getValue(KeyMetadata.OFFICE_DC_TITLE));
				in.put(KeyMetadata.OFFICE_EDITING_CYCLES.getValue(), tm.getValue(KeyMetadata.OFFICE_EDITING_CYCLES));
				in.put(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_APPVERSION.getValue(), tm.getValue(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_APPVERSION));
				in.put(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_APPLICATION.getValue(), tm.getValue(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_APPLICATION));
				in.put(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_COMPANY.getValue(), tm.getValue(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_COMPANY));
				in.put(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_MANAGER.getValue(), tm.getValue(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_MANAGER));
				in.put(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_PRESENTATION_FORMAT.getValue(), tm.getValue(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_PRESENTATION_FORMAT));
				in.put(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_TEMPLATE.getValue(), tm.getValue(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_TEMPLATE));
				in.put(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_TOTAL_TIME.getValue(), tm.getValue(KeyMetadata.OFFICE_EXTENDED_PROPERTIES_TOTAL_TIME));
				in.put(KeyMetadata.OFFICE_GENERATOR.getValue(), tm.getValue(KeyMetadata.OFFICE_GENERATOR));
				in.put(KeyMetadata.OFFICE_INITIAL_CREATOR.getValue(), tm.getValue(KeyMetadata.OFFICE_INITIAL_CREATOR));
				in.put(KeyMetadata.OFFICE_META_CHARACTER_COUNT.getValue(), tm.getValue(KeyMetadata.OFFICE_META_CHARACTER_COUNT));
				in.put(KeyMetadata.OFFICE_META_CHARACTER_COUNT_WITHSPACES.getValue(), tm.getValue(KeyMetadata.OFFICE_META_CHARACTER_COUNT_WITHSPACES));
				in.put(KeyMetadata.OFFICE_META_INITIAL_AUTHOR.getValue(), tm.getValue(KeyMetadata.OFFICE_META_INITIAL_AUTHOR));
				in.put(KeyMetadata.OFFICE_META_KEYWORD.getValue(), tm.getValue(KeyMetadata.OFFICE_META_KEYWORD));
				in.put(KeyMetadata.OFFICE_META_LAST_AUTHOR.getValue(), tm.getValue(KeyMetadata.OFFICE_META_LAST_AUTHOR));
				in.put(KeyMetadata.OFFICE_META_LINE_COUNT.getValue(), tm.getValue(KeyMetadata.OFFICE_META_LINE_COUNT));
				in.put(KeyMetadata.OFFICE_META_PAGE_COUNT.getValue(), tm.getValue(KeyMetadata.OFFICE_META_PAGE_COUNT));
				in.put(KeyMetadata.OFFICE_META_PARAGRAPH_COUNT.getValue(), tm.getValue(KeyMetadata.OFFICE_META_PARAGRAPH_COUNT));
				in.put(KeyMetadata.OFFICE_META_SLIDE_COUNT.getValue(), tm.getValue(KeyMetadata.OFFICE_META_SLIDE_COUNT));
				in.put(KeyMetadata.OFFICE_META_WORD_COUNT.getValue(), tm.getValue(KeyMetadata.OFFICE_META_WORD_COUNT));
				in.put(KeyMetadata.OFFICE_NB_CHARACTER.getValue(), tm.getValue(KeyMetadata.OFFICE_NB_CHARACTER));
				in.put(KeyMetadata.OFFICE_NB_PAGE.getValue(), tm.getValue(KeyMetadata.OFFICE_NB_PAGE));
				in.put(KeyMetadata.OFFICE_NB_PARA.getValue(), tm.getValue(KeyMetadata.OFFICE_NB_PARA));
				in.put(KeyMetadata.OFFICE_NB_WORD.getValue(), tm.getValue(KeyMetadata.OFFICE_NB_WORD));
				in.put(KeyMetadata.OFFICE_PROTECTED.getValue(), tm.getValue(KeyMetadata.OFFICE_PROTECTED));
				in.put(KeyMetadata.OFFICE_PUBLISHER.getValue(), tm.getValue(KeyMetadata.OFFICE_PUBLISHER));
				in.put(KeyMetadata.OFFICE_SUBJECT.getValue(), tm.getValue(KeyMetadata.OFFICE_SUBJECT));
				in.put(KeyMetadata.OFFICE_TITLE.getValue(), tm.getValue(KeyMetadata.OFFICE_TITLE));
				in.put(KeyMetadata.OFFICE_W_COMMENTS.getValue(), tm.getValue(KeyMetadata.OFFICE_W_COMMENTS));
				in.put(KeyMetadata.OFFICE_XMPTPG_NPAGES.getValue(), tm.getValue(KeyMetadata.OFFICE_XMPTPG_NPAGES));
			} //end else if(meta.getSuffix() == Suffix.OFFICE)
			return in;
		} //if(meta.getSuffix() == Suffix.PDF)
		
		if(meta.getSuffix() == Suffix.TXT){
			in.put(KeyMetadata.TXT_CONTENTENCODING.getValue(), tm.getValue(KeyMetadata.TXT_CONTENTENCODING));
			in.put(KeyMetadata.TXT_CONTENTTYPE.getValue(), tm.getValue(KeyMetadata.TXT_CONTENTTYPE));
		}
		
		if(meta.getSuffix() == Suffix.OTHER){
		   //浠�箞閮戒笉鍋氾紱
		}
		
		return in;
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
