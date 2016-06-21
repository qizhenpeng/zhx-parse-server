package test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.techvalley.parse.common.ParseParameter;
import com.techvalley.parse.common.ParserServer;
import com.techvalley.parse.common.ParserServerConfig;
import com.techvalley.parse.common.SolrWrite;
import com.techvalley.search.exception.RowKeyTypeException;
import com.techvalley.search.hbase.ado.DataTable;
import com.techvalley.search.hbase.core.HBaseHelper;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath:spring-mvc-servlet.xml"})
public class ParserServiceImpTest {

	@Resource(name="parserServer")
	private ParserServer pser;
	
	@Resource(name="solrWrite")
	private SolrWrite sw;
	
	private int absSum = ParserServerConfig.ABSTRACT_SUM;
	
	private List<Path> paths ;
	
	public ParserServiceImpTest(){
		
		Path baseHdfs = new Path("/tika/testdata/tika");
		this.paths = Arrays.asList(new Path[]{
			new Path(baseHdfs, "excel97_2003_xls.xls"),
			new Path(baseHdfs, "word2007_docx.docx"),
			new Path(baseHdfs, "ppt_2007_ppt.pptx"),
			new Path(baseHdfs, "testComment.pdf"),
			new Path(baseHdfs, "txttest.txt"),
			new Path(baseHdfs, "jowieji.ppt"), //文件不存在；
			new Path(baseHdfs, "defaultStopWords.txt") //没有读取的权限；
		});
		
	}
	
	//@Test
	public void testparseHbaseSolr(){
		Map<String, Object> keys = new HashMap<String, Object>();
		ParseParameter par = null;
		
		String[] hbase = new String[]{
				ParserServerConfig.HBASE_TABLE_NAME, null, ParserServerConfig.HBASE_FAMILY
		};
		
		//xls
		hbase[1] = "row_key_excel_97_2003_xls";
		par = pser.parserHBaseSolr(paths.get(0), keys, absSum, sw, hbase);
		System.out.println(par.getStatus() + "\t" + par.getMsg());
		TestCase.assertTrue(par.getStatus() == "200");
		
		//pdf
		hbase[1] = "testComment.pdf";
		par = pser.parserHBaseSolr(paths.get(3), keys, absSum, sw, hbase);
		System.out.println(par.getStatus() + "\t" + par.getMsg());
		TestCase.assertTrue(par.getStatus() == "200");
		
		//txt
		hbase[1] = "txttest.txt";
		par = pser.parserHBaseSolr(paths.get(4), keys, absSum, sw, hbase);
		System.out.println(par.getStatus() + "\t" + par.getMsg());
		TestCase.assertTrue(par.getStatus() == "200");
		
		hbase[1] = "jowieji.ppt";
		par = pser.parserHBaseSolr(paths.get(5), keys, absSum, sw, hbase);
		System.out.println(par.getStatus() + "\t" + par.getMsg());
		TestCase.assertTrue(par.getStatus() == "500");
		System.out.println(hbase[2] + "\t" + par.getMsg() + par.getData().toString());
		
		hbase[1] = "defaultStopWords.txt";
		par = pser.parserHBaseSolr(paths.get(6), keys, absSum, sw, hbase);
		TestCase.assertTrue(par.getStatus() == "500");
		System.out.println(hbase[2] + "\t" + par.getMsg() + par.getData().toString());
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testHbase(){
		HBaseHelper hb = new HBaseHelper();
		try {
			DataTable dt = hb.scan("ztb_info", null, null, null, String.class);
			List<Result>  listRows = dt.getRowList();
			for(Result str : listRows){
				System.out.println(Bytes.toString(str.getRow()));
			}
		} catch (IOException | RowKeyTypeException e) {
			e.printStackTrace();
		}
	}
}
