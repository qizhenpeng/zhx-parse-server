package test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Resource;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.techvalley.parse.service.TikaSolrService;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath:spring-mvc-servlet.xml"})
public class TikaSolrServiceTest {

	@Resource(name = "tikaSolrService")
	private TikaSolrService ts;
	
	private List<Path> paths;
	
	public TikaSolrServiceTest(){
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
	
	@Test
	public void testSubmit(){
		Map<String, String> maps = new HashMap<String, String>();
		/**
		 * 正常测试；
		 */
		for(int i=0; i < this.paths.size() - 2; i++){
			Map<String, Object> keys = new HashMap<String, Object>();
			keys.put("path", paths.get(i).toUri().toString());
			keys.put("rowKey", UUID.randomUUID().toString());
			keys.put("UUID", UUID.randomUUID().toString());
			String tn = UUID.randomUUID().toString();
			keys.put("tn", tn);
			maps.put(keys.get("path").toString(), tn);
			ts.submit(paths.get(i), keys, 10, keys.get("rowKey").toString());
			//System.out.println(ts.getStatus(tn));
		}
		Map<String, Object> keys = new HashMap<String, Object>();
		keys.put("path", paths.get(5).toUri().toString());
		keys.put("rowKey", UUID.randomUUID().toString());
		keys.put("UUID", UUID.randomUUID().toString());
		String tn = UUID.randomUUID().toString();
		keys.put("tn", tn);
		maps.put(keys.get("path").toString(), tn);
		ts.submit(paths.get(5), keys, 10, keys.get("rowKey").toString());
		//System.out.println(ts.getStatus(tn).getData());
		for(String key : maps.keySet()){
			System.out.println(key + "\t" + maps.get(key).toString() + "\t" + ts.getStatus(maps.get(key).toString()));
		}
	}
}
