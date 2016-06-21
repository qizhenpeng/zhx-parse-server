package test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.techvalley.parse.common.SolrWrite;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath:spring-mvc-servlet.xml"})
public class SolrWriteTest {
	
	@Resource(name="solrWrite")
	private SolrWrite sw;

	@Test
	public void testSolr(){
		Map<String, String> keys = new HashMap<String, String>();
		keys.put("fasm", "test");
		keys.put("UUID", "DJJSODIOWEJ");
		try {
			sw.write(keys);
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	//@Test
	public void testSolr1() throws SolrServerException, IOException{
		final String zkHost = "S00062:2181";
		final String defaultCollection = "zbt";
		final int zkClientTimeout = 20000;
		final int zkConnectTimeout = 1000;
		CloudSolrServer cl = new CloudSolrServer(zkHost);
		System.out.println("The Cloud SolrServer Instance has benn created!");
		cl.setDefaultCollection(defaultCollection);
		cl.setZkClientTimeout(zkClientTimeout);
		cl.setZkConnectTimeout(zkConnectTimeout);
		SolrInputDocument in = new SolrInputDocument();
		in.addField("UUID", "jowiejwieo");
		in.addField("fasm", "jtejoiw");
		cl.add(in);
		
	}
}
