package test;

import java.util.List;

import junit.framework.TestCase;

import com.techvalley.parse.common.QueryParameter;
import com.techvalley.parse.common.SolrQuery;
import com.techvalley.parse.common.impl.SolrQueryImp;

public class SolrQueryTest extends TestCase{

	private SolrQuery query;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		query = new SolrQueryImp();
		
	}

	public void testQuery(){
		
		//正确测试;
		List<QueryParameter> list01 = query.query("人 小米  中国", 10, 1);
		System.out.println("正常=>" + list01.size());
		
		//分页越界；
		List<QueryParameter> list02 = query.query("项目", 10, -12);
		System.out.println("分页下越界=>" + list02.size());
		List<QueryParameter> list03 = query.query("项目", 10, 200);
		System.out.println("分页上越界=>" + list03.size());
		
		//查无此人
		List<QueryParameter> list04 = query.query("weijweio", 10, 0);
		System.out.println("查无此人=>" + list04.size());
		
		//null测试;
		List<QueryParameter> list05 = query.query(null, 10, 1);
		System.out.println("null=>" + list05.size());
	}
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	
}
