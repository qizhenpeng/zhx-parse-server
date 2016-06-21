package com.techvalley.parse.controller;

import com.techvalley.parse.common.ParseParameter;
import com.techvalley.parse.common.ParserServerConfig;
import com.techvalley.parse.common.QueryParameter;
import com.techvalley.parse.common.QueryParams;
import com.techvalley.parse.common.TikaSolrParams;
import com.techvalley.parse.service.TikaSolrService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Resource;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping({"/tika"})
public class TikaSolrController
{

  @Resource(name="tikaSolrService")
  private TikaSolrService ts;

  @RequestMapping(value={"post"}, method={org.springframework.web.bind.annotation.RequestMethod.POST}, produces={"application/json;charset=utf-8"})
  @ResponseBody
  public ParseParameter submit(@RequestBody TikaSolrParams tika)
  {
    if (!isNull(tika)) {
      ParseParameter par = new ParseParameter();
      par.setStatus("500");
      par.setMsg("failed");
      return par;
    }

    Map params = tika.getKeys();
    params.put("tn", tika.getTn());
    params.put("rowKey", tika.getRowKey());
    params.put("path", tika.getPath());
    params.put("UUID", tika.getUserId());

    Path path = new Path(params.get("path").toString());
    this.ts.submit(path, params, ParserServerConfig.ABSTRACT_SUM, params.get("rowKey").toString());

    return null;
  }
  @RequestMapping(value={"delete"}, method={org.springframework.web.bind.annotation.RequestMethod.GET}, produces={"application/json;charset=utf-8"})
  @ResponseBody
  public Map<String, ParseParameter> delete(@RequestParam String uuid) {
    List uuidss = createList(uuid);
    if ((uuidss == null) || (uuidss.size() <= 0)) {
      return new HashMap();
    }
    return this.ts.delete(uuidss);
  }
  @RequestMapping(value={"get/{key}"}, method={org.springframework.web.bind.annotation.RequestMethod.GET}, produces={"application/json;charset=utf-8"})
  @ResponseBody
  public ParseParameter getStatus(@PathVariable String key) {
    if ((key == null) || ("".equals(key))) {
      ParseParameter par = new ParseParameter();
      par.setStatus("500");
      par.setMsg("the key don't equal null or '' !");
      par.setData(null);
      return par;
    }
    return this.ts.getStatus(key);
  }
  @RequestMapping(value={"query"}, method={org.springframework.web.bind.annotation.RequestMethod.GET}, produces={"application/json;charset=utf8"})
  @ResponseBody
  public QueryParams query(@RequestParam String key, @RequestParam int pageSize, @RequestParam int curPage) {
    if ("".equals(key)) {
      key = null;
    }
    List lists = this.ts.query(key, pageSize, curPage);
    if ((lists == null) || (lists.size() == 0)) {
      return new QueryParams(0, lists);
    }
    return new QueryParams(((QueryParameter)lists.get(0)).getSum(), lists);
  }

  @RequestMapping(value={"hbase/query"}, method={org.springframework.web.bind.annotation.RequestMethod.GET}, produces={"application/json;charset=utf-8"})
  @ResponseBody
  public Map<String, String> query(@RequestParam String tableName, @RequestParam String uuid) {
    Map maps = new HashMap();

    if ((tableName == null) || ("".equals(tableName)) || ("null".equals(tableName))) return maps;
    if ((uuid == null) || ("".equals(uuid)) || ("null".equals(uuid))) return maps;

    maps = this.ts.query(tableName, uuid);
    return maps;
  }

  private List<String> createList(String strs) {
    List uuids = new ArrayList();
    if ((strs == null) || ("".equals(strs)) || ("null".equals(strs))) {
      return uuids;
    }
    String[] arrays = strs.split(",");
    for (String str : arrays) {
      if ((str != null) && (!"".equals(str))) {
        uuids.add(str.trim());
      }
    }

    return uuids;
  }

  private boolean isNull(TikaSolrParams tika) {
    if (tika == null) {
      return false;
    }

    if ((tika.getTn() == null) || ("".equals(tika.getTn())))
      return false;
    if ((tika.getUserId() == null) || ("".equals(tika.getUserId())))
      return false;
    if ((tika.getPath() == null) || ("".equals(tika.getPath())))
      return false;
    if ((tika.getRowKey() == null) || ("".equals(tika.getRowKey())))
      return false;
    if (tika.getKeys() == null) {
      return false;
    }

    return true;
  }
}