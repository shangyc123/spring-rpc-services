package com.zr.springdataelasticsearchservice.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zr.springdataelasticsearchservice.constant.EsConst;
import com.zr.springdataelasticsearchservice.dto.ResultBean;
import com.zr.springdataelasticsearchservice.model.ContentModel;
import com.zr.springdataelasticsearchservice.model.UserModel;
import com.zr.springdataelasticsearchservice.service.ElasticsearchService;
import com.zr.springdataelasticsearchservice.utils.HtmlParseUtil;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticsearchImpl<T> implements ElasticsearchService<T> {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public ElasticsearchImpl(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /*
    try {

    }catch (Exception e){

    }
    */
    @Override
    public ResultBean createIndex(String index) {
        boolean acknowledged = false;
        try {
            CreateIndexRequest indexRequest = new CreateIndexRequest(index);
            CreateIndexResponse indexResponse = restHighLevelClient.indices()
                    .create(indexRequest, RequestOptions.DEFAULT);
            acknowledged = indexResponse.isAcknowledged();
        } catch (Exception e) {
            log.error("createIndex error");
        }
        return acknowledged ? ResultBean.success() : ResultBean.fail();
    }

    @Override
    public ResultBean deleteIndex(String index) {
        boolean acknowledged = false;
        try {
            DeleteIndexRequest indexRequest = new DeleteIndexRequest(index);
            AcknowledgedResponse delete = restHighLevelClient.indices()
                    .delete(indexRequest, RequestOptions.DEFAULT);
            acknowledged = delete.isAcknowledged();
        } catch (Exception e) {
            log.error("deleteIndex error");
        }
        return acknowledged ? ResultBean.success() : ResultBean.fail();
    }

    @Override
    public ResultBean addDoc(Class<T> tClass, String index) {
        int status = 0;
        try {
            IndexRequest request = new IndexRequest(index);
            request.id("1");   // 设置id
            request.type("my_tbl1");  // 设置type
            request.timeout(TimeValue.timeValueSeconds(1));

            // 数据存储
            request.source(JSONObject.toJSONString(tClass), XContentType.JSON);
            IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
            status = response.status().getStatus();

        }catch (Exception e){
            log.error("addDoc error");
        }
        return (status == 200) ? ResultBean.success() : ResultBean.fail() ;
    }

    @Override
    public ResultBean addBulkDocs(List<T> list, String index, String type) {
        boolean fail = true;
        try {
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.timeout(TimeValue.timeValueSeconds(10L));

            for (int i=0; i < list.size(); i++) {
                bulkRequest.add(
                        new IndexRequest(index)
                                .id(""+(i+1))
                                .type(type)  // 7以后，type可以省略
                                .source(JSONObject.toJSONString(list.get(i)), XContentType.JSON)
                );
            }
            BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            fail = bulkResponse.hasFailures();

        }catch (Exception e){
            log.error("bulkRequest error");
        }
        return fail ? ResultBean.fail() : ResultBean.success();
    }

    @Override
    public Map<String, Object> search(String index, String keyword) {
        Map<String, Object> sourceAsMap = null;
        try {
            SearchRequest searchRequest = new SearchRequest(index);
            // 构造搜索条件
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            // 查询条件
            MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("keyword", keyword);
            sourceBuilder.query(matchQuery)
                    .timeout(TimeValue.timeValueSeconds(60));
            searchRequest.source(sourceBuilder);

            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            SearchHit[] hits = searchResponse.getHits().getHits();
            // 构造返回值
            for (SearchHit hit : hits) {
                sourceAsMap  = hit.getSourceAsMap();
            }

//            List<UserModel> userModels = Arrays.stream(hits).map((hit) -> {
//                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
//                UserModel user = new UserModel();
//                user.setName((String) sourceAsMap.get("name"));
//                user.setAge((Integer) sourceAsMap.get("age"));
//                user.setDesc((String) sourceAsMap.get("desc"));
//                user.setEntry_time((String) sourceAsMap.get("experience"));
//                user.setLevel((String) sourceAsMap.get("level"));
//                user.setEntry_time((String) sourceAsMap.get("entry_time"));
//                return user;
//            }).collect(Collectors.toList());
        }catch (Exception e){
            log.error("search error");
        }
        return sourceAsMap;
    }


    // 1、解析数据放入es中
    @Override
    public boolean parseContent(String keywords) {
        boolean fail = true;
        try {
            List<ContentModel> parseHtml = HtmlParseUtil.parseHtml(keywords);
            // 把查询的数据放入 es 中
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.timeout(TimeValue.timeValueSeconds(2));

            parseHtml.forEach((c)-> {
                bulkRequest.add(new IndexRequest(EsConst.INDEX_NAME)
                        .source(JSON.toJSONString(c), XContentType.JSON));
            });

            BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            fail = bulkResponse.hasFailures();
        }catch (Exception e){
            log.error("parseContent error");
        }
        return !fail;
    }

    @Override
    public List<Map<String, Object>> serachPage(String keyword, int pageNum, int pageSize) {

        if (pageNum <= 1) {
            pageNum = 1;
        }
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            // 条件查询
            SearchRequest searchRequest = new SearchRequest(EsConst.INDEX_NAME);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            // 分页
            sourceBuilder.from(pageNum);
            sourceBuilder.size(pageSize);

            //精准匹配
            TermQueryBuilder termQuery = QueryBuilders.termQuery("title", keyword);
            sourceBuilder.query(termQuery);
            sourceBuilder.timeout(TimeValue.timeValueSeconds(50));
            // 高亮
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightBuilder.field("title");
            highlightBuilder.requireFieldMatch(false); // 是否需要多个高亮
            highlightBuilder.preTags("<span style='color:red'>");
            highlightBuilder.postTags("</span>");
            sourceBuilder.highlighter(highlightBuilder);
            // 执行搜索，分页
            searchRequest.source(sourceBuilder);

            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 解析结果，

            for (SearchHit hit : searchResponse.getHits().getHits()){
                // 解析高亮字段
                Map<String, HighlightField> highlightFields =
                        hit.getHighlightFields();
                HighlightField title = highlightFields.get("title");
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                if (title != null) { // 解析高亮,替换高亮字段
                    Text[] fragments = title.fragments();
                    String n_title = "";
                    for (Text t : fragments) {
                        n_title += t;
                    }
                    sourceAsMap.put("title", n_title); // 替换原理的高亮字段
                }
                list.add(sourceAsMap);
            }
        }catch (Exception e){
            log.error("parseContent error");
        }
        return list;
    }


}

