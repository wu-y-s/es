package com.example;

import com.alibaba.fastjson.JSON;
import com.example.entity.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Test
    void contextLoads() throws IOException {
        //创建新索引请求
        CreateIndexRequest request = new CreateIndexRequest("test2");
        //客户端执行请求,并获得响应
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse);
    }

    //获取索引请求
    @Test
    void isIndexExist() throws IOException {
        GetIndexRequest request = new GetIndexRequest("test1");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);

        System.out.println(exists);
    }


    //删除索引请求
    @Test
    void del() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("test2");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    //添加文档
    @Test
    void addDocument() throws IOException {
        User user = new User("wys", 22);
        IndexRequest request = new IndexRequest("test2");
        request.id("2").timeout(TimeValue.timeValueSeconds(1)).source(JSON.toJSONString(user), XContentType.JSON);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

    }

    //判断文档是否存在
    @Test
    void isDocumentExists() throws IOException {
        GetRequest request = new GetRequest("test2", "1");
        //不获取_source的上下文
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        if (exists) {
            GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
            System.out.println(getResponse);
            System.out.println(getResponse.getSourceAsString());
        }

    }

    //获取文档
    @Test
    void getDocument() throws IOException {
        GetRequest request = new GetRequest("test2", "1");
        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
        System.out.println(getResponse);
        System.out.println(getResponse.getSourceAsString());


    }

    //修改文档
    @Test
    void updateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("test2", "1");
        request.timeout(TimeValue.timeValueSeconds(1));
        User user = new User("ww", 21);
        request.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());
    }

    //删除文档
    @Test
    void delDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("test2", "2");
        request.timeout("1s");
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    //批量插入
    @Test
    void add() throws IOException {
        BulkRequest request = new BulkRequest();
        request.timeout("20s");
        ArrayList<User> list = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            //批量更新，删除一样
            request.add(new IndexRequest("test2").id("" + (i + 1)).source(JSON.toJSONString(list.get(i)), XContentType.JSON));
        }
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.hasFailures());//是否失败
    }

    //查
    @Test
    void search() throws IOException {
        SearchRequest searchRequest = new SearchRequest("test2");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //精确查询
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "ww");
        //匹配所有
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(termQueryBuilder);
        //分页
        searchSourceBuilder.from();
        searchSourceBuilder.size();
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        for(SearchHit documentfields : searchResponse.getHits().getHits()){
            System.out.println(documentfields.getSourceAsMap());
        }
    }

}
