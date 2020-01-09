package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.atguigu.config.ElasticSearchConfig;
import com.atguigu.pojo.User;
import com.atguigu.repository.UserRepository;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.jupiter.params.shadow.com.univocity.parsers.annotations.Parsed;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import javax.naming.Name;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class EsDemoTest {
    @Autowired
    private ElasticsearchRestTemplate restTemplate;
    @Autowired
    private UserRepository userRepository;
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void test(){
        this.restTemplate.createIndex(User.class);
        this.restTemplate.putMapping(User.class);
    }

    @Test
    public void testAdd(){
        //this.userRepository.save(new User(1l,"张三,看书听歌打代码",19,"123456"));
        List<User> users = new ArrayList<>();
        users.add(new User(1l, "柳岩", 18, "123456"));
        users.add(new User(2l, "范冰冰", 19, "123456"));
        users.add(new User(3l, "李冰冰", 21, "123456"));
        users.add(new User(4l, "锋哥", 21, "123456"));
        users.add(new User(5l, "小鹿", 22, "123456"));
        users.add(new User(6l, "韩红", 22, "123456"));
        this.userRepository.saveAll(users);
    }

    @Test
    public void testFind() {
        //this.userRepository.findAll().forEach(System.out::println);
        this.userRepository.findUsersByAgeBetween(20,22).forEach(System.out::println);
    }

    @Test
    public void testSearch() {
        //this.userRepository.search(QueryBuilders.matchQuery("name","柳岩")).forEach(System.out::println);
       /* Page<User> age = this.userRepository.search(QueryBuilders.rangeQuery("age").gt(20).lt(25), PageRequest.of(0, 2));
        System.out.println(age.getTotalElements());
        System.out.println(age.getTotalPages());
        age.getContent().forEach(System.out::println);*/

       //初始化自定义查询构建器
        //userRepository
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.matchQuery("name","冰冰").operator(Operator.AND));
        queryBuilder.withSort(SortBuilders.fieldSort("age").order(SortOrder.DESC));
        queryBuilder.withPageable(PageRequest.of(0,2));
        queryBuilder.withHighlightBuilder(new HighlightBuilder().field("name").preTags("<em>").postTags("</em>"));
        queryBuilder.addAggregation(AggregationBuilders.terms("ageAgg").field("age"));
        AggregatedPage<User> page = (AggregatedPage<User>) this.userRepository.search(queryBuilder.build());
        System.out.println(page.getTotalElements());
        System.out.println(page.getTotalPages());
        page.getContent().forEach(System.out::println);
        Terms terms = (Terms) page.getAggregation("ageAgg");
        terms.getBuckets().forEach(bucket -> {
            System.out.println(bucket.getKeyAsString());
        });

    }

    @Test
    public void testSearch2(){
        //初始化自定义查询构建器
        //restTemplate
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.matchQuery("name","冰冰").operator(Operator.AND));
        queryBuilder.withSort(SortBuilders.fieldSort("age").order(SortOrder.DESC));
        queryBuilder.withPageable(PageRequest.of(0,2));
        queryBuilder.withHighlightBuilder(new HighlightBuilder().field("name").preTags("<em>").postTags("</em>"));
        queryBuilder.addAggregation(AggregationBuilders.terms("ageAgg").field("age"));
        this.restTemplate.query(queryBuilder.build(),response -> {
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                String userJOSN = hit.getSourceAsString();
                User user = JSON.parseObject(userJOSN, User.class);
                System.out.println(user);

                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField name = highlightFields.get("name");
                user.setName(name.getFragments()[0].string());
                System.out.println(user);
            }
            Map<String, Aggregation> asMap = response.getAggregations().asMap();
            Terms ageAgg = (Terms) asMap.get("ageAgg");
            ageAgg.getBuckets().forEach(bucket -> System.out.println(bucket.getKeyAsString()));


            return null;
        });
    }

    @Test
    public void testRestHighLevelClient() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("name","冰冰").operator(Operator.AND));
        sourceBuilder.sort("age",SortOrder.DESC);
        sourceBuilder.from(0);
        sourceBuilder.size(2);
        sourceBuilder.highlighter(new HighlightBuilder().field("name").preTags("<em>").postTags("</em>"));
        sourceBuilder.aggregation(AggregationBuilders.terms("pwdAgg").field("password").subAggregation(
                AggregationBuilders.avg("ageAgg").field("age")
        ));

        SearchResponse response = this.restHighLevelClient.search(new SearchRequest(new String[]{"user"}, sourceBuilder), RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            String userJOSN = hit.getSourceAsString();
            User user = JSON.parseObject(userJOSN, User.class);
            System.out.println(user);

            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField name = highlightFields.get("name");
            user.setName(name.getFragments()[0].string());
            System.out.println(user);
        }
        Map<String, Aggregation> asMap = response.getAggregations().asMap();
        ParsedStringTerms pwdAgg = (ParsedStringTerms) asMap.get("pwdAgg");
        pwdAgg.getBuckets().forEach(bucket -> System.out.println(bucket.getKeyAsString()));

    }





}
