package com.atguigu.repository;

import com.atguigu.pojo.User;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface UserRepository extends ElasticsearchRepository<User,Long> {

    List<User> findUsersByAgeBetween(Integer a1,Integer a2);
}
