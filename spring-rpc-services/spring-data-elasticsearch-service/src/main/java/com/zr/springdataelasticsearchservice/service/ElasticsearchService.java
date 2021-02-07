package com.zr.springdataelasticsearchservice.service;

import com.zr.springdataelasticsearchservice.dto.ResultBean;

import java.util.List;
import java.util.Map;

public interface ElasticsearchService<T> {
    // 创建索引（测试）
    ResultBean createIndex(String index);

    // 删除索引(测试)
    ResultBean deleteIndex(String index);

    // 添加文档(测试)
    ResultBean addDoc(Class<T> tClass, String index);

    // 批量添加数据（测试）
    ResultBean addBulkDocs(List<T> list, String index, String type);

    // 查询数据（测试）
    Map<String, Object> search(String index, String desc);

    // 解析数据放入es中
    boolean parseContent(String keywords);

    // 分页查询
    List<Map<String, Object>> serachPage(String keyword, int pageNum, int pageSize);
}
