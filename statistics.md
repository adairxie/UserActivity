### 信誉等级分布统计接口

* GET请求 
* url
```/statistics/accesskey=<query_args>```

* 响应    
```
-- 成功
{
    "message": {
        "personal": 123016,
        "good": 38681,
        "gaofang": 151452
    },
    "success": true
}
```

```
-- 失败
{
    "message": "accesskey not exists",
    "success": false
}
```