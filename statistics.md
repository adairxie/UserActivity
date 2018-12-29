### 信誉等级分布统计接口

* GET方法，query_args表示传递的accesskey   
```/statistics?accesskey=<query_args>```

* 响应, JSON格式    
```
    -- 成功
    {
        "message": {
            "personal": 123016,     # 数字，表示某accesskey下的所有信誉等级为personal的设备数
            "good": 38681,
            "gaofang": 151452
        },
        "success": true             # 布尔型，表示请求是否成功     
    }
    
    -- 失败
    {
        "message": "accesskey not exists",
        "success": false
    }

```