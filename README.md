### 分析用户活跃程度需要统计的数据
* **用户总的在线时长**
    * 指标名称
        * online_time_total
    * 指标含义
        * 单个用户访问的总时长，以小时或者天作为单位。
    * 计算公式    
    
    ```lua
        online_time_total = online_time_total + session_time
    ```
* **用户最近一周的在线时长** 
    * 指标名称
        * week_online_time_total
    * 指标含义
        * 单个用户的最近一周的会话总时长，用来统计最近一周的活跃度，以小时为单位。
    * 计算公式    
    
    ```lua
        week_online_time_total = week_online_time_total + session_time
    ```
* **用户最近一周的在线天数**
    * 指标名称
        * week_online_days
    * 指标含义
        * 用户最近一周的在线天数，在线天数越多，显示用户越活跃。
    * 计算公式    
    
    ```lua
        week_online_days = week_online_days + 1
    ```
* **最近一周的日均在线时长**
    * 指标名称
        * day_avg_online_time
    * 指标含义
        * 用户近一周平均每天的在线时长，平均值越大，说明该用户是资深用户。
    * 计算公式    
    
    ```lua
        day_avg_online_time = week_online_time_total / 7
    ```
* **用户最近一周的访问次数**
    * 指标名称
        * week_access_count
    * 指标含义
        * 用户近一周的访问次数。访问次数越多，表示用户更活跃。
    * 计算公式    
    
    ```lua
        if bytes_sent > 0 then
            week_access_count = week_access_count + 1
        end
    ```
* **用户最近一周的日均有效访问次数**
    * 指标名称
        * day_avg_access_count
    * 指标含义
        * 单个用户近期的、平均每天的有效访问次数，该值越大，表名用户越活跃。
    * 计算公式
    
    ```lua
        day_avg_access_count = week_access_count_total / 7
    ```
* **同一accessKey下，不同设备指纹使用的回源端口个数**
    * 指标名称
        * target_port_num
    * 指标含义
        * 在游戏大厅中，有很多游戏，其使用的tcp端口不同，如果同一fingerprint所使用的端口号较少, 则表示用户不够活跃
    * 计算公式    
    
    ```lua
        target_port_num = 从ES中查找出fingerprint使用的同一accessKey下的端口个数
    ```

### 用户活跃程度评价方法
* 用户活跃程度评价的各权重因子，权重由高至低暂定为以下顺序：
    * 最近一周的日均在线时长， **权重为 2**     
    
    ```lua
        weight_day_avg_online_time = 2
    ```
    * 最近一周的在线时长， **权重为 3**   
    
    ```lua
        weight_week_online_time_total = 2
    ```
    * 用户最近一周的在线天数， **权重为 2**    
    
    ```lua
        weight_week_online_days = 2
    ```
    * 用户总的在线时长， **权重为 10**    
    
    ```lua
        weight_online_time_total = 10
    ```
    * 最近一周的日均访问次数， **权重为 1**    
    
    ```lua
        weight_day_avg_access_count = 1
    ```
    * 最近一周的有效访问次数， **权重为 1**   
    
    ```lua
    weight_week_access_count = 1
    ```
    * 回源端口个数（游戏大厅），**权重为1**    
    
    ```lua
        weight_target_port_num = 1
    ```

* **用户活跃度评价算法**
    * 每个权重因子的满分**100**分，每一个项目得分情况按如下方法统计
        * 最近一周的日均在线时长   
        
        ```lua   
            day_avg_online_time_ratio = day_avg_online_time / DAY_AVG_ONLINE_TIME (DAY_AVG_ONLINE_TIME 为4小时)   
            if day_avg_online_time_ratio > 1 then   
                day_avg_online_time_ratio = 1   
            end
            score_day_avg_online_time = day_avg_online_time_ratio * 100    
        ```
        * 最近一周的在线时长   
        
        ```lua   
            WEEK_ONLINE_TIME = DAY_AVG_ONLINE_TIME * 7
            week_online_time_total_ratio = week_online_time_total / WEEK_ONLINE_TIME   
            if week_online_time_total_ratio > 1 then
                week_online_time_total_ratio = 1
            end
            score_week_online_time_total = week_online_time_total_ratio * 100
        ```
        * 最近一周的在线天数   
        
        ```lua
            score_week_online_days = week_online_days / 7 * 100
        ```
        * 用户总的在线时长   
        
        ```lua   
            ONLINE_TIME_TOTAL = DAY_AVG_ONLINE_TIME * (从起始记录当前日志到现在经过的天数)
            online_time_total_ratio = online_time_total / ONLINE_TIME_TOTAL
            if online_time_total_ratio > 1 then   
                online_time_total_ratio = 1   
            end
            score_online_time_total = online_time_total_ratio * 100
        ```
        * 用户最近一周的日均有效访问次数   
        
        ```lua   
            DAY_ACCESS_COUNT = 100
            day_avg_access_count_ratio = day_avg_access_count / DAY_ACCESS_COUNT   
            if day_avg_access_count_ratio > 1 then
                day_avg_access_count_ratio = 1
            end
            score_day_avg_access_count = day_avg_access_count_ratio * 100   
        ```   
        如果日均有效访问次数**大于等于100次**（es大数据统计出这个阈值），则得分为**100分**
        * 用户最近一周的有效访问次数    
        
        ```lua   
            WEEK_ACCESS_COUNT = 1000
            week_access_count_ratio = week_access_count / WEEK_ACCESS_COUNT
            if week_access_count_ratio > 1 then
                week_access_count_ratio = 1
            end
            score_week_access_count = week_access_count_ratio * 100   
        ```   
        如果最近一周的有效访问次数**大于等于1000次**（es大数据统计出这个阈值）， 则得分为**100分**
        * 回源端口个数   
        
        ```lua   
            TARGET_PORT_NUM = 4
            target_port_num_ratio = target_port_num / TARGET_PORT_NUM   
            if target_port_num_ratio > 1 then
                target_port_num_ratio = 1
            end
            score_target_port_num = target_port_num_ratio * 100   
        ```
    * 用户得分**score**    
        
        ```lua   
            score = score_day_avg_online_time * weight_day_avg_online_time +  
                    score_week_online_time_total * weight_week_online_time_total +   
                    score_week_online_days * weight_week_online_days +  
                    score_online_time_total * weight_online_time_total +  
                    score_day_avg_access_count * weight_day_avg_access_count +
                    score_week_access_count * weight_week_access_count +
                    score_target_port_num * weight_target_port_num   
        ```     
    
### 系统设计   

    * 系统dependencies
        
        * java >= 1.8.0
        * python >= 2.7
        * spark   

    * 系统流程图
    
        ![Alt-Text](https://note.youdao.com/yws/api/personal/file/2F79EFF04613414E85C9C9612C52BDEC?method=download&shareKey=994f84a501b6832ac055de2eb14e6dda)
