package com.ecnu.constants;

import com.ecnu.join.Join;

/**
 * 保存一些配置常量
 * @author ikroal
 * @date 2019-06-14
 * @version: 1.0.0
 */
public class ConfigurationKey {
    /**
     * 中心点路径 key 值，{@link com.ecnu.kmeans.KMeans}
     */
    public static final String CENTERS_PATH = "centersPath";

    /**
     * persons 表名 key 值，{@link Join}
     */
    public static final String PERSONS_NAME = "persons";

    /**
     * orders 表名 key 值，{@link Join}
     */
    public static final String ORDERS_NAME = "orders";
}
