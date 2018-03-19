package org.fanlychie.jibatch.db;

import java.sql.Connection;

/**
 * 数据库连接
 * Created by fanlychie on 2018/3/19.
 */
public interface DatabaseConnection {

    /**
     * 获取数据库连接
     *
     * @return 返回一个新的数据库连接
     */
    Connection getConnection();

}