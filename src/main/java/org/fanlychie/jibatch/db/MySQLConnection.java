package org.fanlychie.jibatch.db;

import org.fanlychie.jibatch.exception.RuntimeCastException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * MYSQL
 * Created by fanlychie on 2018/3/19.
 */
public class MySQLConnection implements DatabaseConnection {

    /**
     * 数据库连接地址
     */
    private String url;

    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    private Logger logger = LoggerFactory.getLogger(MySQLConnection.class);

    /**
     * 创建MYSQL连接对象
     *
     * @param url      数据库连接地址
     * @param username 用户名
     * @param password 密码
     */
    public MySQLConnection(String url, String username, String password) {
        this.username = username;
        this.password = password;
        StringBuilder urlBuilder = new StringBuilder(url);
        addParam(urlBuilder, "autoReconnect", "true");
        addParam(urlBuilder, "useUnicode", "true");
        addParam(urlBuilder, "characterEncoding", "utf-8");
        addParam(urlBuilder, "useServerPrepStmts", "false");
        addParam(urlBuilder, "rewriteBatchedStatements", "true");
        this.url = urlBuilder.toString();
        logger.info(this.url);
    }

    /**
     * 获取数据库连接
     *
     * @return 返回一个新的数据库连接
     */
    public Connection getConnection() {
        try {
            Connection conn = DriverManager.getConnection(url, username, password);
            conn.setAutoCommit(false);
            return conn;
        } catch (Throwable e) {
            throw new RuntimeCastException(e);
        }
    }

    private void addParam(StringBuilder url, String name, String value) {
        if (url.indexOf(name) == -1) {
            if (url.indexOf("?") == -1) {
                url.append("?");
            } else {
                url.append("&");
            }
            url.append(name).append("=").append(value);
        }
    }

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeCastException(e);
        }
    }

}