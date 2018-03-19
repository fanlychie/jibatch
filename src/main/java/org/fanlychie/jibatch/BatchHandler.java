package org.fanlychie.jibatch;

import org.fanlychie.jibatch.db.DatabaseConnection;
import org.fanlychie.jibatch.exception.RuntimeCastException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 批量处理器
 * Created by fanlychie on 2018/3/19.
 */
public class BatchHandler {

    /**
     * SQL语句
     */
    private String sql;

    /**
     * 数据库连接
     */
    private DatabaseConnection dbConn;

    /**
     * 行处理器
     */
    private RowProcessor processor;

    /**
     * 批量处理的线程数
     */
    private int threads = 10;

    /**
     * 批量处理事务提交的阀值
     */
    private int commitThreshold = 100;

    private Logger logger = LoggerFactory.getLogger(BatchHandler.class);

    public BatchHandler() {

    }

    /**
     * 创建对象
     *
     * @param threads         批量处理的线程数
     * @param commitThreshold 批量处理事务提交的阀值
     */
    public BatchHandler(int threads, int commitThreshold) {
        this.threads = threads;
        this.commitThreshold = commitThreshold;
    }

    /**
     * 执行批处理
     *
     * @param conn      数据库连接
     * @param sql       SQL语句
     * @param rows      插入的行数
     * @param processor 行处理器
     */
    public void executeBatch(DatabaseConnection conn, String sql, long rows, RowProcessor processor) {
        this.sql = sql;
        this.dbConn = conn;
        this.processor = processor;
        // 每条线程负责的行数
        long perthreadRows = rows / threads;
        if (perthreadRows < 1) {
            throw new IllegalArgumentException("for input rows=" + rows);
        }
        // 处理第N-1条线程
        for (int i = 1; i < threads; i++) {
            new ExecutorThread((i - 1) * perthreadRows + 1, i * perthreadRows).start();
        }
        // 处理最后一条线程, 最后一条线程处理剩余的所有行数据
        new ExecutorThread((threads - 1) * perthreadRows + 1, rows).start();
    }

    // 线程执行器
    private class ExecutorThread extends Thread {

        private long index;

        private long maxIndex;

        private ExecutorThread(long index, long maxIndex) {
            this.index = index;
            this.maxIndex = maxIndex;
        }

        @Override
        public void run() {
            Connection conn = dbConn.getConnection();
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = conn.prepareStatement(sql);
                for (long i = index; i <= maxIndex; i++) {
                    processor.process(preparedStatement);
                    preparedStatement.addBatch();
                    if (i % commitThreshold == 0) {
                        logger.info("committing");
                        preparedStatement.executeBatch();
                        conn.commit();
                        preparedStatement.clearBatch();
                    }
                }
                logger.info("committing");
                preparedStatement.executeBatch();
                conn.commit();
            } catch (Exception e) {
                throw new RuntimeCastException(e);
            } finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        throw new RuntimeCastException(e);
                    }
                }
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        throw new RuntimeCastException(e);
                    }
                }
            }
        }
    }

}