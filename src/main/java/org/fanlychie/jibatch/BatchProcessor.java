package org.fanlychie.jibatch;

import org.fanlychie.jibatch.db.DatabaseConnection;
import org.fanlychie.jibatch.exception.RuntimeCastException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * 批量处理器
 * Created by fanlychie on 2018/3/19.
 */
public class BatchProcessor {

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
    private int threads;

    /**
     * 批量处理事务提交的阀值
     */
    private int commitThreshold;

    private NumberFormat threadNameFormat;

    private Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    public BatchProcessor() {
        this(10, 1000);
    }

    /**
     * 创建对象
     *
     * @param threads         批量处理的线程数
     * @param commitThreshold 批量处理事务提交的阀值
     */
    public BatchProcessor(int threads, int commitThreshold) {
        this.threads = threads;
        this.commitThreshold = commitThreshold;
        this.threadNameFormat = getNumberFormat("Thread-", threads);
    }

    /**
     * 执行批处理
     *
     * @param conn      数据库连接
     * @param sql       SQL语句
     * @param rows      插入的行数
     * @param processor 行处理器
     */
    public void executeBatch(DatabaseConnection conn, String sql, int rows, RowProcessor processor) {
        this.sql = sql;
        this.dbConn = conn;
        this.processor = processor;
        // 每条线程负责的行数
        int perthreadRows = rows / threads;
        if (perthreadRows < 1) {
            throw new IllegalArgumentException("For input rows=" + rows);
        }
        // 处理第N-1条线程
        for (int i = 1; i < threads; i++) {
            new ExecutorThread(i, (i - 1) * perthreadRows + 1, i * perthreadRows).start();
        }
        // 处理最后一条线程, 最后一条线程处理剩余的所有行数据
        new ExecutorThread(threads, (threads - 1) * perthreadRows + 1, rows).start();
    }

    // 数字格式化
    private NumberFormat getNumberFormat(String prefix, long number) {
        String formatName = (prefix == null ? "" : prefix);
        do {
            formatName += "0";
        } while ((number /= 10) != 0);
        return new DecimalFormat(formatName);
    }

    // 线程执行器
    private class ExecutorThread extends Thread {

        /**
         * 起始索引
         */
        private int index;

        /**
         * 最大索引
         */
        private int maxIndex;

        private ExecutorThread(int threadIndex, int index, int maxIndex) {
            super(threadNameFormat.format(threadIndex));
            this.index = index;
            this.maxIndex = maxIndex;
        }

        @Override
        public void run() {
            long count = index;
            Connection conn = dbConn.getConnection();
            PreparedStatement preparedStatement = null;
            logger.info("Start processing [{}, {}]", index, maxIndex);
            try {
                preparedStatement = conn.prepareStatement(sql);
                for (int i = index; i <= maxIndex; i++) {
                    processor.process(preparedStatement, i);
                    preparedStatement.addBatch();
                    if (i % commitThreshold == 0) {
                        logger.info("Committing [{}, {}]", count, i);
                        count += commitThreshold;
                        preparedStatement.executeBatch();
                        conn.commit();
                        preparedStatement.clearBatch();
                    }
                }
                logger.info("Committing [{}, {}]", count, maxIndex);
                preparedStatement.executeBatch();
                conn.commit();
                logger.info("Finish [{}, {}]", index, maxIndex);
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