package org.fanlychie.jibatch;

import java.sql.PreparedStatement;

/**
 * 行记录处理器
 * Created by fanlychie on 2018/3/19.
 */
public interface RowProcessor {

    /**
     * 处理行数据
     *
     * @param preparedStatement PreparedStatement
     * @param row               当前的行号
     * @throws Exception
     */
    void process(PreparedStatement preparedStatement, int row) throws Exception;

}