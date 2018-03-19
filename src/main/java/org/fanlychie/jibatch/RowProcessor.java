package org.fanlychie.jibatch;

import java.sql.PreparedStatement;

/**
 * 行记录处理器
 * Created by fanlychie on 2018/3/19.
 */
public interface RowProcessor {

    void process(PreparedStatement preparedStatement) throws Exception;

}