package entity;

import java.util.List;

public interface IBasicDao {

    void createTbaleIfNotExists();

    void creatIndex();

    void dropIndex();

    void repair();

    void dropTable();

    void truncateTable();

    List<OrderBean> select();


    void clearInsert(List<clearBean> order);

    void doBatchClear(List<clearBean> order);

    void InsertSnapshot(List<SnapshotBean> order);

    void doSingleSnapshotBean(List<SnapshotBean> order);

    void doBatchSnapshotBean(List<SnapshotBean> order);

    void doBatch(List<SnapshotBean> order);

    List<OrderBean> selectByUserId(Long id);

    void insert(List<OrderBean> order);

    void update(OrderBean order);

    void delete(Long id);

    String selectCount();

    void batchInsert(List<SnapshotBean> order);

    void createSnapshot();
}