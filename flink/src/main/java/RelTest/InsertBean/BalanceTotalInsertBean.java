package RelTest.InsertBean;

import entity.clearBean;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

public class BalanceTotalInsertBean {

    @Getter @Setter
    private String dataBase;
    @Getter @Setter
    private String table;
    @Getter @Setter
    private String dataBaseName;
    @Getter @Setter
    private String tableName;
    @Getter @Setter
    private List<clearBean> clearBeans;

    public BalanceTotalInsertBean(String dataBase, String table) {
        this.dataBase = dataBase;
        this.table = table;
        this.dataBaseName="db_"+dataBase;
        this.tableName="balance_total_"+table;
        this.clearBeans =new ArrayList<>();
    }

    public void addBean(clearBean bean){
        this.clearBeans.add(bean);
    }

    public void addBeans(List<clearBean> beans){
        this.clearBeans.addAll(beans);
    }

    public boolean isBelong(clearBean bean){
        return bean.getTable().equals(this.table) && bean.getDataBase().equals(this.dataBase);
    }

    public void clear(){
        this.clearBeans.clear();
    }
}