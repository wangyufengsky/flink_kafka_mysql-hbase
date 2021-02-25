package entity;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.sql.Date;

public class MysqlBean {
    private static final long serialVersionUID = -3614937227437805644L;
    @Getter @Setter
    private Long id;
    @Getter @Setter
    private Long cst_no;
    @Getter @Setter
    private String busi_type;
    @Getter @Setter
    private Date point_date ;
    @Getter @Setter
    private BigDecimal balance;
    @Getter @Setter
    private BigDecimal sum_amount;
    @Getter @Setter
    private BigDecimal profit_amount;


    public MysqlBean(Long cst_no, String busi_type, Date point_date, BigDecimal balance, BigDecimal sum_amount, BigDecimal profit_amount) {
        this.cst_no = cst_no;
        this.busi_type = busi_type;
        this.point_date = point_date;
        this.balance = balance;
        this.sum_amount = sum_amount;
        this.profit_amount = profit_amount;
    }


    @Override
    public String toString() {
        return "MysqlBean{" +
                "id=" + id +
                ", cst_no=" + cst_no +
                ", busi_type='" + busi_type + '\'' +
                ", point_date=" + point_date +
                ", balance=" + balance +
                ", sum_amount=" + sum_amount +
                ", profit_amount=" + profit_amount +
                '}';
    }


}
