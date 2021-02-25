package entity;

import lombok.*;

import java.math.BigDecimal;

@ToString @Builder @AllArgsConstructor
public class DetailBean {
    @Getter @Setter
    private String custId;
    @Getter @Setter
    private String busiCode;
    @Getter @Setter
    private String busiAmount;
    @Getter @Setter
    private String clearAmount;
    @Getter @Setter
    private String remark;
    @Getter @Setter
    private String date;
    @Getter @Setter
    private String time;
    @Getter @Setter
    private String evenId;
    @Getter @Setter
    private String rowKey;


}
