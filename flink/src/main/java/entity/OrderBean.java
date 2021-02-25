package entity;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Objects;

public class OrderBean implements Serializable {
    private static final long serialVersionUID = -3614937227437805644L;
    @Getter @Setter
    private Long orderId;
    @Getter @Setter
    private Long userId;
    @Getter @Setter
    private String details;

    public OrderBean() {
    }

    public OrderBean(Long userId, String details) {
        this.userId = userId;
        this.details = details;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OrderBean)) {
            return false;
        }
        OrderBean order = (OrderBean) o;
        return Objects.equals(orderId, order.orderId) && Objects.equals(userId, order.userId) && Objects.equals(details, order.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, userId, details);
    }

    @Override
    public String toString() {
        return "Order{" + "orderId=" + orderId + ", userId=" + userId + ", details='" + details + '\'' + '}';
    }
}
