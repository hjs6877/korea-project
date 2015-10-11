package kr.ac.korea;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by ideapad on 2015-10-11.
 */
class LineItem implements WritableComparable<LineItem> {
    private String orderkey;
    private String linenumber;
    private double quantity;
    private double discount;
    private double tax;


    public void set(String orderkey, String linenumber, double quantity, double discount, double tax){
        this.orderkey   = orderkey;
        this.linenumber = linenumber;
        this.quantity   = quantity;
        this.discount   = discount;
        this.tax        = tax;
    }
    public void write(DataOutput output) throws IOException {
        output.writeUTF(orderkey);
        output.writeUTF(linenumber);
        output.writeDouble(quantity);
        output.writeDouble(discount);
        output.writeDouble(tax);
    }

    public void readFields(DataInput input) throws IOException {
        orderkey    = input.readUTF();
        linenumber  = input.readUTF();
        quantity    = input.readDouble();
        discount    = input.readDouble();
        tax         = input.readDouble();
    }

    public int compareTo(LineItem o) {
        return 0;
    }

    public String getOrderkey() {
        return orderkey;
    }

    public void setOrderkey(String orderkey) {
        this.orderkey = orderkey;
    }

    public String getLinenumber() {
        return linenumber;
    }

    public void setLinenumber(String linenumber) {
        this.linenumber = linenumber;
    }

    public double getQuantity() {
        return quantity;
    }

    public void setQuantity(double quantity) {
        this.quantity = quantity;
    }

    public double getDiscount() {
        return discount;
    }

    public void setDiscount(double discount) {
        this.discount = discount;
    }

    public double getTax() {
        return tax;
    }

    public void setTax(double tax) {
        this.tax = tax;
    }


}
