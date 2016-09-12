package fr.jetoile.hadoopunit.sample;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;


@Getter
@Setter
public class Order implements Serializable {

    private String orderid;
    private String clientid;
    private String creationtime;
    private String side;
    private String quantity;

    private String ordertype;

}
