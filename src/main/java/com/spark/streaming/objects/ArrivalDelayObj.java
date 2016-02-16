package com.spark.streaming.objects;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Created by shubham.kankaria on 16/02/16.
 */
@Getter
@Setter
@AllArgsConstructor
public class ArrivalDelayObj implements Serializable {

    private String airlineId;

    private Double arrDelayMinutes;
}
