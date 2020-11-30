package com.arijit.kafka.model;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@ToString
public class Customer {

    private Integer customerId;
    private String customerName;
}
