package com.factory.deliveredcars.web;

import java.time.LocalDate;
import java.time.OffsetDateTime;

public record DeliveredCar(
        long id,
        String vin,
        String model,
        String color,
        String productionLine,
        String destination,
        LocalDate deliveryDate,
        OffsetDateTime eventTs
) {
}
