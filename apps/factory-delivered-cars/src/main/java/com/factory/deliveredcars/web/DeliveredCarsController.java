package com.factory.deliveredcars.web;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class DeliveredCarsController {

    private static final int MAX_PAGE_SIZE = 500;

    private final DeliveredCarRepository repository;

    @Value("${cars.page-size:100}")
    private int pageSize;

    public DeliveredCarsController(DeliveredCarRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/")
    public String index(
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", required = false) Integer size,
            @RequestParam(name = "model", required = false) String modelFilter,
            @RequestParam(name = "destination", required = false) String destinationFilter,
            @RequestParam(name = "deliveryDate", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate deliveryDateFilter,
            Model model
    ) {
        QueryOptions options = toQueryOptions(page, size);
        List<DeliveredCar> cars = repository.findPage(
                modelFilter,
                destinationFilter,
                deliveryDateFilter,
                options.page(),
                options.size()
        );
        int totalItems = repository.count(modelFilter, destinationFilter, deliveryDateFilter);
        int totalPages = totalItems == 0 ? 0 : (int) Math.ceil(totalItems / (double) options.size());

        model.addAttribute("cars", cars);
        model.addAttribute("page", options.page());
        model.addAttribute("size", options.size());
        model.addAttribute("totalItems", totalItems);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("modelFilter", emptyIfNull(modelFilter));
        model.addAttribute("destinationFilter", emptyIfNull(destinationFilter));
        model.addAttribute("deliveryDateFilter", deliveryDateFilter == null ? "" : deliveryDateFilter.toString());
        return "index";
    }

    @GetMapping("/api/delivered-cars")
    @ResponseBody
    public DeliveredCarsResponse deliveredCarsApi(
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", required = false) Integer size,
            @RequestParam(name = "model", required = false) String modelFilter,
            @RequestParam(name = "destination", required = false) String destinationFilter,
            @RequestParam(name = "deliveryDate", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate deliveryDateFilter
    ) {
        QueryOptions options = toQueryOptions(page, size);
        List<DeliveredCar> cars = repository.findPage(
                modelFilter,
                destinationFilter,
                deliveryDateFilter,
                options.page(),
                options.size()
        );
        int totalItems = repository.count(modelFilter, destinationFilter, deliveryDateFilter);
        int totalPages = totalItems == 0 ? 0 : (int) Math.ceil(totalItems / (double) options.size());

        return new DeliveredCarsResponse(
                cars,
                options.page(),
                options.size(),
                totalItems,
                totalPages,
                emptyIfNull(modelFilter),
                emptyIfNull(destinationFilter),
                deliveryDateFilter == null ? "" : deliveryDateFilter.toString(),
                OffsetDateTime.now().toString()
        );
    }

    @GetMapping("/health")
    @ResponseBody
    public String health() {
        return "ok";
    }

    private QueryOptions toQueryOptions(int page, Integer requestedSize) {
        int normalizedPage = Math.max(page, 0);
        int requested = requestedSize == null ? pageSize : requestedSize;
        int normalizedSize = Math.min(Math.max(requested, 1), MAX_PAGE_SIZE);
        return new QueryOptions(normalizedPage, normalizedSize);
    }

    private String emptyIfNull(String value) {
        return value == null ? "" : value;
    }

    private record QueryOptions(int page, int size) {
    }

    public record DeliveredCarsResponse(
            List<DeliveredCar> cars,
            int page,
            int size,
            int totalItems,
            int totalPages,
            String model,
            String destination,
            String deliveryDate,
            String refreshedAt
    ) {
    }
}
