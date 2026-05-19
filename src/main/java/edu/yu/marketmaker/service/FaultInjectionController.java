package edu.yu.marketmaker.service;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test/fault-injection")
@Profile("fault-injection")
public class FaultInjectionController {
    
    private final FaultInjector injector;

    public FaultInjectionController(FaultInjector injector) {
        this.injector = injector;
    }

    @PostMapping("/arm-fault")
    public ArmedStatus armFault(@RequestParam FaultInjector.Event event, @RequestParam String symbol) {
        injector.armSymbol(event, symbol);
        return new ArmedStatus(event, symbol);
    }

    public record ArmedStatus(FaultInjector.Event event, String armedSymbol) {}
}
