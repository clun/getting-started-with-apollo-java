package com.datastax.apollo.controller;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.datastax.apollo.entity.SpacecraftTemperatureOverTime;
import com.datastax.apollo.model.PagedResultWrapper;
import com.datastax.apollo.service.ApolloService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;

@RestController
@Api(
   value = "api/spacecraft/{spaceCraftName}/{journeyId}/instruments", 
   description = "Works with Instruments")
@RequestMapping("api/spacecraft/{spacecraftName}/{journeyId}/instruments")
public class InstrumentsController {
    
    /** Logger for the class. */
    private static final Logger LOGGER = LoggerFactory.getLogger(InstrumentsController.class);
    
    /** Service implementation Injection. */
    private ApolloService spacecraftService;

    /**
     * Constructor.
     *
     * @param spacecraftService
     *      service implementation
     */
    public InstrumentsController(ApolloService spacecraftService) {
        this.spacecraftService = spacecraftService;
    }
    
    /**
     * Retrieve temperatur metrics
     */
    @GetMapping(produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "List all temperature reads", response = List.class)
    @ApiResponse(code = 200, message = "List all journeys for a spacecraft")
    public ResponseEntity<PagedResultWrapper<SpacecraftTemperatureOverTime>> getTemperatureReading(
            @ApiParam(name="spacecraftName", value="Spacecraft name",example = "gemini3",required=true )
            @PathVariable(value = "spacecraftName") String spacecraftName,
            @ApiParam(name="journeyId", value="Identifer for journey",example = "abb7c000-c310-11ac-8080-808080808080",required=true )
            @PathVariable(value = "journeyId") UUID journeyId, 
            @ApiParam(name="pageSize", value="Requested page size, default is 10", required=false ) 
            @RequestParam("pageSize") Optional<Integer> pageSize,
            @ApiParam(name="pageState", value="Use to retrieve next pages", required=false ) 
            @RequestParam("pageState") Optional<String> pageState) {
        LOGGER.info("Retrieving temperature readings for spacecraft {} and journey {}", spacecraftName, journeyId);
        // TODO new Page
        return ResponseEntity.ok(new PagedResultWrapper<>());
    }
    
}
