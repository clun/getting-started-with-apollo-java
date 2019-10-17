package com.datastax.apollo.controller;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import com.datastax.apollo.entity.SpacecraftJourneyCatalog;
import com.datastax.apollo.service.SpacecraftServices;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 * REST Controller for operations on spacecrafts catalog.O
 */
@RestController
@Api(
 value = "/api/spacecrafts", 
 description = "Operations on spacecrafts catalog")
@RequestMapping("/api/spacecrafts")
public class SpacecraftController {
    
    /** Logger for the class. */
    private static final Logger LOGGER = LoggerFactory.getLogger(SpacecraftController.class);
    
    /** Service implementation Injection. */
    private SpacecraftServices spacecraftService;

    /**
     * Constructor.
     *
     * @param spacecraftService
     *      service implementation
     */
    public SpacecraftController(SpacecraftServices spacecraftService) {
        this.spacecraftService = spacecraftService;
    }
    
    /**
     * List all spacecrafts from the catalog
     *  
     * @return
     *      list all {@link SpacecraftJourneyCatalog} available in the table 
     */
    @RequestMapping(method = GET, value = "/", produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "List all spacecrafts and journeys", response = List.class)
    @ApiResponse(code = 200, message = "List all journeys for a spacecraft")
    public ResponseEntity<List<SpacecraftJourneyCatalog>> findAllSpacecrafts() {
        LOGGER.debug("Retrieving all spacecrafts");
        return ResponseEntity.ok(spacecraftService.findAllSpacecrafts());
    }
    
    /**
     * List all journeys for a dedicated spacecraft. If the spacecraft is not found we will show an empty list (an dnot 404.)
     *
     * @param spacecraft_name
     *      spacecraft_name to locate journeys
     * @return
     *     list of associated journey, can be empty
     */
    @RequestMapping(
            value = "/{spacecraftName}",
            method = GET,
            produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "List all journeys for a dedicated spacecraft name", response = List.class)
    @ApiResponse(code = 200, message = "List all journeys for a dedicated spacecraft name")
    public ResponseEntity<List<SpacecraftJourneyCatalog>> findAllJourneysForSpacecraft(
            @ApiParam(name="spacecraftName", value="Spacecraft name",example = "soyuztm-8",required=true )
            @PathVariable(value = "spacecraftName") String spaceCraftName) {
        return ResponseEntity.ok(spacecraftService.findAllJourneysForSpacecraft(spaceCraftName));
    }
    
    /**
     * Find a unique spacecraft journey from its reference.
     *
     * @param spacecraft_name
     *      spacecraft_name to locate journeys
     * @return
     *     list of associated journey, can be empty
     */
    @RequestMapping(
            value = "/{spacecraftName}/{journeyId}",
            method = GET,
            produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Retrieve a journey from its spacecraftname and journeyid", response = List.class)
    @ApiResponses({
        @ApiResponse(code = 200, message = "Returnings SpacecraftJourneyCatalog"),
        @ApiResponse(code = 400, message = "spacecraftName is blank or contains invalid characters (expecting AlphaNumeric)"),
        @ApiResponse(code = 404, message = "No journey exists for the provided spacecraftName and journeyid")
    })
    public ResponseEntity<SpacecraftJourneyCatalog> findSpacecraftJourney(
            @ApiParam(name="spacecraftName", value="Spacecraft name",example = "soyuztm-8",required=true )
            @PathVariable(value = "spacecraftName") String spacecraftName,
            @ApiParam(name="journeyId", value="Identifer for journey",example = "805b1a00-5673-11a8-8080-808080808080",required=true )
            @PathVariable(value = "journeyId") UUID journeyId) {
        LOGGER.info("Fetching journey with spacecraft name {} and journeyid {}", spacecraftName, journeyId);
        // Invoking Service
        Optional<SpacecraftJourneyCatalog> journey = spacecraftService.findJourneyById(spacecraftName, journeyId);
        // Routing Result
        if (!journey.isPresent()) {
            LOGGER.warn("Journey with spacecraft name {} and journeyid {} has not been found", spacecraftName, journeyId);
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(journey.get());
    }
    
    /**
     * Create a new Journey for a Spacecraft
     */
    @RequestMapping(
            method = POST,
            value = "/{spacecraftName}", 
            consumes = TEXT_PLAIN_VALUE, produces = TEXT_PLAIN_VALUE)
    @ApiOperation(value = " Create a new Journey for a Spacecraft", response = String.class)
    @ApiResponses({
            @ApiResponse(code = 201, message = "Journey has been created"),
            @ApiResponse(code = 400, message = "Invalid Spacecraft name provided")
    })
    @ApiImplicitParams({
        @ApiImplicitParam(
            name = "summary",
            value = "Body of the request is a string representing the summary",
            required = true, dataType = "String", paramType = "body")
    })
    public ResponseEntity<UUID> createSpacecraftJourney(
            HttpServletRequest request,
            @ApiParam(name="spacecraftName", value="Spacecraft name",example = "soyuztm-8",required=true )
            @PathVariable(value = "spacecraftName") String spacecraftName,
            @RequestBody String summary) {
        UUID journeyId = spacecraftService.createSpacecraftJourney(spacecraftName, summary);
        // HTTP Created spec, return target resource in 'location' header
        URI location = ServletUriComponentsBuilder.fromRequestUri(request)
                .replacePath("/api/spacecrafts/{spacecraftName}/{journeyId}")
                .buildAndExpand(spacecraftName, journeyId)
                .toUri();
        // HTTP 201 with confirmation number
        return ResponseEntity.created(location).body(journeyId);
    }
    
    /**
     * Handling all errors.
     * @param ex
     * @return
     */
    @ExceptionHandler(value = IllegalArgumentException.class)
    @ResponseStatus(value = HttpStatus.BAD_REQUEST)
    public String _errorBadRequestHandler(IllegalArgumentException ex) {
        return "Invalid Parameter: " + ex.getMessage();
    }

}
