package com.datastax.apollo.service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.datastax.apollo.dao.SessionManager;
import com.datastax.apollo.dao.SpacecraftInstrumentsDao;
import com.datastax.apollo.dao.SpacecraftJourneyDao;
import com.datastax.apollo.dao.SpacecraftMapper;
import com.datastax.apollo.dao.SpacecraftMapperBuilder;
import com.datastax.apollo.entity.SpacecraftJourneyCatalog;
import com.datastax.apollo.entity.SpacecraftTemperatureOverTime;
import com.datastax.apollo.model.PagedResultWrapper;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.uuid.Uuids;

/**
 * Implementation of Service for controller
 * 
 */
@Component
public class ApolloService {

    /** Logger for the class. */
    private static final Logger LOGGER = LoggerFactory.getLogger(ApolloService.class);
   
    /** Driver Daos. */
    private SpacecraftJourneyDao     spacecraftJourneyDao;
    private SpacecraftInstrumentsDao spacecraftInstrumentsDao;
    
    /**
     * Find all spacecrafts in the catalog.
     */
    public List< SpacecraftJourneyCatalog > findAllSpacecrafts() {
        // no paging we don't expect more than 5k journeys
        return getSpaceCraftJourneyDao().findAll().all(); 
    }
    
    /**
     * Find all journeys for a spacecraft.
     * 
     * @param spacecraftName
     *      unique spacecraft name (PK)
     * @return
     *      list of journeys
     */
    public List < SpacecraftJourneyCatalog > findAllJourneysForSpacecraft(String spacecraftName) {
        // no paging we don't expect more than 5k journeys
        return getSpaceCraftJourneyDao().findAllJourneysForSpacecraft(spacecraftName).all();
    }
    
    /**
     * Search by primary key, unique record expect.
     *
     * @param spacecraftName
     *      unique spacecraft name (PK)
     * @param journeyid
     *      journey unique identifier
     * @return
     *      journey details if it exists
     */
    public Optional< SpacecraftJourneyCatalog > findJourneyById(String spacecraftName, UUID journeyId) {
        return getSpaceCraftJourneyDao().findById(spacecraftName, journeyId);
    }
    
    /**
     * Create a new {@link SpacecraftJourneyCatalog}.
     *
     * @param spacecraftName
     *       unique spacecraft name (PK)
     * @param summary
     *       short description
     * @return
     *       generated journey id
     */
    public UUID createSpacecraftJourney(String spacecraftName, String summary) {
        UUID journeyUid = Uuids.timeBased();
        LOGGER.info("Creating journey {} for spacecraft {}", journeyUid, spacecraftName);
        SpacecraftJourneyCatalog dto = new SpacecraftJourneyCatalog();
        dto.setName(spacecraftName);
        dto.setSummary(summary);
        dto.setStart(Instant.now());
        dto.setEnd(Instant.now().plus(1000, ChronoUnit.MINUTES));
        dto.setActive(false);
        dto.setJourneyId(journeyUid);
        getSpaceCraftJourneyDao().upsert(dto);
        return journeyUid;
    }
    
    /**
     * Retrieve temperature readings for a journey.
     *
     * @param spacecraftName
     *      name of spacecrafr
     * @param journeyId
     *      journey identifier
     * @param pageSize
     *      page size
     * @param pageState
     *      page state
     * @return
     *      result page
     */
    public PagedResultWrapper<SpacecraftTemperatureOverTime> getTemperatureReading(
            String spacecraftName, UUID journeyId, 
            Optional<Integer> pageSize, Optional<String> pageState) {
        PagingIterable<SpacecraftTemperatureOverTime> daoResult = 
                getSpaceCraftInstrumentsDao().getTemperatureReading(spacecraftName, journeyId, pageSize, pageState);
        return new PagedResultWrapper<SpacecraftTemperatureOverTime>(daoResult, 
                pageSize.isPresent() ? pageSize.get() : 0);
    }
    
    protected synchronized SpacecraftJourneyDao getSpaceCraftJourneyDao() {
        if (spacecraftJourneyDao == null) {
            CqlSession cqlSession   = SessionManager.getInstance().connectToApollo();
            SpacecraftMapper mapper = new SpacecraftMapperBuilder(cqlSession).build();
            this.spacecraftJourneyDao = mapper.spacecraftJourneyDao(cqlSession.getKeyspace().get());
        }
        return spacecraftJourneyDao;
    }
    
    protected synchronized SpacecraftInstrumentsDao getSpaceCraftInstrumentsDao() {
        if (spacecraftInstrumentsDao == null) {
            CqlSession cqlSession   = SessionManager.getInstance().connectToApollo();
            SpacecraftMapper mapper = new SpacecraftMapperBuilder(cqlSession).build();
            this.spacecraftInstrumentsDao = mapper.spacecraftInstrumentsDao(cqlSession.getKeyspace().get());
        }
        return spacecraftInstrumentsDao;
    }
    
    /**
     * Properly close CqlSession
     */
    @PreDestroy
    public void cleanUp() {
        SessionManager.getInstance().close();
    }
    
}
