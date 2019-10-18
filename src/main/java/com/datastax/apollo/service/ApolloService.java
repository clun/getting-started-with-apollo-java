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
import com.datastax.apollo.dao.SpacecraftJourneyDao;
import com.datastax.apollo.dao.SpacecraftMapper;
import com.datastax.apollo.dao.SpacecraftMapperBuilder;
import com.datastax.apollo.entity.SpacecraftJourneyCatalog;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.uuid.Uuids;

/**
 * Implementation of Service for controller
 * 
 */
@Component
public class ApolloService {

    /** Logger for the class. */
    private static final Logger LOGGER = LoggerFactory.getLogger(ApolloService.class);
   
    /** Driver Dao. */
    private SpacecraftJourneyDao spaceCraftJourneyDao;
    
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
    
    protected synchronized SpacecraftJourneyDao getSpaceCraftJourneyDao() {
        if (spaceCraftJourneyDao == null) {
            CqlSession cqlSession   = SessionManager.getInstance().connectToApollo();
            SpacecraftMapper mapper = new SpacecraftMapperBuilder(cqlSession).build();
            this.spaceCraftJourneyDao = mapper.spacecraftJourneyDao(cqlSession.getKeyspace().get());
        }
        return spaceCraftJourneyDao;
    }
    
    /**
     * Properly close CqlSession
     */
    @PreDestroy
    public void cleanUp() {
        SessionManager.getInstance().close();
    }
    
}
