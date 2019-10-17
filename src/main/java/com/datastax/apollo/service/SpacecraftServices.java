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
public class SpacecraftServices {

    /** Logger for the class. */
    private static final Logger LOGGER = LoggerFactory.getLogger(SpacecraftServices.class);
   
    /** Driver Dao. */
    private SpacecraftJourneyDao spaceCraftJourneyDao;
    
    /**
     * Find all spacecrafts in the catalog.
     */
    public List< SpacecraftJourneyCatalog > findAllSpacecrafts() {
        if (null == spaceCraftJourneyDao) initDao();
        // no paging we don't expect more than 5k journeys
        return spaceCraftJourneyDao.findAll().all(); 
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
        if (null == spaceCraftJourneyDao) initDao();
        // no paging we don't expect more than 5k journeys
        return spaceCraftJourneyDao.findAllJourneysForSpacecraft(spacecraftName).all();
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
        if (null == spaceCraftJourneyDao) initDao();
        return spaceCraftJourneyDao.findById(spacecraftName, journeyId);
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
        spaceCraftJourneyDao.upsert(dto);
        return journeyUid;
    }
    
    /**
     * Initialize Dao (SessionManager must have been initialized first).
     */
    protected void initDao() {
        CqlSession cqlSession = SessionManager.getInstance().getCqlSession();
        SpacecraftMapper mapper = new SpacecraftMapperBuilder(cqlSession).build(); 
        this.spaceCraftJourneyDao = mapper.spacecraftJourneyDao(cqlSession.getKeyspace().get());
    }
    
    /**
     * Properly close CqlSession
     */
    @PreDestroy
    public void cleanUp() {
        SessionManager.getInstance().close();
    }
    
}
