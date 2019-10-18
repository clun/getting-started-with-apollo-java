package com.datastax.apollo.dao;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import com.datastax.apollo.entity.SpacecraftLocationOverTime;
import com.datastax.apollo.entity.SpacecraftPressureOverTime;
import com.datastax.apollo.entity.SpacecraftTemperatureOverTime;
import com.datastax.apollo.model.PagedResultWrapper;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.annotations.Select;

/**
 * Operation to work with instruments
 */
@Dao
public interface SpacecraftInstrumentsDao {
    
    /**
     * Search for temperature readings.
     * TODO : We could replace the query provider here
     */
    @Select(customWhereClause = "spacecraft_name= :spacecraftName AND journey_id= :journeyId")
    PagingIterable<SpacecraftTemperatureOverTime> getTemperatureReading(
            String spacecraftName, UUID JourneyId,
            Function<BoundStatementBuilder, BoundStatementBuilder> setAttributes);
    
    /**
     * Search for temperature readings.
     */
    @QueryProvider(providerClass = SpacecraftInstrumentsQueryProvider.class, 
       entityHelpers = { SpacecraftTemperatureOverTime.class, SpacecraftPressureOverTime.class, 
                         SpacecraftLocationOverTime.class})
    PagingIterable<SpacecraftTemperatureOverTime> getTemperatureReading(
            String spacecraftName, UUID JourneyId, Optional<Integer> pageSize,
            Optional<String> pagingState);
    
    /**
     * Search for pressure readings.
     */
    @QueryProvider(providerClass = SpacecraftInstrumentsQueryProvider.class, 
       entityHelpers = { SpacecraftTemperatureOverTime.class, SpacecraftPressureOverTime.class, 
                         SpacecraftLocationOverTime.class})
    PagedResultWrapper<SpacecraftPressureOverTime> getPressureReading(
            String spacecraftName, UUID JourneyId, Optional<Integer> pageSize,
            Optional<String>  pagingState);
    
    /**
     * Search for location readings.
     */
    @QueryProvider(providerClass = SpacecraftInstrumentsQueryProvider.class, 
       entityHelpers = { SpacecraftTemperatureOverTime.class, SpacecraftPressureOverTime.class, 
                         SpacecraftLocationOverTime.class})
    PagedResultWrapper<SpacecraftLocationOverTime> getLocationReading(
            String spacecraftName, UUID JourneyId, Optional<Integer> pageSize,
            Optional<String>  pagingState);
}
