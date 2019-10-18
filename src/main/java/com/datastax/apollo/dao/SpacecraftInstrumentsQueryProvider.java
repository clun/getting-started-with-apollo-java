package com.datastax.apollo.dao;

import static com.datastax.apollo.entity.AbstractInstrumentReading.COLUMN_JOURNEY_ID;
import static com.datastax.apollo.entity.AbstractInstrumentReading.COLUMN_SPACECRAFT_NAME;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.relation.Relation.column;

import java.util.Optional;
import java.util.UUID;

import com.datastax.apollo.entity.SpacecraftLocationOverTime;
import com.datastax.apollo.entity.SpacecraftPressureOverTime;
import com.datastax.apollo.entity.SpacecraftTemperatureOverTime;
import com.datastax.apollo.model.PagedResultWrapper;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.protocol.internal.util.Bytes;

/**
 * Implementation of Dynamic queries.
 *
 */
public class SpacecraftInstrumentsQueryProvider {
    
    private CqlSession cqlSession;
    
    /** Helper for bean, tables mappings. */
    private EntityHelper<SpacecraftTemperatureOverTime> ehTemperature;
    private EntityHelper<SpacecraftPressureOverTime>    ehPressure;
    private EntityHelper<SpacecraftLocationOverTime>    ehLocation;
    
    /** Statements Against Apollo. */
    private PreparedStatement psInsertTemperatureReading;
    private PreparedStatement psInsertPressureReading;
    private PreparedStatement psInsertLocationReading;
    
    private PreparedStatement psSelectTemperatureReading;
    private PreparedStatement psSelectPressureReading;
    private PreparedStatement psSelectLocationReading;
    
    /**
     * Constructor invoked by the DataStax driver based on Annotation {@link QueryProvider} 
     * set on class {@link SpacecraftInstrumentsDao}.
     * 
     * @param context
     *      context to extrat dse session
     * @param ehTemperature
     *      entity helper to interact with bean {@link SpacecraftTemperatureOverTime}
     * @param ehPressure
     *      entity helper to interact with bean {@link SpacecraftPressureOverTime}
     * @param ehLocation
     *      entity helper to interact with bean {@link SpacecraftLocationOverTime}      
     */
    public SpacecraftInstrumentsQueryProvider(MapperContext context,
            EntityHelper<SpacecraftTemperatureOverTime>  ehTemperature,
            EntityHelper<SpacecraftPressureOverTime>     ehPressure,
            EntityHelper<SpacecraftLocationOverTime>     ehLocation) {
        this.cqlSession     = context.getSession();
        this.ehTemperature  = ehTemperature;
        this.ehPressure     = ehPressure;
        this.ehLocation     = ehLocation;
        
        // Leveraging EntityHelper for insert queries
        psInsertTemperatureReading = cqlSession.prepare(ehTemperature.insert().asCql());
        psInsertPressureReading    = cqlSession.prepare(ehPressure.insert().asCql());
        psInsertLocationReading    = cqlSession.prepare(ehLocation.insert().asCql());
        
        psSelectTemperatureReading = cqlSession.prepare(
                selectFrom(SpacecraftTemperatureOverTime.TABLE_NAME).all()
                .where(column(COLUMN_SPACECRAFT_NAME).isEqualTo(bindMarker(COLUMN_SPACECRAFT_NAME)))
                .where(column(COLUMN_JOURNEY_ID).isEqualTo(bindMarker(COLUMN_JOURNEY_ID)))
                .build());
        psSelectPressureReading = cqlSession.prepare(
                selectFrom(SpacecraftPressureOverTime.TABLE_NAME).all()
                .where(column(COLUMN_SPACECRAFT_NAME).isEqualTo(bindMarker(COLUMN_SPACECRAFT_NAME)))
                .where(column(COLUMN_JOURNEY_ID).isEqualTo(bindMarker(COLUMN_JOURNEY_ID)))
                .build());
        psSelectLocationReading = cqlSession.prepare(
                selectFrom(SpacecraftLocationOverTime.TABLE_NAME).all()
                .where(column(COLUMN_SPACECRAFT_NAME).isEqualTo(bindMarker(COLUMN_SPACECRAFT_NAME)))
                .where(column(COLUMN_JOURNEY_ID).isEqualTo(bindMarker(COLUMN_JOURNEY_ID)))
                .build());
    }
    
    /**
     * Retrieve Temperature reading for a journey.
     */
    public PagingIterable<SpacecraftTemperatureOverTime> getTemperatureReading(
            String spacecraftName,
            UUID journeyId,
            Optional<Integer> pageSize,
            Optional<String>  pagingState) {
        
        // Detailing operations for the first (next will be much compact)
        
        // (1) - Bind the prepared statement with parameters 
        BoundStatement bsTemperature = psSelectTemperatureReading.bind()
                .setUuid(COLUMN_JOURNEY_ID, journeyId)
                .setString(spacecraftName, spacecraftName);

        // (2) - Update the bound statement to add paging metadata (pageSize, pageState)
        bsTemperature = paging(bsTemperature, pageSize, pagingState);
        
        // (3) - Executing query
        ResultSet resultSet = cqlSession.execute(bsTemperature);
        
        // (4) - Using the entity Help to marshall to expect bean
        return resultSet.map(ehTemperature::get);
    }
    
    /**
     * Retrieve Pressure reading for a journey.
     */
    public PagedResultWrapper<SpacecraftPressureOverTime> getPressureReading(
            String spacecraftName, UUID journeyId, Optional<Integer> pageSize, Optional<String>  pagingState) {
       return new PagedResultWrapper<SpacecraftPressureOverTime>(
               cqlSession.execute(paging(psSelectPressureReading.bind()
                       .setUuid(COLUMN_JOURNEY_ID, journeyId)
                       .setString(spacecraftName, spacecraftName), pageSize, pagingState))
               .map(ehPressure::get), pageSize.isPresent() ? pageSize.get() : 0);
    }
    
    /**
     * Retrieve Location reading for a journey.
     */
    public PagedResultWrapper<SpacecraftLocationOverTime> getLocationReading(
            String spacecraftName, UUID journeyId, Optional<Integer> pageSize, Optional<String>  pagingState) {
       return new PagedResultWrapper<SpacecraftLocationOverTime>(
               cqlSession.execute(paging(psSelectLocationReading.bind()
                       .setUuid(COLUMN_JOURNEY_ID, journeyId)
                       .setString(spacecraftName, spacecraftName), pageSize, pagingState))
               .map(ehLocation::get), pageSize.isPresent() ? pageSize.get() : 0);
    }
    
    /**
     * Syntaxic sugar to help with paging
     */
    private BoundStatement paging(BoundStatement bs, Optional<Integer> pageSize, Optional<String>  pagingState) {
        if(pageSize.isPresent()) {
            bs = bs.setPageSize(pageSize.get());
         }
         if (pagingState.isPresent()) {
            bs = bs.setPagingState(Bytes.fromHexString(pagingState.get()));
         };
         return bs;
    }
    
    /**
     * Syntaxic sugar to help with mapping
     */
    public static <T> BoundStatement bind(PreparedStatement preparedStatement, T entity, EntityHelper<T> entityHelper) {
        BoundStatementBuilder boundStatement = preparedStatement.boundStatementBuilder();
        entityHelper.set(entity, boundStatement, NullSavingStrategy.DO_NOT_SET);
        return boundStatement.build();
    }
}
