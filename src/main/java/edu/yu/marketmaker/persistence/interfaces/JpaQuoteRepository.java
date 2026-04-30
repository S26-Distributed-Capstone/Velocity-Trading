package edu.yu.marketmaker.persistence.interfaces;

import edu.yu.marketmaker.persistence.BaseJpaRepository;
import edu.yu.marketmaker.persistence.QuoteEntity;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * JPA Repository interface for QuoteEntity persistence.
 * Spring Data JPA requires concrete interfaces to create proxy implementations.
 */
@Repository
public interface JpaQuoteRepository extends BaseJpaRepository<QuoteEntity, String> {
    Optional<QuoteEntity> findBySymbol(String symbol);
    void deleteBySymbol(String symbol);
    List<QuoteEntity> findAllBySymbolIn(Collection<String> symbols);
}
