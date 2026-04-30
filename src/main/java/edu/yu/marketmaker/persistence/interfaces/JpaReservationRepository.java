package edu.yu.marketmaker.persistence.interfaces;

import edu.yu.marketmaker.persistence.BaseJpaRepository;
import edu.yu.marketmaker.persistence.ReservationEntity;
import org.springframework.stereotype.Repository;

/**
 * JPA Repository interface for ReservationEntity persistence.
 * Spring Data JPA requires concrete interfaces to create proxy implementations.
 */
@Repository
public interface JpaReservationRepository extends BaseJpaRepository<ReservationEntity, String> {
}

