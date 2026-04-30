package edu.yu.marketmaker.exposurereservation;

import edu.yu.marketmaker.model.Reservation;

import java.util.Collection;
import java.util.Optional;

public interface ReservationRepository {
    void save(Reservation reservation);
    Optional<Reservation> findById(String id);
    Collection<Reservation> findAll();
}