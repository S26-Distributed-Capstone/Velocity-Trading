package edu.yu.marketmaker.model;

public record ReservationResponse(
        String id,
        ReservationStatus status,
        int grantedBidQuantity,
        int grantedAskQuantity
)
{

}
