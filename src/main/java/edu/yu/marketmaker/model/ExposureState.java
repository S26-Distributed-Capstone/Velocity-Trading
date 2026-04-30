package edu.yu.marketmaker.model;

public record ExposureState(int bidUsage, int askUsage, int totalCapacity, int activeReservations) {}