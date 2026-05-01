/**
 * Core market-making logic: turning position state into quotes.
 *
 * A market-maker node owns a subset of symbols (assigned by the
 * {@link edu.yu.marketmaker.cluster cluster} package) and for each owned symbol
 * consumes position/fill updates, tracks inventory, and produces bid/ask quotes
 * that get published back to the exchange.
 *
 * Key responsibilities:
 * <ul>
 *   <li>Driver loop that reacts to state changes ({@link edu.yu.marketmaker.marketmaker.MarketMaker}).</li>
 *   <li>Position and snapshot tracking per owned symbol
 *       ({@link edu.yu.marketmaker.marketmaker.PositionTracker},
 *       {@link edu.yu.marketmaker.marketmaker.SnapshotTracker}).</li>
 *   <li>Quote generation strategies ({@link edu.yu.marketmaker.marketmaker.QuoteGenerator},
 *       {@link edu.yu.marketmaker.marketmaker.ProductionQuoteGenerator},
 *       {@link edu.yu.marketmaker.marketmaker.TestQuoteGenerator}).</li>
 *   <li>Status/introspection endpoints
 *       ({@link edu.yu.marketmaker.marketmaker.MarketMakerStatusController}).</li>
 * </ul>
 */
package edu.yu.marketmaker.marketmaker;
