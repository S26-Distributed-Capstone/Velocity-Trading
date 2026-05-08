package edu.yu.marketmaker.exchange;

import edu.yu.marketmaker.model.Fill;

/**
 * Interface for a component that handles sending fills
 */
public interface FillSender {
    
    /**
     * Handle the sending of a fill
     * @param fill the fill to send
     */
    void sendFill(Fill fill);
}
