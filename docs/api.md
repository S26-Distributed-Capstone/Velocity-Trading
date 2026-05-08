# Market-Maker HTTP API Reference

All endpoints are REST (JSON over HTTP). Each service is activated by a Spring profile.

---

## Exchange Service (`exchange` profile)

| Method   | Path               | Description                                                                                                             |
|----------|--------------------|-------------------------------------------------------------------------------------------------------------------------|
| **GET**  | `/quotes/{symbol}` | Retrieve the current quote for a given ticker symbol. Returns **404** if the symbol is not found.                       |
| **PUT**  | `/quotes/{symbol}` | Create or update the quote for a symbol. Expects a `Quote` JSON body.                                                   |
| **POST** | `/orders`          | Submit an external order for matching. Expects an `ExternalOrder` JSON body. Returns **400** if order validation fails. |
| **GET**  | `/health`          | Health-check endpoint. Returns a `ServiceHealth` object (`{ healthy, uptime, name }`).                                  |

### Error Handling (ExchangeServiceAdvice)

| Exception                  | HTTP Status         | Description                                                                    |
|----------------------------|---------------------|--------------------------------------------------------------------------------|
| `QuoteNotFoundException`   | **404 Not Found**   | Returned when `GET /quotes/{symbol}` references a symbol with no active quote. |
| `OrderValidationException` | **400 Bad Request** | Returned when `POST /orders` receives an invalid order.                        |

---

## Exposure Reservation Service (`exposure-reservation` profile)

| Method   | Path                            | Description                                                                                                                                                                        |
|----------|---------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **POST** | `/reservations`                 | Request exposure capacity for a quote. Expects a `Quote` JSON body. Returns a `ReservationResponse` with status `GRANTED`, `PARTIAL`, or `DENIED`.                                 |
| **POST** | `/reservations/{id}/apply-fill` | Apply a fill to an existing reservation (by UUID). Reduces reserved exposure by the filled amount. Expects an `int` body (the filled quantity). Returns a `FreedCapacityResponse`. |
| **POST** | `/reservations/{id}/release`    | Manually release a reservation (by UUID), typically when a quote is cancelled or replaced without being filled. Returns a `FreedCapacityResponse`.                                 |
| **GET**  | `/exposure`                     | Retrieve the current global exposure state (usage, total capacity, active reservation count). Returns an `ExposureState` object.                                                   |
| **GET**  | `/health`                       | Health-check endpoint. Returns a `ServiceHealth` object.                                                                                                                           |

---

## Trading State Service (`trading-state` profile)

| Method   | Path                  | Description                                                                                                                                 |
|----------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| **POST** | `/state/fills`        | Submit a fill to update system-wide positions. Expects a `Fill` JSON body. Returns **400** for invalid input, **500** on repository errors. |
| **GET**  | `/positions`          | Retrieve all current positions. Returns a collection of `Position` objects.                                                                 |
| **GET**  | `/positions/{symbol}` | Retrieve a specific position by ticker symbol. Returns the `Position` if present, or empty.                                                 |
| **GET**  | `/health`             | Health-check endpoint. Returns a `ServiceHealth` object.                                                                                    |

### RSocket Endpoints (TCP)

These use Spring RSocket `@MessageMapping` — **not** HTTP.

| Route                | Pattern          | Description                                                                               |
|----------------------|------------------|-------------------------------------------------------------------------------------------|
| `state.fills`        | request-response | Submit a fill (same logic as `POST /state/fills`).                                        |
| `positions`          | request-stream   | Stream all current positions.                                                             |
| `positions.{symbol}` | request-response | Get a single position by symbol.                                                          |
| `state.stream`       | request-stream   | Subscribe to a live stream of `StateSnapshot` events (current state + real-time updates). |
