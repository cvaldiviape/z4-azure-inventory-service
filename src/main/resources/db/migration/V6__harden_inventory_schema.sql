ALTER TABLE inventory_reservations
    ADD CONSTRAINT fk_inventory_reservations_product FOREIGN KEY (product_id) REFERENCES inventories(product_id),
    ADD CONSTRAINT chk_inventory_reservations_quantity CHECK (quantity > 0),
    ADD CONSTRAINT chk_inventory_reservations_status CHECK (status IN ('RESERVED', 'RELEASED', 'CONFIRMED'));

CREATE INDEX idx_inventory_reservations_order_status
    ON inventory_reservations (order_id, status);

ALTER TABLE inbox_events
    ADD COLUMN aggregate_id VARCHAR(100),
    ADD COLUMN correlation_id VARCHAR(100),
    ADD COLUMN causation_id VARCHAR(100),
    ADD COLUMN producer VARCHAR(100),
    ADD COLUMN source_topic VARCHAR(200);

CREATE INDEX idx_inbox_events_correlation_id ON inbox_events (correlation_id);

CREATE TABLE inventory_movements (
    id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES inventories(product_id),
    order_id BIGINT NOT NULL,
    reservation_id VARCHAR(100) NOT NULL REFERENCES inventory_reservations(reservation_id),
    event_id VARCHAR(100) NOT NULL,
    movement_type VARCHAR(30) NOT NULL,
    quantity INT NOT NULL,
    available_before INT NOT NULL,
    available_after INT NOT NULL,
    reserved_before INT NOT NULL,
    reserved_after INT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT chk_inventory_movements_type CHECK (movement_type IN ('RESERVATION', 'RELEASE')),
    CONSTRAINT chk_inventory_movements_quantity CHECK (quantity > 0),
    CONSTRAINT chk_inventory_movements_quantities CHECK (
        available_before >= 0 AND available_after >= 0
        AND reserved_before >= 0 AND reserved_after >= 0
    )
);

CREATE INDEX idx_inventory_movements_product_created_at
    ON inventory_movements (product_id, created_at);

CREATE INDEX idx_inventory_movements_order_created_at
    ON inventory_movements (order_id, created_at);
