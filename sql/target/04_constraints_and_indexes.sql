ALTER TABLE dwh.dim_location DROP CONSTRAINT IF EXISTS pk_dim_location;
ALTER TABLE dwh.dim_location ADD CONSTRAINT pk_dim_location PRIMARY KEY (location_key);

ALTER TABLE dwh.dim_customer DROP CONSTRAINT IF EXISTS pk_dim_customer;
ALTER TABLE dwh.dim_customer ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key);

ALTER TABLE dwh.dim_seller DROP CONSTRAINT IF EXISTS pk_dim_seller;
ALTER TABLE dwh.dim_seller ADD CONSTRAINT pk_dim_seller PRIMARY KEY (seller_key);

ALTER TABLE dwh.dim_product DROP CONSTRAINT IF EXISTS pk_dim_product;
ALTER TABLE dwh.dim_product ADD CONSTRAINT pk_dim_product PRIMARY KEY (product_key);

ALTER TABLE dwh.dim_date DROP CONSTRAINT IF EXISTS pk_dim_date;
ALTER TABLE dwh.dim_date ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_key);

ALTER TABLE dwh.fact_orders DROP CONSTRAINT IF EXISTS pk_fact_orders;
ALTER TABLE dwh.fact_orders ADD CONSTRAINT pk_fact_orders PRIMARY KEY (order_key);

ALTER TABLE dwh.fact_sales DROP CONSTRAINT IF EXISTS pk_fact_sales;
ALTER TABLE dwh.fact_sales ADD CONSTRAINT pk_fact_sales PRIMARY KEY (sales_key);

ALTER TABLE dwh.dim_customer DROP CONSTRAINT IF EXISTS fk_dim_customer_dim_location;
ALTER TABLE dwh.dim_customer ADD CONSTRAINT fk_dim_customer_dim_location 
    FOREIGN KEY (location_key) REFERENCES dwh.dim_location (location_key);

ALTER TABLE dwh.dim_seller DROP CONSTRAINT IF EXISTS fk_dim_seller_dim_location;
ALTER TABLE dwh.dim_seller ADD CONSTRAINT fk_dim_seller_dim_location 
    FOREIGN KEY (location_key) REFERENCES dwh.dim_location (location_key);

ALTER TABLE dwh.fact_orders DROP CONSTRAINT IF EXISTS fk_fact_orders_dim_customer;
ALTER TABLE dwh.fact_orders ADD CONSTRAINT fk_fact_orders_dim_customer 
    FOREIGN KEY (customer_key) REFERENCES dwh.dim_customer (customer_key);

ALTER TABLE dwh.fact_orders DROP CONSTRAINT IF EXISTS fk_fact_orders_dim_location;
ALTER TABLE dwh.fact_orders ADD CONSTRAINT fk_fact_orders_dim_location 
    FOREIGN KEY (shipping_location_key) REFERENCES dwh.dim_location (location_key);

ALTER TABLE dwh.fact_orders DROP CONSTRAINT IF EXISTS fk_fact_orders_dim_date_purchase;
ALTER TABLE dwh.fact_orders ADD CONSTRAINT fk_fact_orders_dim_date_purchase 
    FOREIGN KEY (purchase_date_key) REFERENCES dwh.dim_date (date_key);

ALTER TABLE dwh.fact_orders DROP CONSTRAINT IF EXISTS fk_fact_orders_dim_date_approved;
ALTER TABLE dwh.fact_orders ADD CONSTRAINT fk_fact_orders_dim_date_approved 
    FOREIGN KEY (approved_date_key) REFERENCES dwh.dim_date (date_key);

ALTER TABLE dwh.fact_orders DROP CONSTRAINT IF EXISTS fk_fact_orders_dim_date_delivered;
ALTER TABLE dwh.fact_orders ADD CONSTRAINT fk_fact_orders_dim_date_delivered 
    FOREIGN KEY (delivered_date_key) REFERENCES dwh.dim_date (date_key);

ALTER TABLE dwh.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_sales_dim_customer;
ALTER TABLE dwh.fact_sales ADD CONSTRAINT fk_fact_sales_dim_customer 
    FOREIGN KEY (customer_key) REFERENCES dwh.dim_customer (customer_key);

ALTER TABLE dwh.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_sales_dim_product;
ALTER TABLE dwh.fact_sales ADD CONSTRAINT fk_fact_sales_dim_product 
    FOREIGN KEY (product_key) REFERENCES dwh.dim_product (product_key);

ALTER TABLE dwh.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_sales_dim_seller;
ALTER TABLE dwh.fact_sales ADD CONSTRAINT fk_fact_sales_dim_seller 
    FOREIGN KEY (seller_key) REFERENCES dwh.dim_seller (seller_key);

ALTER TABLE dwh.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_sales_dim_location;
ALTER TABLE dwh.fact_sales ADD CONSTRAINT fk_fact_sales_dim_location 
    FOREIGN KEY (shipping_location_key) REFERENCES dwh.dim_location (location_key);

ALTER TABLE dwh.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_sales_dim_date;
ALTER TABLE dwh.fact_sales ADD CONSTRAINT fk_fact_sales_dim_date 
    FOREIGN KEY (purchase_date_key) REFERENCES dwh.dim_date (date_key);

CREATE INDEX IF NOT EXISTS ix_fact_orders_order_id ON dwh.fact_orders (order_id);
CREATE INDEX IF NOT EXISTS ix_fact_orders_purchase_date_key ON dwh.fact_orders (purchase_date_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_order_id ON dwh.fact_sales (order_id);
CREATE INDEX IF NOT EXISTS ix_fact_sales_product_key ON dwh.fact_sales (product_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_seller_key ON dwh.fact_sales (seller_key);
