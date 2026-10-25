CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.olist_customers_dataset (
    customer_id              VARCHAR(32),
    customer_unique_id       VARCHAR(32),
    customer_zip_code_prefix INT,
    customer_city            VARCHAR(100),
    customer_state           VARCHAR(10),
    _extracted_at            TIMESTAMP,
    _batch_id                VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS {schema}.olist_products_dataset (
    product_id                 VARCHAR(32),
    product_category_name      VARCHAR(100),
    product_name_lenght        INT,
    product_description_lenght INT,
    product_photos_qty         INT,
    product_weight_g           INT,
    product_length_cm          INT,
    product_height_cm          INT,
    product_width_cm           INT,
    _extracted_at              TIMESTAMP,
    _batch_id                  VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS {schema}.olist_sellers_dataset (
    seller_id              VARCHAR(32),
    seller_zip_code_prefix INT,
    seller_city            VARCHAR(100),
    seller_state           VARCHAR(10),
    _extracted_at          TIMESTAMP,
    _batch_id              VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS {schema}.olist_orders_dataset (
    order_id                      VARCHAR(32),
    customer_id                   VARCHAR(32),
    order_status                  VARCHAR(20),
    order_purchase_timestamp      TIMESTAMP,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    _extracted_at                 TIMESTAMP,
    _batch_id                     VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS {schema}.olist_order_items_dataset (
    order_id            VARCHAR(32),
    order_item_id       INT,
    product_id          VARCHAR(32),
    seller_id           VARCHAR(32),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10, 2),
    freight_value       NUMERIC(10, 2),
    _extracted_at       TIMESTAMP,
    _batch_id           VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS {schema}.olist_order_payments_dataset (
    order_id             VARCHAR(32),
    payment_sequential   INT,
    payment_type         VARCHAR(30),
    payment_installments INT,
    payment_value        NUMERIC(10, 2),
    _extracted_at        TIMESTAMP,
    _batch_id            VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS {schema}.olist_order_reviews_dataset (
    review_id               VARCHAR(32),
    order_id                VARCHAR(32),
    review_score            INT,
    review_comment_title    VARCHAR(255),
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    _extracted_at           TIMESTAMP,
    _batch_id               VARCHAR(30)
);
