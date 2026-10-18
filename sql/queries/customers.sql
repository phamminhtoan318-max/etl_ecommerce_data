SELECT 
    [customer_id],
    [customer_unique_id],
    [customer_zip_code_prefix],
    [customer_city],
    [customer_state]
FROM [dbo].[olist_customers_dataset] WITH (NOLOCK);
