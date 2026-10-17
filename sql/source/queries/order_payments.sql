SELECT 
    [order_id],
    [payment_sequential],
    [payment_type],
    [payment_installments],
    [payment_value]
FROM [dbo].[olist_order_payments_dataset] WITH (NOLOCK);
