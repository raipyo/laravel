UPDATE
    transactions
    JOIN
    ewallet_transactions ON (ewallet_transactions.ewallet_transaction_type_id = transactions.ewallet_transaction_type_id 
    AND ewallet_transactions.processor_reference_number = transactions.bux_reference_id)
SET
    transactions.ewallet_transaction_id = ewallet_transactions.id;