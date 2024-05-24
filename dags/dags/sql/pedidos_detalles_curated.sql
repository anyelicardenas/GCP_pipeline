SELECT 
    idPedido, 
    CAST(CONCAT(REPLACE(idProducto, ' ', '')) AS INT64) AS idProducto, 
    precioUnitario, 
    cantidad, 
    descuento  
FROM `{project_name}.{dataset_name}.{table_name}`
ORDER BY idProducto