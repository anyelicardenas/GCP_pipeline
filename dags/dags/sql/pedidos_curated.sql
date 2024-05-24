SELECT idPedido, idCliente, idEmpleado, 
DATE(REPLACE(fechaPedido, '/', '-')) as fechaPedido,
DATE(fechaRequerida) AS fechaRequerida, 
DATE(NULLIF(fechaEnvio, '')) AS fechaEnvio,
idTransportista,
flete 
FROM `{project_name}.{dataset_name}.{table_name}`
ORDER BY fechaPedido, fechaRequerida, fechaEnvio