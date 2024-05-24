SELECT idTransportista, count(idTransportista) as numero_de_entregas
FROM `{project_name}.{dataset_name}.pedidos_curated`
INNER JOIN `{project_name}.{dataset_name}.pedidos_detalles_curated`
ON `{project_name}.{dataset_name}.pedidos_curated`.idPedido =  `{project_name}.{dataset_name}.pedidos_detalles_curated`.idPedido
INNER JOIN `{project_name}.{dataset_name}.productos_curated`
ON `{project_name}.{dataset_name}.productos_curated`.idProducto =  `{project_name}.{dataset_name}.pedidos_detalles_curated`.idProducto
INNER JOIN `{project_name}.{dataset_name}.categorias_curated`
ON `{project_name}.{dataset_name}.categorias_curated`.idCategoria = `{project_name}.{dataset_name}.productos_curated`.idCategoria
WHERE `{project_name}.{dataset_name}.categorias_curated`.nombreCategoria = 'Beverages' and fechaEnvio is not NULL and fechaEnvio < CURRENT_DATETIME()
GROUP BY idTransportista
ORDER BY numero_de_entregas DESC;