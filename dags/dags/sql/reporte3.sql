SELECT DISTINCT nombreContacto
FROM `{project_name}.{dataset_name}.clientes_curated`
INNER JOIN `{project_name}.{dataset_name}.pedidos_curated` 
ON `{project_name}.{dataset_name}.clientes_curated`.idCliente = `{project_name}.{dataset_name}.pedidos_curated`.idCliente
INNER JOIN `{project_name}.{dataset_name}.pedidos_detalles_curated`
ON `{project_name}.{dataset_name}.pedidos_curated`.idPedido = `{project_name}.{dataset_name}.pedidos_detalles_curated`.idPedido 
INNER JOIN `{project_name}.{dataset_name}.productos_curated`
ON `{project_name}.{dataset_name}.pedidos_detalles_curated`.idProducto = `{project_name}.{dataset_name}.productos_curated`.idProducto
WHERE `{project_name}.{dataset_name}.productos_curated`.nombreProducto = 'Tofu';
