-- SELECT COUNT(1) as totalVentasCategoria, `{project_name}.{dataset_name}.productos_curated`.idCategoria 
-- FROM `{project_name}.{dataset_name}.pedidos_detalles_curated`
-- INNER JOIN `{project_name}.{dataset_name}.productos_curated`
-- ON `{project_name}.{dataset_name}.pedidos_detalles_curated`.idProducto = `{project_name}.{dataset_name}.productos_curated`.idProducto
-- GROUP BY `{project_name}.{dataset_name}.productos_curated`.idCategoria
-- ORDER BY totalVentasCategoria DESC;

SELECT COUNT(1) as totalVentasCategoria, productos_curated.idCategoria 
FROM `{project_name}.{dataset_name}.pedidos_detalles_curated` as pedidos_curated
INNER JOIN `{project_name}.{dataset_name}.productos_curated` as productos_curated
ON pedidos_detalles_curated.idProducto = productos_curated.idProducto
GROUP BY productos_curated.idCategoria
ORDER BY totalVentasCategoria DESC;