SELECT COUNT(1) AS fechaPedidoCount, FORMAT_DATETIME("%B", fechaPedido) as mes
FROM `{project_name}.{dataset_name}.pedidos_curated`
where fechaPedido > DATETIME_SUB((SELECT max(fechaPedido) FROM `{project_name}.{dataset_name}.pedidos_curated`), INTERVAL 5 MONTH)
GROUP BY mes;