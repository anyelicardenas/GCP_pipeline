SELECT idEmpleado, nombreEmpleado, titulo, ciudad, pais,
NULLIF(reportaA, '') AS reportaA 
FROM `{project_name}.{dataset_name}.{table_name}`
ORDER BY idEmpleado