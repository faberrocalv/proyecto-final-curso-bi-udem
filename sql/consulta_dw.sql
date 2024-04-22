SELECT 
ventas.id_factura,
ventas.id_cliente,
ventas.id_categoria,
ventas.id_metodo_pago,
ventas.id_fecha,
ventas.id_centro_comercial,
ventas.precio_unitario,
ventas.cantidad,
ventas.venta_total,
clientes.genero,
clientes.edad,
metodo_pago.nombre_metodo_pago,
fecha.fecha_completa,
fecha.anio,
fecha.mes,
fecha.dia,
fecha.trimestre,
fecha.nombre_mes,
fecha.dia_semana,
categoria.nombre_categoria,
centro_comercial.nombre_centro_comercial
FROM 
ventas
LEFT JOIN clientes ON ventas.id_cliente = clientes.id
LEFT JOIN metodo_pago ON ventas.id_metodo_pago = metodo_pago.id
LEFT JOIN fecha ON ventas.id_fecha = fecha.id
LEFT JOIN categoria ON ventas.id_categoria = categoria.id
LEFT JOIN centro_comercial ON ventas.id_centro_comercial = centro_comercial.id;