SELECT * FROM `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza`
 WHERE num_poliza = '120020939';

SELECT * FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_journey_tmp_vitto`
WHERE nro_doc_cliente = '09874990'
and codproducto_final = '8202'
order by id_estado_asesoria_orden desc,fechora_actualizacion_estado_asesoria desc;

Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto`
where nro_doc_cliente = '09874990'
and codproducto_final = '8202';

Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_vitto` 
where id_persona_cliente = 'AX-10600298'
and id_producto_final = 'AX-8202';

Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.tramite_vitto` 
where num_documento_cliente = '49009739'
and id_producto =  'AX-8202';

Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` 
where trm_id_contratante = 'AX-10508507'
and trm_id_producto =  'AX-8202';


