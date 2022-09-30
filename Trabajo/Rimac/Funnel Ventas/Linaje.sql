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


---


SELECT * FROM `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza`
 WHERE num_poliza ='1300004548'
 -- id_poliza in ('AE-619391823','AE-619391980');


SELECT * FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_journey_tmp_vitto` j --cot_nro_poliza,pago_flg,id_estado_cotizacion,codproducto_final,origen_data,
WHERE nro_doc_cliente = '29554615'
--and codproducto_final = '8917'
 ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC


Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto`
where trim(nro_doc_cliente) = '29554615'
and trim(codproducto_final) = '8917';

Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_cambio_vitto` 
where numpol_ext_cot = '1300004897' 
--id_persona_cliente = 'AX-10600298'
--and id_producto_final = 'AX-8202';

Select tipo_operacion,id_producto,origen_data,id_poliza,num_documento_cliente,mes_produccion
 from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.tramite_cambios_vitto` 
where --id_poliza = 'AE-619391823'
num_documento_cliente in ('76880202','46651226','77272512','42990024','41598962') order by 5
--and id_producto =  'AX-8202';

Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_cambios_vitto` 
where trm_id_contratante = 'AX-10508507'
and trm_id_producto =  'AX-8202';
