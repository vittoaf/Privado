CREATE OR REPLACE TABLE `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_unionall_vitto` 
AS
Select id_cotizacion_origen, nro_doc_cliente,codproducto_final,pago_flg,'Journey' Origen
from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_journey_tmp_vitto` 
UNION ALL
select id_cotizacion_origen,nro_doc_cliente,codproducto_final,pago_flg ,'CotWeb' Origen
from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_cotweb_tmp_vitto` 
UNION ALL
select id_cotizacion_origen,nro_doc_cliente,codproducto_final,pago_flg ,'FinRisk' Origen
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_finrisk_tmp_vitto` 
UNION ALL
select id_cotizacion_origen,nro_doc_cliente,codproducto_final,pago_flg ,'SAS' Origen
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto`