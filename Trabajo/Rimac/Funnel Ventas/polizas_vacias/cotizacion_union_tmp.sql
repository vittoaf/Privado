--DROP TABLE IF EXISTS `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto_anterior`;
--DROP TABLE IF EXISTS `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto`;

CREATE OR REPLACE TABLE `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto`
as

SELECT
	ARRAY_AGG(id_cotizacion_origen ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS id_cotizacion_origen,
	ARRAY_AGG(j.origen_lead_cliente ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS origen_lead_cliente,
	j.codigo_asesor,
	j.email_asesor,
	j.nro_doc_asesor,
	j.tip_doc_asesor,
	j.nro_doc_cliente,
	j.tip_doc_cliente,
	MAX(j.origen_data) AS origen_data,
	ARRAY_AGG(j.fecha_registro ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS fecha_registro,
	ARRAY_AGG(j.producto_recomendado ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS producto_recomendado,
	ARRAY_AGG(j.codproducto_final ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS codproducto_final,
	ARRAY_AGG(j.id_estado_cotizacion ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS id_estado_cotizacion,
	ARRAY_AGG(j.des_estado_cotizacion ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS des_estado_cotizacion,
	ARRAY_AGG(j.semaforo_precotizacion ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS semaforo_precotizacion,
	ARRAY_AGG(j.semaforo_cotizacion ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS semaforo_cotizacion,
	ARRAY_AGG(j.id_asesoria_origen ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS id_asesoria_origen,
	ARRAY_AGG(j.id_estado_asesoria ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS id_estado_asesoria,
	ARRAY_AGG(j.des_estado_asesoria ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS des_estado_asesoria,
	ARRAY_AGG(j.des_moneda_presentada ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS des_moneda_presentada,
	/*BEGIN: Se agrego para logica de flg_presentada*/
	ARRAY_AGG(j.frecuencia_pago ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS frecuencia_pago,
	ARRAY_AGG(j.cod_prod ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS cod_prod,
	ARRAY_AGG(j.id_pago ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS id_pago,
	ARRAY_AGG(j.importepp ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS importepp,
	ARRAY_AGG(j.importep ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS importep,
	ARRAY_AGG(j.importe ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS importe,
	ARRAY_AGG(j.monto_pago ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS monto_pago,
	ARRAY_AGG(j.prima_ahorro ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS prima_ahorro,
	ARRAY_AGG(j.monto_descuento ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS monto_descuento,
	ARRAY_AGG(j.importe_cot ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS importe_cot,
	/*END: Se agrego para logica de flg_presentada*/
	ARRAY_AGG(j.prima_anual_usd ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS prima_anual_usd,
	ARRAY_AGG(j.pago_flg ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS pago_flg,
	ARRAY_AGG(j.fec_crea_pago ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS fec_crea_pago,
	MAX(j.fec_actualizacion_estado_asesoria) AS fec_actualizacion_estado_asesoria,
	--Variables nuevas para COTSAS
	ARRAY_AGG(j.canal ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS canal,
	ARRAY_AGG(j.cot_nro_poliza ORDER BY j.id_estado_asesoria_orden DESC, j.id_estado_cotizacion DESC, j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS numpol,
	--Variables nuevas para COTSAS
	SAFE_CAST(NULL AS FLOAT64) AS tasa_venta,
	periodo
FROM 
	--`rs-nprd-dlk-dt-stg-msrl-d0d7.stg_modelo_prospeccion.cotizacion_journey_tmp` j
	--`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_prospeccion.cotizacion_journey_tmp` j
	`rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_journey_tmp_vitto` j
GROUP BY
	j.codigo_asesor,
	j.email_asesor,
	j.nro_doc_asesor,
	j.tip_doc_asesor,
	j.nro_doc_cliente,
	j.tip_doc_cliente,
	periodo

UNION ALL

SELECT
	ARRAY_AGG(j.id_cotizacion_origen ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_cotizacion_origen,
	ARRAY_AGG(j.origen_lead_cliente ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS origen_lead_cliente,
	ARRAY_AGG(j.codigo_asesor ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS codigo_asesor,
	ARRAY_AGG(j.email_asesor ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS email_asesor,
	ARRAY_AGG(j.nro_doc_asesor ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS nro_doc_asesor,
	ARRAY_AGG(j.tip_doc_asesor ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS tip_doc_asesor,
	j.nro_doc_cliente,
	j.tip_doc_cliente,
	MAX(j.origen_data) AS origen_data,
	ARRAY_AGG(j.fecha_registro ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS fecha_registro,
	ARRAY_AGG(j.producto_recomendado ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS producto_recomendado,
	ARRAY_AGG(j.codproducto_final ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS codproducto_final,
	ARRAY_AGG(j.id_estado_cotizacion ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_estado_cotizacion,
	ARRAY_AGG(j.des_estado_cotizacion ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS des_estado_cotizacion,
	ARRAY_AGG(j.semaforo_precotizacion ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS semaforo_precotizacion,
	ARRAY_AGG(j.semaforo_cotizacion ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS semaforo_cotizacion,
	ARRAY_AGG(j.id_asesoria_origen ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_asesoria_origen,
	ARRAY_AGG(j.id_estado_asesoria ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_estado_asesoria,
	ARRAY_AGG(j.des_estado_asesoria ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS des_estado_asesoria,
	ARRAY_AGG(j.des_moneda_presentada ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS des_moneda_presentada,
	/*BEGIN: Se agrego para logica de flg_presentada*/
	ARRAY_AGG(j.frecuencia_pago ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS frecuencia_pago,
	ARRAY_AGG(j.cod_prod ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS cod_prod,
	ARRAY_AGG(j.id_pago ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_pago,
	ARRAY_AGG(j.importepp ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS importepp,
	ARRAY_AGG(j.importep ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS importep,
	ARRAY_AGG(j.importe ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS importe,
	ARRAY_AGG(j.monto_pago ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS monto_pago,
	ARRAY_AGG(j.prima_ahorro ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS prima_ahorro,
	ARRAY_AGG(j.monto_descuento ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS monto_descuento,
	ARRAY_AGG(j.importe_cot ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS importe_cot,
	/*END: Se agrego para logica de flg_presentada*/
	ARRAY_AGG(j.prima_anual_usd ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS prima_anual_usd,
	ARRAY_AGG(j.pago_flg ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS pago_flg,
	ARRAY_AGG(j.fec_crea_pago ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS fec_crea_pago,
	ARRAY_AGG(j.fec_actualizacion_estado_asesoria ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS fec_actualizacion_estado_asesoria,
	--Variables nuevas para COTSAS
	ARRAY_AGG(j.canal ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS canal,
	ARRAY_AGG(j.numpol ORDER BY j.estadocotizacion DESC,j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS numpol,
	--Variables nuevas para COTSAS
	SAFE_CAST(NULL AS FLOAT64) AS tasa_venta,
	periodo
FROM 
	--`rs-nprd-dlk-dt-stg-msrl-d0d7.stg_modelo_prospeccion.cotizacion_cotweb_tmp` j
	--`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_prospeccion.cotizacion_cotweb_tmp` j
	`rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_cotweb_tmp_vitto` j
GROUP BY
	-- j.codigo_asesor,
	-- j.email_asesor,
	-- j.nro_doc_asesor,
	-- j.tip_doc_asesor,
	j.nro_doc_cliente,
	j.tip_doc_cliente,
	periodo

UNION ALL


SELECT
	j.id_cotizacion_origen,
	ARRAY_AGG(j.origen_lead_cliente ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS origen_lead_cliente,
	j.codigo_asesor,
	j.email_asesor,
	j.nro_doc_asesor,
	j.tip_doc_asesor,
	j.nro_doc_cliente,
	j.tip_doc_cliente,
	MAX(j.origen_data) AS origen_data,
	ARRAY_AGG(j.fecha_registro ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS fecha_registro,
	ARRAY_AGG(j.producto_recomendado ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS producto_recomendado,
	ARRAY_AGG(j.codproducto_final ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS codproducto_final,
	ARRAY_AGG(j.id_estado_cotizacion ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_estado_cotizacion,
	ARRAY_AGG(j.des_estado_cotizacion ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS des_estado_cotizacion,
	ARRAY_AGG(j.semaforo_precotizacion ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS semaforo_precotizacion,
	ARRAY_AGG(j.semaforo_cotizacion ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS semaforo_cotizacion,
	ARRAY_AGG(j.id_asesoria_origen ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_asesoria_origen,
	ARRAY_AGG(j.id_estado_asesoria ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_estado_asesoria,
	ARRAY_AGG(j.des_estado_asesoria ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS des_estado_asesoria,
	ARRAY_AGG(j.des_moneda_presentada ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS des_moneda_presentada,
	/*BEGIN: Se agrego para logica de flg_presentada*/
	ARRAY_AGG(j.frecuencia_pago ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS frecuencia_pago,
	--ARRAY_AGG(j.cod_prod ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS cod_prod,
	j.cod_prod,
	ARRAY_AGG(j.id_pago ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_pago,
	ARRAY_AGG(j.importepp ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS importepp,
	ARRAY_AGG(j.importep ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS importep,
	ARRAY_AGG(j.importe ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS importe,
	ARRAY_AGG(j.monto_pago ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS monto_pago,
	ARRAY_AGG(j.prima_ahorro ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS prima_ahorro,
	ARRAY_AGG(j.monto_descuento ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS monto_descuento,
	ARRAY_AGG(j.importe_cot ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS importe_cot,
	/*END: Se agrego para logica de flg_presentada*/
	ARRAY_AGG(j.prima_anual_usd ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS prima_anual_usd,
	ARRAY_AGG(j.pago_flg ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS pago_flg,
	ARRAY_AGG(j.fec_crea_pago ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS fec_crea_pago,
	MAX(j.fec_actualizacion_estado_asesoria) AS fec_actualizacion_estado_asesoria,
	--Variables nuevas para COTSAS
	ARRAY_AGG(j.canal ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS canal,
	ARRAY_AGG(j.numpol ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS numpol,
	--Variables nuevas para COTSAS
	SAFE_CAST(NULL AS FLOAT64) AS tasa_venta,
	periodo
FROM 
	`rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto` j
WHERE numpol IS NOT NULL
GROUP BY
	j.id_cotizacion_origen,
	j.codigo_asesor,
	j.email_asesor,
	j.nro_doc_asesor,
	j.tip_doc_asesor,
	j.nro_doc_cliente,
	j.tip_doc_cliente,
	j.cod_prod,	
	periodo

UNION ALL

SELECT
	ARRAY_AGG(j.id_cotizacion_origen ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_cotizacion_origen,
	ARRAY_AGG(j.origen_lead_cliente ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS origen_lead_cliente,
	j.codigo_asesor,
	j.email_asesor,
	j.nro_doc_asesor,
	j.tip_doc_asesor,
	j.nro_doc_cliente,
	j.tip_doc_cliente,
	MAX(j.origen_data) AS origen_data,
	ARRAY_AGG(j.fecha_registro ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS fecha_registro,
	ARRAY_AGG(j.producto_recomendado ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS producto_recomendado,
	ARRAY_AGG(j.codproducto_final ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS codproducto_final,
	ARRAY_AGG(j.id_estado_cotizacion ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_estado_cotizacion,
	ARRAY_AGG(j.des_estado_cotizacion ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS des_estado_cotizacion,
	ARRAY_AGG(j.semaforo_precotizacion ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS semaforo_precotizacion,
	ARRAY_AGG(j.semaforo_cotizacion ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS semaforo_cotizacion,
	ARRAY_AGG(j.id_asesoria_origen ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_asesoria_origen,
	ARRAY_AGG(j.id_estado_asesoria ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_estado_asesoria,
	ARRAY_AGG(j.des_estado_asesoria ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS des_estado_asesoria,
	ARRAY_AGG(j.des_moneda_presentada ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS des_moneda_presentada,
	/*BEGIN: Se agrego para logica de flg_presentada*/
	ARRAY_AGG(j.frecuencia_pago ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS frecuencia_pago,
	--ARRAY_AGG(j.cod_prod ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS cod_prod,
	j.cod_prod,
	ARRAY_AGG(j.id_pago ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS id_pago,
	ARRAY_AGG(j.importepp ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS importepp,
	ARRAY_AGG(j.importep ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS importep,
	ARRAY_AGG(j.importe ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS importe,
	ARRAY_AGG(j.monto_pago ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS monto_pago,
	ARRAY_AGG(j.prima_ahorro ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS prima_ahorro,
	ARRAY_AGG(j.monto_descuento ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS monto_descuento,
	ARRAY_AGG(j.importe_cot ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS importe_cot,
	/*END: Se agrego para logica de flg_presentada*/
	ARRAY_AGG(j.prima_anual_usd ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS prima_anual_usd,
	ARRAY_AGG(j.pago_flg ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS pago_flg,
	ARRAY_AGG(j.fec_crea_pago ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS fec_crea_pago,
	MAX(j.fec_actualizacion_estado_asesoria) AS fec_actualizacion_estado_asesoria,
	--Variables nuevas para COTSAS
	ARRAY_AGG(j.canal ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS canal,
	ARRAY_AGG(j.numpol ORDER BY j.fechora_actualizacion_estado_asesoria DESC,j.id_cotizacion_origen DESC)[OFFSET(0)] AS numpol,
	--Variables nuevas para COTSAS
	SAFE_CAST(NULL AS FLOAT64) AS tasa_venta,
	periodo
FROM 
	`rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto` j
WHERE numpol IS NULL
GROUP BY
	j.codigo_asesor,
	j.email_asesor,
	j.nro_doc_asesor,
	j.tip_doc_asesor,
	j.nro_doc_cliente,
	j.tip_doc_cliente,
	j.cod_prod,
	periodo
	
UNION ALL 
 
SELECT
	ARRAY_AGG(j.id_cotizacion_origen ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS id_cotizacion_origen,
	ARRAY_AGG(j.origen_lead_cliente ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS origen_lead_cliente,
	ARRAY_AGG(j.codigo_asesor ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS codigo_asesor,
	ARRAY_AGG(j.email_asesor ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS email_asesor,
	ARRAY_AGG(j.nro_doc_asesor ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS nro_doc_asesor,
	ARRAY_AGG(j.tip_doc_asesor ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS tip_doc_asesor,
	j.nro_doc_cliente,
	j.tip_doc_cliente,
	MAX(j.origen_data) AS origen_data,
	ARRAY_AGG(j.fecha_registro ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS fecha_registro,
	ARRAY_AGG(j.producto_recomendado ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS producto_recomendado,
	ARRAY_AGG(j.codproducto_final ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS codproducto_final,
	ARRAY_AGG(j.id_estado_cotizacion ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS id_estado_cotizacion,
	ARRAY_AGG(j.des_estado_cotizacion ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS des_estado_cotizacion,
	ARRAY_AGG(j.semaforo_precotizacion ORDER BY j.R_GANADA ASC, j.regla ASC,j.id_estado_asesoria DESC)[OFFSET(0)] AS semaforo_precotizacion,
	ARRAY_AGG(j.semaforo_cotizacion ORDER BY j.R_GANADA ASC, j.regla ASC,j.id_estado_asesoria DESC)[OFFSET(0)] AS semaforo_cotizacion,
	ARRAY_AGG(j.id_asesoria_origen ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS id_asesoria_origen,
	ARRAY_AGG(j.id_estado_asesoria ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS id_estado_asesoria,
	ARRAY_AGG(j.des_estado_asesoria ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS des_estado_asesoria,
	ARRAY_AGG(j.des_moneda_presentada ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS des_moneda_presentada,
	/*BEGIN: Se agrego para logica de flg_presentada*/
	ARRAY_AGG(j.frecuencia_pago ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS frecuencia_pago,
	ARRAY_AGG(j.cod_prod ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS cod_prod,
	ARRAY_AGG(j.id_pago ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS id_pago,
	ARRAY_AGG(j.importepp ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS importepp,
	ARRAY_AGG(j.importep ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS importep,
	ARRAY_AGG(j.importe ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS importe,
	ARRAY_AGG(j.monto_pago ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS monto_pago,
	ARRAY_AGG(j.prima_ahorro ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS prima_ahorro,
	ARRAY_AGG(j.monto_descuento ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS monto_descuento,
	ARRAY_AGG(j.importe_cot ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS importe_cot,
	/*END: Se agrego para logica de flg_presentada*/
	ARRAY_AGG(j.prima_anual_usd ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS prima_anual_usd,
	ARRAY_AGG(j.pago_flg ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS pago_flg,
	ARRAY_AGG(j.fec_crea_pago ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS fec_crea_pago,
	ARRAY_AGG(j.fec_actualizacion_estado_asesoria ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS fec_actualizacion_estado_asesoria,
	--Variables nuevas para COTSAS
	ARRAY_AGG(j.canal ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS canal,
	ARRAY_AGG(j.numpol ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS numpol,
	--Variables nuevas para FINRISK
	ARRAY_AGG(j.tasa_venta ORDER BY j.R_GANADA ASC, j.regla ASC,j.fechora_actualizacion_estado_asesoria DESC)[OFFSET(0)] AS tasa_venta,	
	periodo
FROM 
`rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_finrisk_tmp_vitto` j
--where j.nro_doc_cliente ='08843608'
GROUP BY
	j.nro_doc_cliente,
	j.tip_doc_cliente,
	periodo;