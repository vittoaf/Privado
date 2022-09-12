--DROP TABLE IF EXISTS `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto_anterior`;
--DROP TABLE IF EXISTS `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto`;
 Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto`
where nro_doc_cliente = '47443265';
	
SELECT
	id_cotizacion_origen  AS id_cotizacion_origen,
	origen_lead_cliente  AS origen_lead_cliente,
	j.codigo_asesor,
	j.email_asesor,
	j.nro_doc_asesor,
	j.tip_doc_asesor,
	j.nro_doc_cliente,
	j.tip_doc_cliente,
	origen_data AS origen_data,
	fecha_registro   AS fecha_registro,
	producto_recomendado  AS producto_recomendado,
	codproducto_final  AS codproducto_final,
	id_estado_cotizacion  AS id_estado_cotizacion,
	des_estado_cotizacion  AS des_estado_cotizacion,
	semaforo_precotizacion  AS semaforo_precotizacion,
	semaforo_cotizacion  AS semaforo_cotizacion,
	id_asesoria_origen  AS id_asesoria_origen,
	id_estado_asesoria  AS id_estado_asesoria,
	des_estado_asesoria  AS des_estado_asesoria,
	des_moneda_presentada  AS des_moneda_presentada,
	/*BEGIN: Se agrego para logica de flg_presentada*/
	frecuencia_pago  AS frecuencia_pago,
	cod_prod  AS cod_prod,
	id_pago  AS id_pago,
	importepp  AS importepp,
	importep  AS importep,
	importe  AS importe,
	monto_pago  AS monto_pago,
	prima_ahorro  AS prima_ahorro,
	monto_descuento AS monto_descuento,
	importe_cot AS importe_cot,
	/*END: Se agrego para logica de flg_presentada*/
	prima_anual_usd AS prima_anual_usd,
	pago_flg AS pago_flg,
	fec_crea_pago AS fec_crea_pago,
	fec_actualizacion_estado_asesoria AS fec_actualizacion_estado_asesoria,
	--Variables nuevas para COTSAS
	canal AS canal,
	cot_nro_poliza AS numpol,
	--Variables nuevas para COTSAS
	SAFE_CAST(NULL AS FLOAT64) AS tasa_venta,
	periodo
FROM 
	`rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_journey_tmp_vitto` j
  where j.nro_doc_cliente = '47443265' --'45106974'

