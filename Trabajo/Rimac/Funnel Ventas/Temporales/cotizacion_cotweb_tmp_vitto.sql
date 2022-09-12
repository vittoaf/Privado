
CREATE OR REPLACE TABLE `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_cotweb_tmp_vitto`
AS
WITH unnested_data AS (
	SELECT 
		a.asesor,
		a.documentocliente,
		a.fecharegistro,
		a.hora,
		a.prospecto,
		a.codproducto,
		a.email,
		a.idcotizacion, 
		a.simulacionid, 
		a.vencimiento,
		a.estadocotizacion,
		a.originlead,
		dp.valor,
		dp.nombre,
		p.idvendedor,
		p.moneda,
		p.canal,
		p.CodProducto AS CodProducto_prod,
		rv.status
	FROM 
		--`rs-nprd-dlk-data-rwz-51a6.de__canales.TMP_COTIZADOR` a
		--`rs-nprd-dlk-dd-rwz-a406.de__canales.TMP_COTIZADOR` a
		`rs-nprd-dlk-dd-rwz-a406.de__canales.TMP_COTIZADOR` a
		CROSS JOIN UNNEST(request) AS r
		CROSS JOIN UNNEST(response_value) AS rv
		CROSS JOIN UNNEST(r.datosparticulares) AS dp
		CROSS JOIN UNNEST(r.producto) AS p
)
SELECT
	a.idcotizacion AS `id_cotizacion_origen`,
	ndoc.estadocotizacion,
	IFNULL(cfg_origenld.DESCRIP_HOMOLOGADO,'N.D.') AS `origen_lead_cliente`,
	COALESCE(email.valor,ndoc.idvendedor,a.email) AS `codigo_asesor`,
	COALESCE(email.valor,ndoc.idvendedor,a.email) AS `email_asesor`,
	SAFE_CAST(NULL AS STRING) AS `nro_doc_asesor`,
	SAFE_CAST(NULL AS STRING) AS `tip_doc_asesor`,
	IFNULL(ndoc.valor,a.documentocliente) AS `nro_doc_cliente`,
	SAFE_CAST(NULL AS STRING) AS `tip_doc_cliente`,
	'COTIZADOR WEB' AS origen_data,
	CAST(a.fecharegistro AS DATE) AS fecha_registro,
	'VAS' AS producto_recomendado,
	cfg_codprod.ID_HOMOLOGADO AS codproducto_final,
	99 AS `id_estado_cotizacion`,
	cfg_estado.ID_HOMOLOGADO AS `des_estado_cotizacion`,
	CAST(NULL AS STRING) AS `semaforo_precotizacion`,
	CAST(NULL AS STRING) AS `semaforo_cotizacion`,
	a.idcotizacion AS `id_asesoria_origen`,
	99 AS `id_estado_asesoria`,
	cfg_estado.ID_HOMOLOGADO AS `des_estado_asesoria`,
	/*BEGIN: Se agrego para logica de flg_presentada*/
	ndoc.moneda AS `des_moneda_presentada`,
	SAFE_CAST(NULL AS STRING) AS frecuencia_pago,
	SAFE_CAST(NULL AS STRING) AS cod_prod,
	SAFE_CAST(NULL AS INT64) AS id_pago,
	SAFE_CAST(NULL AS FLOAT64) AS importepp,
	SAFE_CAST(NULL AS FLOAT64) AS importep,
	SAFE_CAST(NULL AS FLOAT64) AS importe,
	SAFE_CAST(NULL AS FLOAT64) AS monto_pago,
	SAFE_CAST(NULL AS FLOAT64) AS prima_ahorro,
	SAFE_CAST(NULL AS FLOAT64) AS monto_descuento,
	SAFE_CAST(NULL AS FLOAT64) AS importe_cot,
	/*END: Se agrego para logica de flg_presentada*/
	CAST(prim.valor AS DECIMAL) AS `prima_anual_usd`,
	'0' AS pago_flg,
	CAST(NULL AS DATE) AS `fec_crea_pago`,
	CAST(a.fecharegistro AS DATE) AS fec_actualizacion_estado_asesoria,
	CAST(a.fecharegistro ||' '|| a.hora||':00' AS DATETIME) AS fechora_actualizacion_estado_asesoria,
	DATE_TRUNC(CAST(a.fecharegistro AS DATE), MONTH) AS periodo,
	--Variables nuevas para COTSAS: Inicio
	SAFE_CAST('FUERZA DE VENTA' AS STRING) AS canal,
	SAFE_CAST(NULL AS STRING) AS numpol
	--Variables nuevas para COTSAS: Fin
FROM 
	--`rs-nprd-dlk-data-rwz-51a6.de__canales.TMP_COTIZADOR` a
	--`rs-nprd-dlk-dd-rwz-a406.de__canales.TMP_COTIZADOR` a
	`rs-nprd-dlk-dd-rwz-a406.de__canales.TMP_COTIZADOR` a
	LEFT JOIN unnested_data email 
		ON (
			a.idcotizacion=email.idcotizacion 
			AND email.nombre='asesor'
		)
	LEFT JOIN unnested_data ndoc 
		ON (
			a.idcotizacion=ndoc.idcotizacion 
		AND ndoc.nombre='nrodocumento'
		)
	LEFT JOIN unnested_data tdoc 
		ON (
			a.idcotizacion=tdoc.idcotizacion 
			AND tdoc.nombre='tipodocumento'
		)
	LEFT JOIN unnested_data prim 
		ON (
			a.idcotizacion=prim.idcotizacion 
			AND prim.nombre='prima'
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.de__datalake.cfg_staging_maestra` cfg_origenld
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_origenld
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_origenld
		ON (
			cfg_origenld.ID_ORIGEN='JOURNEY' 
			AND cfg_origenld.TIPO_PARAMETRO='ORIGENLEAD' 
			AND cfg_origenld.CODIGO_ORIGEN=UPPER(a.originlead)
		)
	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.de__datalake.cfg_staging_maestra` cfg_estado
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_estado
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_estado
		ON (
			cfg_estado.ID_ORIGEN='COTWEB' 
			AND cfg_estado.TIPO_PARAMETRO='ESTADOCOTI' 
			AND cfg_estado.CODIGO_ORIGEN=email.status
		)
	
	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.de__datalake.cfg_staging_maestra` cfg_codprod
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_codprod
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_codprod
		ON (
			cfg_codprod.ID_ORIGEN='COTWEB' 
			AND cfg_codprod.TIPO_PARAMETRO='CODPROD' 
			AND cfg_codprod.CODIGO_ORIGEN=UPPER(TRIM(a.codproducto))
		)
--WHERE email.estadocotizacion=true;
;