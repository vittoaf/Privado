
CREATE OR REPLACE TABLE `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_journey_tmp_vitto`
AS
WITH pre_cot_data AS (
	SELECT 
		tmp_pre.id_adn,MAX(tmp_pre.id_precotizacion) AS id_precotizacion
	FROM 
		--`rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` tmp_pre
		--`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` tmp_pre
		`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` tmp_pre
	GROUP BY 
		tmp_pre.id_adn
),

pre_pag_data AS (
	SELECT 
		tmp_pag.id_cotizacion, 
		MAX(tmp_pag.id_pago) AS id_pago
	FROM 
		--`rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` tmp_pag
		--`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` tmp_pag
		`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` tmp_pag
	WHERE
		(tmp_pag.estado > -1) 
		OR (tmp_pag.estado = -1 AND tmp_pag.mensaje ='Se inactiva por toc negativo')
	GROUP BY 
		tmp_pag.id_cotizacion
)

SELECT
	CAST(cot.id_cotizacion AS STRING) AS `id_cotizacion_origen`,
	IFNULL(cfg_origenld.DESCRIP_HOMOLOGADO,'N.D.') AS `origen_lead_cliente`,
	CAST(usu.idusuario AS STRING) AS `codigo_asesor`,
	usu.correo AS `email_asesor`,
	seg.documento AS `nro_doc_asesor`,
	IFNULL(REPLACE(cfg_tipdoca.DESCRIP_ORIGEN,'.',''),'N.D.') AS `tip_doc_asesor`,
	per.nro_documento AS `nro_doc_cliente`,
	IFNULL(REPLACE(cfg_tipdocc.DESCRIP_ORIGEN,'.',''),'N.D.') AS `tip_doc_cliente`,
	'JOURNEY' AS origen_data,
	CASE
		WHEN (b.id_estado > 0 AND b.id_estado < 4 AND pre.id_precotizacion IS NOT NULL) 
			THEN CAST(pre.fec_crea AS DATE) 
		ELSE CAST(b.fec_modif AS DATE)
	END AS fecha_registro,
	pre.cod_prod AS producto_recomendado,
	pro.codprodax AS codproducto_final,
	cot.estado AS id_estado_cotizacion,
	cfg_estado.DESCRIP_HOMOLOGADO AS des_estado_cotizacion,
	pre.semaforo AS `semaforo_precotizacion`,
	cot.semaforo AS `semaforo_cotizacion`,
	CAST(b.id_bitacora AS STRING) AS `id_asesoria_origen`,
	CASE
		WHEN ((b.id_estado > 0) AND (b.id_estado < 4) AND (pre.id_precotizacion IS NOT NULL)) 
			THEN 13
		ELSE `b`.`id_estado` 
	END AS `id_estado_asesoria`,
	CASE
		WHEN ((b.id_estado > 0) AND (b.id_estado < 4) AND (pre.id_precotizacion IS NOT NULL)) 
			THEN 'RECOMENDAR PRODUCTO'
		ELSE est.nombre_estado 
	END AS `des_estado_asesoria`,
	--MONEDA BEGIN
	--pag.tipo_pago,
	--pag.cod_moneda,
	--cot.tipo_moneda,
	--END MONEDA
	/*BEGIN: Se agrego para logica de flg_presentada*/
	(
	CASE
		WHEN (pag.tipo_pago = 1 OR pag.tipo_pago = 3) 
			THEN ( 
				CASE 
					WHEN pag.cod_moneda IS NULL 
						THEN (
							CASE 
								WHEN (cot.tipo_moneda = 1) THEN 'USD'
								WHEN (cot.tipo_moneda = 2) THEN 'PEN'
							END
						) 
					ELSE pag.cod_moneda 
				END
			)
		WHEN pag.tipo_pago = 2
			THEN (
				CASE
					WHEN (cot.tipo_moneda = 1) THEN 'USD'
					WHEN (cot.tipo_moneda = 2) THEN 'PEN'
				END
			)
		WHEN (pag.tipo_pago IS NULL AND pag.cod_moneda IS NULL AND cot.tipo_moneda IS NOT NULL)
			THEN (
				CASE
					WHEN (cot.tipo_moneda = 1) THEN 'USD'
					WHEN (cot.tipo_moneda = 2) THEN 'PEN'
				END
			)
	END
	) AS `des_moneda_presentada`, -- Moneda Pago utilizada por Monica Reyes
	cot.frecuencia_pago,
	pre.cod_prod,
	CASE
		--WHEN cot.estado = 3 THEN CAST(IFNULL(cot.numero_poliza,cot.id_cotizacion) AS STRING)
		WHEN cot.estado = 3 THEN COALESCE(numero_poliza_ae, CAST(cot.numero_poliza AS STRING) ,CAST(cot.id_cotizacion AS STRING) ) 
	END AS cot_nro_poliza,
	pag.id_pago,
	pripag.importe importepp,
	pag.importe importep,
	CASE 
		WHEN pag.importe IS NULL 
			THEN (
				CASE 
					WHEN pripag.importe IS NULL
						THEN (cot.monto_pago+COALESCE(cot.prima_ahorro,0) + COALESCE(cot.monto_descuento,0))
					ELSE pripag.importe 
				END
			)
		ELSE pag.importe 
	END AS importe,
	cot.monto_pago,
	cot.prima_ahorro,
	cot.monto_descuento,
	(cot.monto_pago+COALESCE(cot.prima_ahorro,0) + COALESCE(cot.monto_descuento,0)) AS importe_cot,
	ROUND(
		CASE
			WHEN cot.frecuencia_pago = '1'
				THEN 
					CAST(
						(
							(
								CASE 
									WHEN pag.importe IS NULL 
										THEN (
											CASE 
												WHEN pripag.importe IS NULL
													THEN (cot.monto_pago+COALESCE(cot.prima_ahorro,0) + COALESCE(cot.monto_descuento,0))
												ELSE pripag.importe 
											END
										)
								ELSE pag.importe END
							) * 12.0
						) AS DECIMAL
					)
			WHEN cot.frecuencia_pago = '12' AND LOWER(pre.cod_prod)='flexivida' -- Si es FlexiVida de frecuencia Anual, se utiliza el campo "prima" de Data_Journey
				THEN CAST(IFNULL(cot.monto_pago,0.00) + IFNULL(cot.monto_descuento,0.00) AS DECIMAL)
			ELSE (
				CASE
					WHEN cot.frecuencia_pago = '6'
						THEN 
							CAST(
								((
									CASE 
										WHEN pag.importe IS NULL 
											THEN (
												CASE 
													WHEN pripag.importe IS NULL
														THEN (cot.monto_pago+COALESCE(cot.prima_ahorro,0) + COALESCE(cot.monto_descuento,0))
													ELSE pripag.importe 
												END
											)
										ELSE pag.importe 
									END
								) * 2.0) 
							AS DECIMAL)
				ELSE 
					CAST(
						(
							CASE 
								WHEN pag.importe IS NULL 
									THEN (
										CASE 
											WHEN pripag.importe IS NULL
												THEN (cot.monto_pago+COALESCE(cot.prima_ahorro,0) + COALESCE(cot.monto_descuento,0))
											ELSE pripag.importe 
										END
									)
								ELSE pag.importe 
							END
						) AS DECIMAL
					)
				END
			) END,2
	) AS `prima_anual_usd`,
	CASE 
		WHEN pripag.importe>0 OR pag.importe>0
		THEN '1' ELSE '0'
	END pago_flg,
	CASE 
		WHEN pag.fec_crea IS NULL 
		THEN CAST(pripag.fec_crea AS DATE)
		ELSE CAST(SUBSTRING(pag.fec_crea,1,10) AS DATE) 
	END AS `fec_crea_pago`,
	/*END: Se agrego para logica de flg_presentada*/
	CASE
		WHEN (b.id_estado > 0 AND b.id_estado < 4 AND pre.id_precotizacion IS NOT NULL)
			THEN CAST(pre.fec_crea AS DATE) 
		ELSE CAST(b.fec_modif AS DATE)
	END AS fec_actualizacion_estado_asesoria,
	CASE
		WHEN (b.id_estado > 0 AND b.id_estado < 4 AND pre.id_precotizacion IS NOT NULL)
			THEN CAST(pre.fec_crea AS DATETIME) 
		ELSE CAST(b.fec_modif AS DATETIME)
	END AS fechora_actualizacion_estado_asesoria,
	CASE
		WHEN (b.id_estado > 0 AND b.id_estado < 4 AND pre.id_precotizacion IS NOT NULL)
			THEN DATE_TRUNC(CAST(pre.fec_crea AS DATE), MONTH) 
		ELSE DATE_TRUNC(CAST(b.fec_modif AS DATE), MONTH)
	END AS Periodo,
	--Variables nuevas para COTSAS: Inicio
	SAFE_CAST('FUERZA DE VENTA' AS STRING) AS canal,
	SAFE_CAST(NULL AS STRING) AS id_tramite,

	CASE
		WHEN ((b.id_estado > 0) AND (b.id_estado < 4) AND (pre.id_precotizacion IS NOT NULL)) 
			THEN 4
		ELSE 
			CASE 
				WHEN b.id_estado = 1  THEN 1 
				WHEN b.id_estado = 2  THEN 2 
				WHEN b.id_estado = 3  THEN 3 
				WHEN b.id_estado = 4  THEN 5 
				WHEN b.id_estado = 12 THEN 6 
				WHEN b.id_estado = 6  THEN 7
				WHEN b.id_estado = 7  THEN 8
				WHEN b.id_estado = 5  THEN 9 
				WHEN b.id_estado = 0  THEN 10   
			END 
	END AS id_estado_asesoria_orden


	--Variables nuevas para COTSAS: Fin
FROM 
	--`rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_bitacora` b
	--`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_bitacora` b
	`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_bitacora` b

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_agendamiento` a
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_agendamiento` a
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_agendamiento` a
		ON (
			(b.id_cliente = a.id_cliente) 
			AND (b.usu_crea = a.id_usuario)
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_estado` est
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_estado` est
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_estado` est
		ON (
			est.id_estado = b.id_estado
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_persona` per
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_persona` per
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_persona` per
		ON (
			a.id_persona = per.id_persona
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_seg_usuario` seg
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_seg_usuario` seg
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_seg_usuario` seg
		ON (
			a.id_usuario = seg.idusuario
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_usuario` usu
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_usuario` usu
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_usuario` usu
		ON (
			a.id_usuario = usu.idusuario
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_adn` adn
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_adn` adn
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_adn` adn
		ON (
			b.id_adn = adn.id_adn
		)
	LEFT JOIN pre_cot_data max_pre
		ON (
			max_pre.id_adn = adn.id_adn
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` pre
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` pre
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` pre
		ON (
			pre.id_adn = adn.id_adn 
			AND pre.id_precotizacion=max_pre.id_precotizacion
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_cotizacion` cot
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_cotizacion` cot
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_cotizacion` cot
		ON (
			b.id_cotizacion = cot.id_cotizacion
		)
	LEFT JOIN pre_pag_data max_pag
		ON (
			max_pag.id_cotizacion = b.id_cotizacion
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` pag
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` pag
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` pag
		ON (
			pag.id_cotizacion = b.id_cotizacion 
			AND pag.id_pago = max_pag.id_pago
		)
	/*BEGIN: Se agrego para logica de flg_presentada*/
	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_primer_pago` pripag
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_primer_pago` pripag
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_primer_pago` pripag
		ON (
			pripag.id_cotizacion = b.id_cotizacion
		)

	/*END: Se agrego para logica de flg_presentada*/
	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_producto` pro
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_producto` pro
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_producto` pro
		ON (
			cot.`cod_producto` = pro.`nombre`
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.de__datalake.cfg_staging_maestra` cfg_estado
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_estado
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_estado
		ON (
			cfg_estado.ID_ORIGEN='JOURNEY' 
			AND cfg_estado.TIPO_PARAMETRO='ESTADOCOTI' 
			AND cfg_estado.CODIGO_ORIGEN=CAST(cot.estado AS STRING)
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.de__datalake.cfg_staging_maestra` cfg_tipdoca
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_tipdoca
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_tipdoca
		ON (
			cfg_tipdoca.ID_ORIGEN='JOURNEY' 
			AND cfg_tipdoca.TIPO_PARAMETRO='TIPODOC' 
			AND cfg_tipdoca.CODIGO_ORIGEN=seg.tipodocumento
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.de__datalake.cfg_staging_maestra` cfg_tipdocc
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_tipdocc
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_tipdocc
		ON (
			cfg_tipdocc.ID_ORIGEN='JOURNEY' 
			AND cfg_tipdocc.TIPO_PARAMETRO='TIPODOC' 
			AND cfg_tipdocc.CODIGO_ORIGEN=CAST(per.id_tipodocumento AS STRING)
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.de__datalake.cfg_staging_maestra` cfg_origenld
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_origenld
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_origenld
		ON (
			cfg_origenld.ID_ORIGEN='JOURNEY' 
			AND cfg_origenld.TIPO_PARAMETRO='ORIGENLEAD' 
			AND cfg_origenld.CODIGO_ORIGEN=UPPER(per.origen)
		)
WHERE
	((b.`id_estado` > 0) OR ((b.`id_estado` = 0) AND (b.`id_adn` IS NOT NULL)))
	AND (
		usu.correo NOT IN (
			'xt1568@rimac.com.pe',
			'daniella.poppe@rimac.com.pe',
			'andrea.garavito@rimac.com.pe',
			'yoisy.vasquez@rimac.com.pe',
			'yumiko.asato@rimac.com.pe',
			'lcastanedar@rimac.com.pe',
			'mirella.concha@rimac.com.pe',
			'carlos.zeballos@rimac.com.pe',
			'xt2385@rimac.com.pe',
			'paloma.urbina@rimac.com.pe',
			'xt1765@rimac.com.pe',
			'wendy.su@rimac.com.pe',
			'xt1931@rimac.com.pe',
			'xt1702@rimac.com.pe',
			'cesar.acuna@rimac.com.pe',
			'miguel.rodriguezb@rimac.com.pe',
			'miguel.corahua@rimac.com.pe',
			'abelzu@rimac.com.pe',
			'pedro.valle@rimac.com.pe',
			'eatanta@indracompany.com',
			'xt1874@rimac.com.pe',
			'david.inga@rimac.com.pe',
			'xt1403@rimac.com.pe',
			'antonela.mendezh@rimac.com.pe',
			'shayla.gomez@rimac.com.pe',
			'xt0033@rimac.com.pe',
			'mfarfanr@rimac.com.pe',
			'maria.altet@rimac.com.pe',
			'jorge.vilca@rimac.com.pe',
			'melissa.ponce@rimac.com.pe',
			'xt2649@rimac.com.pe',
			'xt2844@rimac.com.pe',
			'ruben.valdivieso@rimac.com.pe',
			'xt2603@rimac.com.pe'
		) 
	)
	AND
	(
		(
			(
				CAST(TRIM(REGEXP_REPLACE(seg.documento,'[^0-9]','')) AS BIGINT) = CAST(TRIM(REGEXP_REPLACE(per.nro_documento,'[^0-9]','')) AS BIGINT) 
				AND pag.num_obligacion IS NOT NULL
			) -- estado_pago='Procesado'
			or
			(
				CAST(TRIM(REGEXP_REPLACE(seg.documento,'[^0-9]','')) AS BIGINT) <> CAST(TRIM(REGEXP_REPLACE(per.nro_documento,'[^0-9]','')) AS BIGINT) 
			)
		)
		AND per.nro_documento NOT LIKE '999999%'
		--AND seg.codigo_acselx NOT IN ('0000000','0','1962466','2828128','5147034','7403602','8843744') --Cambio solicitado por JHAIR Osorio
		AND seg.codigo_acselx NOT IN ('0000000','0')
	);