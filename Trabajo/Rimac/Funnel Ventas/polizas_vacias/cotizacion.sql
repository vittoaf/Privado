--DROP TABLE IF EXISTS `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_vitto`;
--DROP TABLE IF EXISTS `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_vitto_anterior`;

CREATE OR REPLACE TABLE `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_vitto`
AS
WITH persona_data AS (
	SELECT 
		a.id_persona, 
		b.tip_documento, 
		b.num_documento, 
		MAX(c.correo_corporativo) AS correo_corporativo,
		MAX(c.cod_sap) AS cod_sap,
		MAX(a.cod_acselx) AS cod_ax
	FROM 
		--`rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_persona.persona` a
		--`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		CROSS JOIN UNNEST (a.documento_identidad) b
		--LEFT JOIN `rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_persona.colaborador` c
		--LEFT JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.colaborador` c
		LEFT JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.colaborador` c
			ON (a.id_persona=c.id_persona)
	WHERE 
		b.ind_documento_principal = '1' -- 1: DNI. Si se desea todos los documentos asociados, se quita este filtro.
		--AND UPPER(TRIM(b.tip_documento))='DNI'
		AND IFNULL(a.bq__soft_deleted,false)=false 
		AND IFNULL(c.bq__soft_deleted,false)=false
	GROUP BY 
		a.id_persona, 
		b.tip_documento, 
		b.num_documento
),
personas_correo_data AS (
	SELECT 
		UPPER(c.correo_corporativo) AS correo_corporativo,
		ARRAY_AGG(a.cod_acselx ORDER BY c.periodo DESC)[OFFSET(0)] AS cod_ax,
		ARRAY_AGG(c.id_persona ORDER BY c.periodo DESC)[OFFSET(0)] AS id_persona
	FROM 
		--`rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_persona.colaborador` c
		--`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.colaborador` c
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.colaborador` c

		--LEFT JOIN `rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_persona.persona` a
		--LEFT JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		LEFT JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
			ON (a.id_persona=c.id_persona)
		CROSS JOIN UNNEST (a.documento_identidad) b
	-- WHERE c.correo_corporativo='DIANA.DIAZ@RIMAC.COM.PE'
	GROUP BY 
		UPPER(c.correo_corporativo)
),
email_dni_data AS (
	SELECT 
		c.correo_corporativo
		,ARRAY_AGG(b.num_documento ORDER BY a.id_persona DESC)[OFFSET(0)] AS dni
	FROM 
		--`rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_persona.persona` a
		--`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		CROSS JOIN UNNEST (a.documento_identidad) b

		--LEFT JOIN `rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_persona.colaborador` c
		--LEFT JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.colaborador` c
		LEFT JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.colaborador` c
			ON (a.id_persona=c.id_persona)
	WHERE 
		b.ind_documento_principal = '1' -- 1: DNI. Si se desea todos los documentos asociados, se quita este filtro.
		AND UPPER(TRIM(b.tip_documento))='DNI'
		AND UPPER(TRIM(c.correo_corporativo)) LIKE '%RIMAC.COM.PE%'
		AND IFNULL(a.bq__soft_deleted,false)=false 
		AND IFNULL(c.bq__soft_deleted,false)=false
	GROUP BY 
		c.correo_corporativo
),
jerarquia_ffvv_data AS (
	SELECT
		jfv.id_persona,
		jfv.dsc_periodo,
		MAX(jfv.id_jerarquia_fuerza_ventas) AS id_jerarquia_fuerza_ventas,
		MAX(jfv.id_intermediario_rimac) AS id_intermediario,
		MAX(jfv.id_canal) AS id_canal,
		MAX(jfv.dsc_canal) AS dsc_canal,
		MAX(jfv.id_subcanal) AS id_subcanal,
		MAX(jfv.dsc_subcanal) AS dsc_subcanal,
		MAX(jfv.dsc_localidad) AS dsc_localidad,
		MAX(jfv.dsc_agencia) AS dsc_agencia,
		MAX(jfv.dsc_nombre_jefe2) AS dsc_nombre_jefe2,
		MAX(jfv.dsc_nombre_jefe1) AS dsc_nombre_jefe1,
		MAX(jfv.cod_sap) AS cod_sap,
		MAX(jfv.tip_documento) AS tip_documento,
		MAX(jfv.num_documento) AS num_documento,
		MAX(jfv.dsc_nombre_corto) AS dsc_nombre_corto,
		MAX(jfv.dsc_motivo_estado) AS dsc_motivo_estado
	FROM 
		--`rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv
		--`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv
	WHERE 
		LOWER(jfv.dsc_tipo_puesto)='asesor'
	GROUP BY 
		jfv.id_persona,
		jfv.dsc_periodo
),
/*
jerarquia_consolidado_vigente_data AS (
	SELECT 
		b.periodo
		,SAFE_CAST(TRIM(REGEXP_REPLACE(b.dni,'[^0-9 ]','')) AS BIGINT) AS dni
		,ARRAY_AGG(b.agencia ORDER BY b.periodo DESC)[OFFSET(0)] AS agencia
		,ARRAY_AGG(b.localidad ORDER BY b.periodo DESC)[OFFSET(0)] AS localidad
		,ARRAY_AGG(b.canal ORDER BY b.periodo DESC)[OFFSET(0)] AS canal
		,ARRAY_AGG(b.sap ORDER BY b.periodo DESC)[OFFSET(0)] AS sap
		,ARRAY_AGG(b.nombres||' '||b.apellido_paterno||' '||b.apellido_materno ORDER BY b.periodo DESC)[OFFSET(0)] AS nombre_asesor
		,ARRAY_AGG(b.ax ORDER BY b.periodo DESC)[OFFSET(0)] AS ax
		,ARRAY_AGG(b.edad ORDER BY b.periodo DESC)[OFFSET(0)] AS edad
		,ARRAY_AGG(b.ingreso ORDER BY b.periodo DESC)[OFFSET(0)] AS ingreso
		,ARRAY_AGG(b.meses ORDER BY b.periodo DESC)[OFFSET(0)] AS meses
		,ARRAY_AGG(b.user_red ORDER BY b.periodo DESC)[OFFSET(0)] AS user_red
		,ARRAY_AGG(b.categoria ORDER BY b.periodo DESC)[OFFSET(0)] AS categoria
		,ARRAY_AGG(b.estado_actual ORDER BY b.periodo DESC)[OFFSET(0)] AS estado_actual
		,ARRAY_AGG(b.motivo_estado ORDER BY b.periodo DESC)[OFFSET(0)] AS motivo_estado
		,ARRAY_AGG(b.nombre_jefe ORDER BY b.periodo DESC)[OFFSET(0)] AS supervisor
		,ARRAY_AGG(b.nombre_jefe2 ORDER BY b.periodo DESC)[OFFSET(0)] AS gerente_agencia
		,ARRAY_AGG(b.nombre_jefe3 ORDER BY b.periodo DESC)[OFFSET(0)] AS subgerente
	FROM 
		--`rs-nprd-dlk-data-rwz-51a6.de__canales.jerarquia_consolidado_vigente` b
		--`rs-nprd-dlk-dd-rwz-a406.de__canales.jerarquia_consolidado_vigente` b
		`rs-nprd-dlk-dd-rwz-a406.de__canales.jerarquia_consolidado_vigente` b
	WHERE 
		LOWER(TRIM(tipo_puesto))='asesor' 
		AND LOWER(TRIM(modelo))='vida'
	GROUP BY 
		b.periodo,
		SAFE_CAST(TRIM(REGEXP_REPLACE(b.dni,'[^0-9 ]','')) AS BIGINT)
),
*/
jerarquia_ffvv_data_xcodsap AS (  -- Jerarquia de la ffvv agrupado por el codigo SAP
	SELECT
		CAST(jfv.cod_sap AS STRING) AS cod_sap ,
		max(jfv.id_intermediario_rimac) AS id_intermediario_rimac
	FROM 
		--`rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv
	WHERE LOWER(jfv.dsc_tipo_puesto)='asesor'
        AND jfv.cod_sap <> 0
	GROUP BY CAST(jfv.cod_sap AS STRING)
),
tramites_hist_rentas AS (  --  Actualizar el intermediario para el caso de los tramites de RRVV
    SELECT thr.nro_tramite_solicitud ,
           trim(thr.cod_sap_asesor_final) AS cod_sap_asesor_final ,
           jfv.id_intermediario_rimac AS id_intermediario ,
           trim(cast(cod_producto_final AS STRING))  AS  cod_producto_final 
    --FROM `rs-nprd-dlk-data-rwz-51a6.de__canales.tramites_hist_rentas` thr 
    FROM `rs-nprd-dlk-dd-rwz-a406.de__canales.tramites_hist_rentas` thr 
    LEFT JOIN jerarquia_ffvv_data_xcodsap jfv ON ( jfv.cod_sap  = trim(thr.cod_sap_asesor_final) )
    WHERE trim(cast(cod_producto_final AS STRING)) IN ('8822','8827','8102','8917','8901')  -- solo productos del funnel de Rentas 
      AND COALESCE( SAFE_CAST(thr.cod_sap_asesor_final AS NUMERIC) || '' , '') <> ''  -- que presente un codigo sap correcto
      AND thr.nro_tramite_solicitud <> 'PENDIENTE' 
),
tramites_data AS (
	SELECT
		DATE_TRUNC(CAST(trm.FECCREACION AS DATE), MONTH) AS Periodo,
		-- obtener el id_intermediario
		CASE 
            WHEN thr.nro_tramite_solicitud IS NOT NULL THEN CAST(thr.id_intermediario AS INT)
		    ELSE CAST(trm.NUMIDBROKER AS INT) 
        END AS id_intermediario,
		--
		TRIM(trm.DOCCLIENTE) AS dni_cliente,
		TRIM(trm.CODPRODUCTO) AS cod_producto,
		MAX(CAST(COALESCE(trm.FECSOLICITUD,trm.FECCREACION) AS DATE)) AS fecha_presentada,
		ARRAY_AGG(trm.NUMTRAMITE ORDER BY trm.NUMPOLIZA DESC, trm.FECCREACION DESC)[OFFSET(0)] AS id_tramite
	FROM 
		--`rs-nprd-dlk-data-rwz-51a6.bdwf__appnote.TRAMITE` trm
		--`rs-nprd-dlk-dd-rwz-a406.bdwf__appnote.TRAMITE` trm
		`rs-nprd-dlk-dd-rwz-a406.bdwf__appnote.TRAMITE` trm
	LEFT JOIN tramites_hist_rentas  thr ON ( thr.nro_tramite_solicitud = trm.numtramite )  --  tramites de rentas
	GROUP BY 
		DATE_TRUNC(CAST(trm.FECCREACION AS DATE), MONTH),
		CASE 
            WHEN thr.nro_tramite_solicitud IS NOT NULL THEN CAST(thr.id_intermediario AS INT)
		    ELSE CAST(trm.NUMIDBROKER AS INT) 
        END ,
		TRIM(trm.DOCCLIENTE),
		TRIM(trm.CODPRODUCTO)
),
tipo_cambio_data AS (
	SELECT 
		FORMAT_DATE('%Y%m', tc.fechoracambio ) AS periodo,
		MAX(CAST(tc.TASACAMBIO AS decimal)) AS tasacambio
	FROM 
		--`rs-nprd-dlk-data-rwz-51a6.prod__acselx.TASA_CAMBIO` tc
		--`rs-nprd-dlk-dd-rwz-a406.prod__acselx.TASA_CAMBIO` tc
		`rs-nprd-dlk-dd-rwz-a406.prod__acselx.TASA_CAMBIO` tc
	WHERE 
		tipotasa = 'M' 
		AND codmoneda = 'USD' 
		AND codmonedafin = 'SOL'
		and FORMAT_DATE('%Y%m', tc.fechoracambio ) >= '201001' 
		-- AND FORMAT_DATE('%Y%m', tc.fechoracambio ) <= '2025'
	GROUP BY 
		FORMAT_DATE('%Y%m', tc.fechoracambio)
),
producto_ax_data AS (
	SELECT 
	po.cod_producto,
	ARRAY_AGG(pr.id_producto ORDER BY coalesce(pr.id_estado,'ACT2') ASC,pr.id_producto DESC)[OFFSET(0)] as id_producto,
	ARRAY_AGG(po.nom_producto ORDER BY coalesce(pr.id_estado,'ACT2') ASC,pr.id_producto DESC)[OFFSET(0)] as nom_producto
	FROM `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_producto.producto` AS pr
	CROSS JOIN UNNEST (pr.producto_origen) AS po
	WHERE pr.id_origen = "AX"
	GROUP BY po.cod_producto
),
canal_data AS (
	SELECT ec.dsc_canal,max(ec.id_canal) as id_canal
	FROM `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.estructura_canal` ec
	group by ec.dsc_canal
),
first_final_cotizacion AS (
SELECT
	CAST(ROW_NUMBER() OVER() AS String) AS id_cotizacion,
	a.id_cotizacion_origen,
	a.origen_lead_cliente,
	cnl.id_canal as id_canal_cot,
	a.canal as des_canal_cot,--Campos nuevos por ajuste SAS
	a.numpol as numpol_ext_cot,--Campos nuevos por ajuste SAS
	IFNULL(per_ase_j.id_persona,per_ase_c.id_persona) AS id_persona_asesor,
	-- IFNULL(per_ase_j.cod_sap,per_ase_c.cod_sap) AS cod_sap,
	int.id_jerarquia_fuerza_ventas,
	CAST(COALESCE(int.id_intermediario, per_ase_j.cod_ax, per_ase_c.cod_ax) AS INT) AS id_intermediario,
	-- a.codigo_asesor,
	-- a.email_asesor,
	IF(coalesce(a.nro_doc_asesor,'')!='',a.nro_doc_asesor,email.dni) AS des_nro_doc_asesor,
	IF(a.origen_data in ('COTIZADOR WEB','FINRISK RENTA VITALICIA','FINRISK RENTA GARANTIZADA') AND email.dni IS NOT NULL,'DNI',a.tip_doc_asesor) AS des_tip_doc_asesor,
	per_cli.id_persona AS id_persona_cliente, -- per_cli.id_persona
	a.nro_doc_cliente AS des_nro_doc_cliente,
	a.tip_doc_cliente AS des_tip_doc_cliente,
	a.origen_data,
	a.fecha_registro AS fecha_estado_asesoria,
	a.producto_recomendado,
	pro.id_producto AS id_producto_final,
	a.id_estado_cotizacion,
	a.des_estado_cotizacion,
	IFNULL(a.semaforo_precotizacion,a.semaforo_cotizacion) AS scoring,
	a.id_asesoria_origen,
	a.id_estado_asesoria,
	a.des_estado_asesoria,
	a.des_moneda_presentada,
	a.pago_flg,
	CASE
		WHEN trm.id_tramite IS NOT NULL OR (a.origen_data='JOURNEY' AND a.pago_flg='1')
			THEN COALESCE(trm.fecha_presentada, a.fec_crea_pago, a.fec_actualizacion_estado_asesoria) --include fecha Pago
	END AS fecha_presentada,
	a.prima_anual_usd AS prima_anual,
	CASE
		WHEN UPPER(TRIM(a.des_moneda_presentada))='PEN'
			THEN round(a.prima_anual_usd/tc.tasacambio,2)
		ELSE a.prima_anual_usd
	END AS prima_anual_usd,
	CASE
		WHEN trm.id_tramite IS NOT NULL OR (a.origen_data='JOURNEY' AND a.pago_flg='1')
			THEN (
				CASE
					WHEN UPPER(TRIM(a.des_moneda_presentada))='PEN'
						THEN round(a.prima_anual_usd/tc.tasacambio,2)
					ELSE a.prima_anual_usd
				END
			)
	END AS prima_anual_presentada_usd,
	/*BEGIN: Se agrego para logica de flg_presentada*/
	-- si tramite es NULL pero tiene id_pago, importe, monto_pago --> flg 1
	-- si tramite es NOT NULL, no se valida el pago
	CASE 
		WHEN trm.id_tramite IS NOT NULL OR (a.origen_data='JOURNEY' AND a.pago_flg='1')
			THEN '1' 
		ELSE '0'
	END AS flg_presentada, 
	/*END: Se agrego para logica de flg_presentada*/
	tc.tasacambio,
	COALESCE(a.fec_crea_pago,trm.fecha_presentada,a.fec_actualizacion_estado_asesoria) AS fecha_tasa_cambio,
	trm.id_tramite as id_tramite,
	trm.Periodo as periodo_tramite,
	--CAST(a.fec_actualizacion_estado_asesoria AS DATE) AS fecha_estado_asesoria,
	a.tasa_venta,
	a.periodo,
	CURRENT_DATE() AS fec_insercion,
	CURRENT_DATE() AS fec_modificacion,
	false AS bq__soft_deleted
FROM 
	--`rs-nprd-dlk-dt-stg-msrl-d0d7.stg_modelo_prospeccion.cotizacion_union_tmp` a
	--`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_prospeccion.cotizacion_union_tmp` a
	`rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto` a

	--LEFT JOIN `rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_producto.producto` pro
	--LEFT JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_producto.producto` pro
	LEFT JOIN producto_ax_data pro
		ON (
			pro.cod_producto=a.codproducto_final
		)
	LEFT JOIN persona_data per_cli
		ON (
			per_cli.num_documento=a.nro_doc_cliente
			AND per_cli.tip_documento=if(coalesce(a.tip_doc_cliente,'N.D.')='N.D.','DNI',a.tip_doc_cliente)
		)
	LEFT JOIN persona_data per_ase_j
		ON (
			per_ase_j.num_documento=a.nro_doc_asesor
			AND per_ase_j.tip_documento=if(coalesce(a.tip_doc_asesor,'N.D.')='N.D.','DNI',a.tip_doc_asesor)
			AND a.origen_data NOT IN ('COTIZADOR WEB')
		)
	LEFT JOIN personas_correo_data per_ase_c
		ON (
			UPPER(TRIM(per_ase_c.correo_corporativo))=UPPER(TRIM(a.email_asesor))
			AND a.origen_data IN ('COTIZADOR WEB','FINRISK RENTA VITALICIA','FINRISK RENTA GARANTIZADA')
		)
	LEFT JOIN jerarquia_ffvv_data int
		ON (
			int.id_persona=IFNULL(per_ase_j.id_persona,per_ase_c.id_persona)
			AND int.dsc_periodo=FORMAT_DATE('%Y%m', a.periodo )
		)
	LEFT JOIN email_dni_data email --Temporal Hasta que data del Modelo Intermediario este arreglada
		ON (
			LOWER(TRIM(a.email_asesor))=LOWER(TRIM(email.correo_corporativo))
		)
	/*
	LEFT JOIN jerarquia_consolidado_vigente_data jcv --Temporal Hasta que data del Modelo Intermediario este arreglada
		ON (
			TRIM(jcv.periodo)=TRIM(FORMAT_DATE('%Y%m', a.periodo )) AND
			jcv.dni=SAFE_CAST(TRIM(REGEXP_REPLACE(IFNULL(a.nro_doc_asesor,email.dni),'[^0-9 ]','')) AS BIGINT)
		)
	*/
	LEFT JOIN tramites_data trm
		ON (
			trm.Periodo=a.periodo 
			--AND trm.id_intermediario=CAST(COALESCE(int.id_intermediario, per_ase_j.cod_ax, per_ase_c.cod_ax, jcv.ax) AS INT)
			AND trm.id_intermediario=CAST(COALESCE(int.id_intermediario, per_ase_j.cod_ax, per_ase_c.cod_ax) AS INT)
			AND SAFE_CAST(TRIM(REGEXP_REPLACE(trm.dni_cliente,'[^0-9 ]','')) AS BIGINT)=SAFE_CAST(TRIM(REGEXP_REPLACE(a.nro_doc_cliente,'[^0-9 ]','')) AS BIGINT)
			AND trm.cod_producto=a.codproducto_final
		)
	LEFT JOIN tipo_cambio_data tc
		ON (
			tc.periodo = FORMAT_DATE('%Y%m', COALESCE(a.fec_crea_pago,trm.fecha_presentada,a.periodo) ) 
		)
	LEFT JOIN canal_data cnl
		ON (
			upper(trim(cnl.dsc_canal))=upper(trim(a.canal))
		)
),
first_free_tramite AS (
	SELECT 
		tra.Periodo,
		tra.id_intermediario,
		tra.dni_cliente,
		tra.cod_producto,
		--tra.fecha_presentada,
		tra.id_tramite
	FROM 
		tramites_data tra 
		LEFT JOIN first_final_cotizacion ffc 
			ON tra.id_tramite=ffc.id_tramite
	WHERE
		ffc.id_tramite IS NULL
),
second_final_cotizacion AS (
	SELECT 
		ffc.id_cotizacion,
		ffc.id_cotizacion_origen,
		ffc.origen_lead_cliente,
		ffc.id_canal_cot,
		ffc.des_canal_cot,
		ffc.numpol_ext_cot,
		ffc.id_persona_asesor,
		ffc.des_nro_doc_cliente,
		ffc.id_jerarquia_fuerza_ventas,
		ffc.id_intermediario,
		ffc.des_nro_doc_asesor,
		ffc.id_persona_cliente,
		ffc.origen_data,
		ffc.fecha_estado_asesoria,
		ffc.producto_recomendado,
		ffc.id_producto_final,
		ffc.id_estado_cotizacion,
		ffc.des_estado_cotizacion,
		ffc.scoring,
		ffc.id_asesoria_origen,
		ffc.id_estado_asesoria,
		ffc.des_estado_asesoria,
		ffc.des_moneda_presentada,
		ffc.fecha_presentada,
		ffc.prima_anual,
		ffc.prima_anual_usd,
		ffc.prima_anual_presentada_usd,
		ffc.pago_flg,
		ffc.flg_presentada,
		ffc.tasacambio,
		ffc.fecha_tasa_cambio,
		COALESCE(ffc.id_tramite,fft.id_tramite) as id_tramite,
		COALESCE(ffc.periodo_tramite,fft.Periodo) as periodo_tramite,
		ffc.tasa_venta,
		ffc.periodo,
		ffc.fec_insercion,
		ffc.fec_modificacion,
		ffc.bq__soft_deleted
	FROM 
		first_final_cotizacion ffc
		LEFT JOIN first_free_tramite fft 
			ON ffc.id_tramite IS NULL
				AND DATE_ADD(ffc.periodo, INTERVAL 1 MONTH) = fft.Periodo 
				AND ffc.id_intermediario = fft.id_intermediario 
				AND SAFE_CAST(TRIM(REGEXP_REPLACE(ffc.des_nro_doc_cliente,'[^0-9 ]','')) AS BIGINT)=SAFE_CAST(TRIM(REGEXP_REPLACE(fft.dni_cliente,'[^0-9 ]','')) AS BIGINT)
				AND SUBSTRING(ffc.id_producto_final,4,LENGTH(ffc.id_producto_final)) = fft.cod_producto 
),
second_free_tramite AS (
	SELECT 
		tra.Periodo,
		tra.id_intermediario,
		tra.dni_cliente,
		tra.cod_producto,
		--tra.fecha_presentada,
		tra.id_tramite
	FROM 
		tramites_data tra 
		LEFT JOIN second_final_cotizacion sfc 
			ON tra.id_tramite=sfc.id_tramite
	WHERE
		sfc.id_tramite IS NULL
),
final_cotizacion AS (
	SELECT 
		sfc.id_cotizacion,
		sfc.id_cotizacion_origen,
		sfc.origen_lead_cliente,
		sfc.id_canal_cot,
		sfc.des_canal_cot,
		sfc.numpol_ext_cot,
		sfc.id_persona_asesor,
		sfc.des_nro_doc_cliente,
		sfc.id_jerarquia_fuerza_ventas,
		sfc.id_intermediario,
		sfc.des_nro_doc_asesor,
		sfc.id_persona_cliente,
		sfc.origen_data,
		sfc.fecha_estado_asesoria,
		sfc.producto_recomendado,
		sfc.id_producto_final,
		sfc.id_estado_cotizacion,
		sfc.des_estado_cotizacion,
		sfc.scoring,
		sfc.id_asesoria_origen,
		sfc.id_estado_asesoria,
		sfc.des_estado_asesoria,
		sfc.des_moneda_presentada,
		sfc.fecha_presentada,
		sfc.prima_anual,
		sfc.prima_anual_usd,
		sfc.prima_anual_presentada_usd,
		sfc.pago_flg,
		sfc.flg_presentada,
		sfc.tasacambio,
		sfc.fecha_tasa_cambio,
		COALESCE(sfc.id_tramite, sft.id_tramite) AS id_tramite,
		COALESCE(sfc.periodo_tramite,sft.Periodo) as periodo_tramite,
		sfc.tasa_venta,
		sfc.periodo,
		sfc.fec_insercion,
		sfc.fec_modificacion,
		sfc.bq__soft_deleted
	FROM 
		second_final_cotizacion sfc
		LEFT JOIN second_free_tramite sft 
			ON sfc.id_tramite IS NULL 
				AND DATE_ADD(sfc.periodo, INTERVAL 2 MONTH) = sft.Periodo
				AND sfc.id_intermediario = sft.id_intermediario
				AND SAFE_CAST(TRIM(REGEXP_REPLACE(sfc.des_nro_doc_cliente,'[^0-9 ]','')) AS BIGINT)=SAFE_CAST(TRIM(REGEXP_REPLACE(sft.dni_cliente,'[^0-9 ]','')) AS BIGINT)
				AND SUBSTRING(sfc.id_producto_final,4,LENGTH(sfc.id_producto_final)) = sft.cod_producto
)
SELECT 
	id_cotizacion,
	id_cotizacion_origen,
	origen_lead_cliente,
	id_canal_cot,
	--des_canal_cot,
	numpol_ext_cot,
	id_persona_asesor,
	id_jerarquia_fuerza_ventas,
	id_intermediario,
	des_nro_doc_asesor,
	id_persona_cliente,
	--des_nro_doc_cliente,
	origen_data,
	fecha_estado_asesoria,
	producto_recomendado,
	id_producto_final,
	id_estado_cotizacion,
	des_estado_cotizacion,
	scoring,
	id_asesoria_origen,
	id_estado_asesoria,
	des_estado_asesoria,
	des_moneda_presentada,
	fecha_presentada,
	prima_anual,
	prima_anual_usd,
	prima_anual_presentada_usd,
	/*CASE 
		WHEN id_tramite IS NOT NULL OR (origen_data='JOURNEY' AND prima_anual_usd > 0)
			THEN '1' 
		ELSE '0'
	END AS */
	flg_presentada,
	tasacambio,
	fecha_tasa_cambio,
	CASE
		WHEN id_tramite IS NULL AND origen_data='JOURNEY' AND pago_flg='1'
		THEN (	'JV-' || id_cotizacion ||'-'||
				FORMAT_DATE('%Y%m', fecha_estado_asesoria) ||
				if(id_cotizacion_origen is null,'','-'||id_cotizacion_origen) )
		ELSE id_tramite
	END AS id_tramite,
	periodo_tramite,
	CASE
		WHEN id_tramite IS NOT NULL THEN 'LOTUS'
		ELSE if((origen_data='JOURNEY' AND pago_flg='1'),'JOURNEY DIGITAL',null)
	END AS origen_tramite,
	cast(tasa_venta AS NUMERIC) AS tasa_venta,
	periodo,
	fec_insercion,
	fec_modificacion,
	bq__soft_deleted
FROM 
	final_cotizacion;