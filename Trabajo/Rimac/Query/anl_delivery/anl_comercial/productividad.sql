DECLARE var_periodo_proceso DEFAULT DATE_TRUNC(CAST('{periodo}' AS DATE), MONTH);

TRUNCATE TABLE `{project_id_anl_msrl}.anl_comercial.productividad`;

INSERT INTO `{project_id_anl_msrl}.anl_comercial.productividad`
WITH bitacora_jfv AS (
	SELECT  
		jfv.dsc_periodo,
		jfv.id_jerarquia_fuerza_ventas,
		max(jfv.id_canal) AS id_canal,
		max(jfv.dsc_canal) AS dsc_canal,
		max(jfv.id_subcanal) AS id_subcanal,
		max(jfv.dsc_subcanal) AS dsc_subcanal,
		max(jfv.dsc_localidad) AS dsc_localidad,
		max(jfv.dsc_agencia) AS dsc_agencia,
		max(jfv.dsc_nombre_jefe2) AS dsc_nombre_jefe2,
		max(jfv.dsc_nombre_jefe1) AS dsc_nombre_jefe1,
		max(jfv.cod_sap) AS cod_sap,
		max(jfv.tip_documento) AS tip_documento,
		max(jfv.dsc_nombre_corto) AS dsc_nombre_corto, --per.nom_corto AS nombre_asesor,
		max(jfv.dsc_motivo_estado) AS dsc_motivo_estado,
		max(jfv.ind_dotacion_activa) AS ind_dotacion_activa,
		max(jfv.dsc_supervision) AS dsc_supervision   
	FROM 
		`{project_id_stg}.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv -- ESTRUCTURA ELOY
		--`rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv -- ESTRUCTURA ELOY
	WHERE 
		LOWER(jfv.dsc_tipo_puesto)='asesor' 
		AND CAST(jfv.dsc_periodo AS string) >= FORMAT_DATE('%Y%m', var_periodo_proceso)
	GROUP BY 
		jfv.dsc_periodo,
		jfv.id_jerarquia_fuerza_ventas
),
tramite_data AS (
	SELECT 
		trm.numtramite,
		MAX(trm.fec_creacion) AS fec_creacion,
		MAX(trm.fec_emision) AS  fec_emision
	FROM 
		`{project_id_stg_msrl}.stg_modelo_operaciones.tramite` trm
		--`rs-nprd-dlk-dt-stg-msrl-d0d7.stg_modelo_operaciones.tramite` trm
	WHERE trm.origen_data IN ('LOTUS','JOURNEY DIGITAL')  
		-- AND   lower(trm.tipo_tramite) IN ('emision','emisión')  -- se comenta para obtener la fecha de emision del tramite
	GROUP BY 
		trm.numtramite
),
product_data AS (
	SELECT
		pro.id_producto,
		MAX(pro.nom_producto) AS nom_producto,
		MAX(j.cod_riesgo) AS cod_riesgo,
		ARRAY_AGG(
		j.nom_riesgo ORDER BY j.cod_riesgo DESC
		)[OFFSET(0)] AS nom_riesgo,
		MAX(j.agrupacion_n1) AS agrupacion_n1,
		MAX(j.agrupacion_n2) AS agrupacion_n2,
		MAX(j.agrupacion_n3) AS agrupacion_n3,
		MAX(j.agrupacion_n4) AS agrupacion_n4
	FROM 
		`{project_id_stg}.stg_modelo_producto.producto` pro
		--`rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_producto.producto` pro
		,UNNEST(pro.producto_jerarquia) AS j
	GROUP BY 
		pro.id_producto
),
persona_data AS (
	SELECT 
		a.id_persona, 
		b.tip_documento, 
		b.num_documento,
		a.cod_acselx,
		a.nom_corto,
		---
		a.ape_paterno,
		a.ape_materno,
		a.nom_persona,
		---
		MAX(c.correo_corporativo) AS correo_corporativo,
		MAX(c.cod_sap) AS cod_sap
	FROM 
		`{project_id_stg}.stg_modelo_persona.persona` a
		--`rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_persona.persona` a
		CROSS JOIN unnest (a.documento_identidad) b
		LEFT JOIN `{project_id_stg}.stg_modelo_persona.colaborador` c
		--LEFT JOIN `rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_persona.colaborador` c
			ON (a.id_persona=c.id_persona)
	WHERE 
		b.ind_documento_principal = '1' -- Se obtiene el documento principal de la persona 
		-- AND upper(trim(b.tip_documento))='DNI' -- Se comenta esto para el caso de los clientes o colaboradores que presenten CE u otro documento como principal
		AND ifnull(a.bq__soft_deleted,false)=false 
		AND ifnull(c.bq__soft_deleted,false)=false
	GROUP BY 
		a.id_persona, 
		b.tip_documento, 
		b.num_documento,
		a.cod_acselx,
		a.nom_corto,
		---
		a.ape_paterno,
		a.ape_materno,
		a.nom_persona
		---  
),
first_productividad AS (
	SELECT 
		cot.id_jerarquia_fuerza_ventas,
		jfv.id_canal AS id_canal,
		jfv.dsc_canal AS des_canal,
		jfv.id_subcanal AS id_sub_canal,
		jfv.dsc_subcanal AS des_sub_canal,
		jfv.dsc_localidad AS localidad,
		jfv.dsc_agencia AS agencia,
		jfv.dsc_nombre_jefe2 AS gerente_agencia,
		jfv.dsc_nombre_jefe1 AS supervisor,
		jfv.cod_sap AS cod_sap_asesor,
		per_col.cod_acselx AS cod_ax_asesor, --per.cod_acselx AS cod_ax_asesor,
		jfv.tip_documento AS tipo_doc_asesor,
		cot.des_nro_doc_asesor AS nro_doc_asesor,
		jfv.dsc_nombre_corto, --per.nom_corto AS nombre_asesor,
		jfv.dsc_motivo_estado,
		jfv.ind_dotacion_activa AS dotacion_activa,
		cot.id_cotizacion AS cot_id_cotizacion,
		cot.origen_lead_cliente AS cot_id_origen,
		pro.cod_riesgo AS cot_id_riesgo,
		pro.nom_riesgo AS cot_des_riesgo,
		pro.agrupacion_n1 AS cot_des_agrupacion_n1,
		pro.agrupacion_n2 AS cot_des_agrupacion_n2,
		pro.agrupacion_n3 AS cot_des_agrupacion_n3,
		cot.id_producto_final AS cot_id_producto,
		pro.nom_producto AS des_producto,
		cot.producto_recomendado AS cot_des_producto_recomendado,
		cot.des_moneda_presentada AS cot_moneda_presentada,
		cot.prima_anual_presentada_usd AS cot_mnt_prima_presentada_usd,
		cot.scoring AS cot_tip_scoring,
		cot.id_asesoria_origen  AS cot_id_asesoria,
		cot.id_estado_asesoria  AS cot_id_estado_asesoria,
		cot.des_estado_asesoria AS cot_des_estado_asesoria,
		cot.origen_data,
		cot.id_tramite AS trm_id_tramite,
		cot.id_tramite AS trm_numero_tramite,
		cot.fecha_presentada AS cot_fecha_presentada,
		cot.fecha_estado_asesoria AS cot_fecha_estado_asesoria,
		cot.flg_presentada as trm_flg_presentada,
		trm.fec_emision AS trm_fecha_emitida,   -- se cambia por la fecha de emision de la poliza
		cot.periodo AS periodo,
		---
		cot.id_persona_cliente	, 
		IF( jfv.dsc_supervision LIKE 'ESPECIALISTA%' , 'S', 'N' ) AS  ind_asesor_especialista ,   
		--   
		cli.tip_documento AS tip_doc_cliente,
		cli.num_documento AS nro_doc_cliente,
		cli.ape_paterno AS ape_paterno_cliente,
		cli.ape_materno AS ape_materno_cliente,
		cli.nom_persona AS nom_cliente,
		---
		current_date() AS fec_insercion,
		current_date() AS fec_modificacion,
		false AS bq__soft_deleted
	FROM 
		`{project_id_stg_msrl}.stg_modelo_prospeccion.cotizacion` cot
		--`rs-nprd-dlk-dt-stg-msrl-d0d7.stg_modelo_prospeccion.cotizacion` cot
		LEFT JOIN tramite_data trm
			ON (trm.numtramite=cot.id_tramite)
		LEFT JOIN product_data pro
			ON ( trim(pro.id_producto)=cast(cot.id_producto_final as string) )
		LEFT JOIN bitacora_jfv jfv
			ON (jfv.id_jerarquia_fuerza_ventas=cot.id_jerarquia_fuerza_ventas
				AND cast(jfv.dsc_periodo as string)=FORMAT_DATE('%Y%m', cot.periodo))
		LEFT JOIN persona_data per_col
			ON (per_col.id_persona=cot.id_persona_asesor)
		--
		LEFT JOIN persona_data cli
			ON (cli.id_persona=cot.id_persona_cliente)
		--
	WHERE cot.origen_data IN ('JOURNEY','COTIZADOR WEB') AND
		  cot.periodo >= var_periodo_proceso
),
grouped_first_productividad AS (
	SELECT  
		periodo,
		id_jerarquia_fuerza_ventas,
		id_canal,
		des_canal,
		id_sub_canal,
		des_sub_canal,
		localidad,
		agencia,
		gerente_agencia,
		supervisor,
		cod_sap_asesor,
		tipo_doc_asesor,
		dsc_nombre_corto,
		dsc_motivo_estado,
		dotacion_activa 
	FROM
		first_productividad
	WHERE
		periodo >= var_periodo_proceso 
		AND id_jerarquia_fuerza_ventas IS NOT NULL
	GROUP BY 
		periodo,
		id_jerarquia_fuerza_ventas,
		id_canal,
		des_canal,
		id_sub_canal,
		des_sub_canal,
		localidad,
		agencia,
		gerente_agencia,
		supervisor,
		cod_sap_asesor,
		tipo_doc_asesor,
		dsc_nombre_corto,
		dsc_motivo_estado,
		dotacion_activa 
),
data_bitacora_floja AS (
	SELECT  	
		bjd.id_jerarquia_fuerza_ventas,
		bjd.id_canal AS id_canal,
		bjd.dsc_canal AS des_canal,
		bjd.id_subcanal AS id_sub_canal,
		bjd.dsc_subcanal AS des_sub_canal,
		bjd.dsc_localidad AS localidad,
		bjd.dsc_agencia AS agencia,
		bjd.dsc_nombre_jefe2 AS gerente_agencia,
		bjd.dsc_nombre_jefe1 AS supervisor,
		bjd.cod_sap AS cod_sap_asesor,
		SAFE_CAST(NULL AS STRING) AS cod_ax_asesor,
		bjd.tip_documento AS tipo_doc_asesor,
		SAFE_CAST(NULL AS STRING) AS nro_doc_asesor,
		bjd.dsc_nombre_corto,
		bjd.dsc_motivo_estado,
		bjd.ind_dotacion_activa AS dotacion_activa,
		SAFE_CAST(NULL AS STRING) AS cot_id_cotizacion,
		SAFE_CAST(NULL AS STRING) AS cot_id_origen,
		SAFE_CAST(NULL AS STRING) AS cot_id_riesgo,
		SAFE_CAST(NULL AS STRING) AS cot_des_riesgo,
		SAFE_CAST(NULL AS STRING) AS cot_des_agrupacion_n1,
		SAFE_CAST(NULL AS STRING) AS cot_des_agrupacion_n2,
		SAFE_CAST(NULL AS STRING) AS cot_des_agrupacion_n3,
		SAFE_CAST(NULL AS STRING) AS cot_id_producto,
		SAFE_CAST(NULL AS STRING) AS des_producto,
		SAFE_CAST(NULL AS STRING) AS cot_des_producto_recomendado,
		SAFE_CAST(NULL AS STRING) AS cot_moneda_presentada,
		SAFE_CAST(NULL AS NUMERIC) AS cot_mnt_prima_presentada_usd,
		SAFE_CAST(NULL AS STRING) AS cot_tip_scoring,
		SAFE_CAST(NULL AS STRING) AS cot_id_asesoria,
		SAFE_CAST(NULL AS INT64) AS cot_id_estado_asesoria,
		SAFE_CAST(NULL AS STRING) AS cot_des_estado_asesoria,
		SAFE_CAST(NULL AS STRING) AS origen_data,
		SAFE_CAST(NULL AS STRING) AS trm_id_tramite,
		SAFE_CAST(NULL AS STRING) AS trm_numero_tramite,
		SAFE_CAST(NULL AS DATE) AS cot_fecha_presentada,
		SAFE_CAST(NULL AS DATE) AS cot_fecha_estado_asesoria,
		SAFE_CAST(NULL AS STRING) AS trm_flg_presentada,
		SAFE_CAST(NULL AS DATE) AS trm_fecha_emitida,
		SAFE_CAST(CONCAT(SUBSTRING(bjd.dsc_periodo,1,4),'-', SUBSTRING(bjd.dsc_periodo,5,2),'-01') AS DATE) AS dsc_periodo, --JOTA
		---
		SAFE_CAST(NULL AS STRING) AS id_persona_cliente , 
		IF( bjd.dsc_supervision LIKE 'ESPECIALISTA%' , 'S', 'N' ) AS  ind_asesor_especialista ,   
		--
		SAFE_CAST(NULL AS STRING) AS tip_doc_cliente,
		SAFE_CAST(NULL AS STRING) AS nro_doc_cliente,
		SAFE_CAST(NULL AS STRING) AS ape_paterno_cliente,
		SAFE_CAST(NULL AS STRING) AS ape_materno_cliente,
		SAFE_CAST(NULL AS STRING) AS nom_cliente,		  
		---
		current_date() AS fec_insercion,
		current_date() AS fec_modificacion,
		false AS bq__soft_deleted
	FROM 
		bitacora_jfv bjd 
		LEFT JOIN grouped_first_productividad gfp 
			ON bjd.id_jerarquia_fuerza_ventas = gfp.id_jerarquia_fuerza_ventas 
				AND bjd.dsc_periodo = FORMAT_DATE('%Y%m', gfp.periodo)
	WHERE 
		bjd.dsc_periodo >= FORMAT_DATE('%Y%m', var_periodo_proceso)  
		AND gfp.id_jerarquia_fuerza_ventas IS NULL
		AND gfp.periodo IS NULL

)

SELECT 
	fp.id_canal,
	fp.des_canal,
	fp.id_sub_canal,
	fp.des_sub_canal,
	fp.localidad,
	fp.agencia,
	fp.gerente_agencia,
	fp.supervisor,
	fp.cod_sap_asesor,
	fp.cod_ax_asesor,
	fp.tipo_doc_asesor,
	fp.nro_doc_asesor,
	fp.dsc_nombre_corto,
	fp.dsc_motivo_estado,
	fp.dotacion_activa,
	fp.cot_id_cotizacion,
	fp.cot_id_origen,
	fp.cot_id_riesgo,
	fp.cot_des_riesgo,
	fp.cot_des_agrupacion_n1,
	fp.cot_des_agrupacion_n2,
	fp.cot_des_agrupacion_n3,
	fp.cot_id_producto,
	fp.des_producto,
	fp.cot_des_producto_recomendado,
	fp.cot_moneda_presentada,
	fp.cot_mnt_prima_presentada_usd,
	fp.cot_tip_scoring,
	fp.cot_id_asesoria,
	fp.cot_id_estado_asesoria,
	fp.cot_des_estado_asesoria,
	fp.origen_data,
	fp.trm_id_tramite,
	fp.trm_numero_tramite,
	fp.cot_fecha_presentada,
	fp.cot_fecha_estado_asesoria,
	fp.trm_flg_presentada,
	'1' AS flg_asesor_cotizacion,	
	fp.trm_fecha_emitida,
	--
	fp.id_persona_cliente, 
	fp.ind_asesor_especialista,     
	--   
	fp.tip_doc_cliente,
	fp.nro_doc_cliente,
	fp.ape_paterno_cliente,
	fp.ape_materno_cliente,
	fp.nom_cliente, 
	--
	fp.periodo,
	fp.fec_insercion,
	fp.fec_modificacion,
	fp.bq__soft_deleted
FROM 
	first_productividad fp

UNION ALL 

SELECT 
	dbf.id_canal,
	dbf.des_canal,
	dbf.id_sub_canal,
	dbf.des_sub_canal,
	dbf.localidad,
	dbf.agencia,
	dbf.gerente_agencia,
	dbf.supervisor,
	dbf.cod_sap_asesor,
	SAFE_CAST(dbf.cod_ax_asesor AS STRING) AS cod_ax_asesor,
	dbf.tipo_doc_asesor,
	SAFE_CAST(dbf.nro_doc_asesor AS STRING) AS nro_doc_asesor,
	dbf.dsc_nombre_corto,
	dbf.dsc_motivo_estado,
	dbf.dotacion_activa,
	SAFE_CAST(dbf.cot_id_cotizacion AS STRING) AS cot_id_cotizacion,
	SAFE_CAST(dbf.cot_id_origen AS STRING) AS cot_id_origen,
	SAFE_CAST(dbf.cot_id_riesgo AS STRING) AS cot_id_riesgo,
	SAFE_CAST(dbf.cot_des_riesgo AS STRING) AS cot_des_riesgo,
	SAFE_CAST(dbf.cot_des_agrupacion_n1 AS STRING) AS cot_des_agrupacion_n1,
	SAFE_CAST(dbf.cot_des_agrupacion_n2 AS STRING) AS cot_des_agrupacion_n2,
	SAFE_CAST(dbf.cot_des_agrupacion_n3 AS STRING) AS cot_des_agrupacion_n3,
	SAFE_CAST(dbf.cot_id_producto AS STRING) AS cot_id_producto,
	SAFE_CAST(dbf.des_producto AS STRING) AS des_producto,
	SAFE_CAST(dbf.cot_des_producto_recomendado AS STRING) AS cot_des_producto_recomendado,
	SAFE_CAST(dbf.cot_moneda_presentada AS STRING) AS cot_moneda_presentada,
	SAFE_CAST(dbf.cot_mnt_prima_presentada_usd AS NUMERIC) AS cot_mnt_prima_presentada_usd,
	SAFE_CAST(dbf.cot_tip_scoring AS STRING) AS cot_tip_scoring,
	SAFE_CAST(dbf.cot_id_asesoria AS STRING) AS cot_id_asesoria,
	SAFE_CAST(dbf.cot_id_estado_asesoria AS INT64) AS cot_id_estado_asesoria,
	SAFE_CAST(dbf.cot_des_estado_asesoria AS STRING) AS cot_des_estado_asesoria,
	SAFE_CAST(dbf.origen_data AS STRING) AS origen_data,
	SAFE_CAST(dbf.trm_id_tramite AS STRING) AS trm_id_tramite,
	SAFE_CAST(dbf.trm_numero_tramite AS STRING) AS trm_numero_tramite,
	SAFE_CAST(dbf.cot_fecha_presentada AS DATE) AS cot_fecha_presentada,
	SAFE_CAST(dbf.dsc_periodo AS DATE) AS cot_fecha_estado_asesoria,
	SAFE_CAST(dbf.trm_flg_presentada AS STRING) AS trm_flg_presentada,
	'0' AS flg_asesor_cotizacion,
	SAFE_CAST(dbf.trm_fecha_emitida AS DATE) AS trm_fecha_emitida,
	---
	dbf.id_persona_cliente,   
	dbf.ind_asesor_especialista,
	--
	dbf.tip_doc_cliente,
	dbf.nro_doc_cliente,
	dbf.ape_paterno_cliente,
	dbf.ape_materno_cliente,
	dbf.nom_cliente,
	--
	SAFE_CAST(dbf.dsc_periodo AS DATE) AS dsc_periodo,
	dbf.fec_insercion,
	dbf.fec_modificacion,
	dbf.bq__soft_deleted
FROM 
	data_bitacora_floja dbf;