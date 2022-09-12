DECLARE var_start_date DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE('America/Lima'), YEAR), INTERVAL {numyear} YEAR);

TRUNCATE TABLE `{project_id_anl_msrl}.delivery_jarvis.cr_funnel_ventas_rrvv`; 

INSERT INTO `{project_id_anl_msrl}.delivery_jarvis.cr_funnel_ventas_rrvv`
WITH
t_vista_gestion_ffvv_rentas AS (
	SELECT 
		fv.trm_mes_produccion AS mes_produccion,
		fv.trm_periodo AS periodo,
		fv.trm_fec_creacion AS fec_creacion_tramite,
		fv.trm_id_intermediario AS id_intermediario,
		fv.nom_intermediario, 
		fv.pol_id_poliza AS id_poliza, 
		fv.pol_fec_emision AS fec_emision_poliza, 
		fv.pol_tip_forma_pago AS tip_forma_pago_poliza, 
		fv.pol_id_origen AS id_origen_poliza,
		fv.trm_id_contratante AS id_contratante, 
		fv.nombre_completo_contratante, 
		fv.trm_id_producto AS id_producto,
		fv.des_producto, 
		fv.trm_des_riesgo AS des_riesgo,
		fv.trm_agrupacion_n1 AS agrupacion_n1,
		fv.trm_agrupacion_n2 AS agrupacion_n2,
		fv.trm_agrupacion_n3 AS agrupacion_n3,
		fv.bit_id_persona_asesor AS id_persona_asesor,
		CAST(fv.bit_id_canal AS STRING) AS id_canal,
		fv.bit_des_canal AS des_canal,
		CAST(fv.bit_id_subcanal AS STRING) AS id_subcanal,
		fv.bit_des_subcanal AS des_subcanal,
		fv.bit_localidad AS des_localidad,
		fv.bit_agencia AS des_agencia,
		fv.ciudad AS des_ciudad,
		--
		fv.ind_asesor_financiero  AS ind_asesor_financiero ,        
		fv.ind_asesor_especialista  AS ind_asesor_especialista,     
		fv.ind_cliente_nuevo AS ind_cliente_nuevo ,  
		--    
		fv.bit_gerente_agencia AS gerente_agencia, 
		fv.bit_supervisor AS supervisor,
		CAST(fv.bit_cod_sap_asesor AS STRING) AS cod_sap_asesor,
		fv.bit_nombre_corto_asesor AS nom_corto_asesor, 
		fv.bit_estado_gestion AS des_est_gestion, 
		fv.bit_categoria AS des_categoria, 
		CAST(fv.dotacion_activa AS STRING) AS dotacion_activa,
		CAST(fv.dotacion_real AS STRING) AS dotacion_real,
		CAST(fv.bit_dotacion_activa AS STRING) AS bit_dotacion_activa,   
		-- informacion de la meta x supervisor
		fv.id_meta_comercial , 
		fv.mnt_meta_supervisor,
		-- informacion de las cotizaciones por jerarquia 
		fv.id_bitacora_jerarquia_fuerza_ventas,
		fv.mnt_cotiz_asesor_primas_anual_usd, 
		-- totalizar importe de la prima presentada y emitida por asesor
		COUNT(DISTINCT fv.trm_id_tramite) AS cnt_tramites,
		COUNT(DISTINCT fv.pol_id_poliza) AS cnt_polizas,
		ROUND(SUM(fv.mnt_prima_presentada_anualizada_usd),2) AS mnt_prima_presentada_anualizada_usd,
		ROUND(SUM(fv.mnt_prima_emitida_bruta_anualizada_usd),2) AS mnt_prima_emitida_bruta_anualizada_usd,
		'GESTION FFVV RENTAS' AS vista_pbi , 
		CAST( NULL AS STRING) AS id_cotizacion ,
		current_date() AS fec_procesamiento
	FROM 
		`{project_id_anl_msrl}.anl_comercial.funnel_ventas` fv 
		--`rs-nprd-dlk-dt-anlyt-msrl-1570.anl_comercial.funnel_ventas` fv
	WHERE 
			fv.bit_id_subcanal = 202 -- RENTA VITALICIA o Rentas Vitalicias
		AND fv.trm_mes_produccion >= var_start_date
	GROUP BY 
		1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41
) ,
t_cliente_xperiodo AS (  -- Para obtener si es un cliente nuevo o antiguo para las cotizaciones de rentas 
    SELECT  
            cprod.id_sub_canal ,
            cprod.id_persona_cliente  ,
            min( cprod.periodo ) AS periodo_min , 
            max( cprod.periodo ) AS periodo_max 
	FROM 
		`{project_id_anl_msrl}.anl_comercial.productividad` cprod
		--`rs-nprd-dlk-dt-anlyt-msrl-1570.anl_comercial.productividad`cprod
	WHERE 
		cprod.periodo >= var_start_date 
		AND cprod.des_canal    = 'FUERZA DE VENTA' 
		AND cprod.id_sub_canal =  202  -- RENTAS VITALICIAS - Renta Vitalicia
    GROUP BY cprod.id_sub_canal , cprod.id_persona_cliente
) ,
t_vista_cotizaciones AS (
	SELECT  
		fv.trm_mes_produccion   AS mes_produccion, 
		com.periodo AS periodo,
		fv.trm_fec_creacion AS fec_creacion_tramite,
		fv.trm_id_intermediario AS id_intermediario,
		fv.nom_intermediario, 
		fv.pol_id_poliza AS id_poliza, 
		com.trm_fecha_emitida AS fec_emision_poliza, 
		fv.pol_tip_forma_pago AS tip_forma_pago_poliza, 
		fv.pol_id_origen AS id_origen_poliza,
		com.id_persona_cliente AS id_contratante,  -- dato de cotizaciones  
		fv.nombre_completo_contratante, 
		com.cot_id_producto AS id_producto,  -- dato de cotizaciones 
		com.des_producto AS des_producto,  -- dato de cotizaciones 
		com.cot_des_riesgo AS des_riesgo,
		com.cot_des_agrupacion_n1 AS agrupacion_n1, -- dato de cotizaciones
		com.cot_des_agrupacion_n2 AS agrupacion_n2, -- dato de cotizaciones
		com.cot_des_agrupacion_n3 AS agrupacion_n3, -- dato de cotizaciones
		CAST(NULL AS STRING) AS id_persona_asesor, 
		CAST(com.id_canal AS STRING) AS id_canal, 	-- dato de cotizaciones
		com.des_canal  AS des_canal,  				-- dato de cotizaciones
		CAST(com.id_sub_canal AS STRING) AS id_subcanal,  -- dato de cotizaciones
		com.des_sub_canal AS des_subcanal, -- dato de cotizaciones
		com.localidad AS des_localidad,        -- dato de cotizaciones
		com.agencia AS des_agencia, 		   -- dato de cotizaciones
		fv.ciudad AS des_ciudad,
		--
		fv.ind_asesor_financiero  AS ind_asesor_financiero ,        
		com.ind_asesor_especialista AS ind_asesor_especialista,      		-- dato de cotizaciones
		IF( com.periodo = cxp.periodo_min  , 'S' , 'N' ) AS ind_cliente_nuevo,  -- dato de cotizaciones
		--    
		com.gerente_agencia AS gerente_agencia,  -- dato de cotizaciones
		com.supervisor AS supervisor,            -- dato de cotizaciones
		CAST(com.cod_sap_asesor AS STRING) AS cod_sap_asesor, -- dato de cotizaciones
		com.nombre_asesor  AS nom_corto_asesor,  -- dato de cotizaciones
		fv.bit_estado_gestion AS des_est_gestion, 
		fv.bit_categoria AS des_categoria, 
		CAST(com.dotacion_activa AS STRING) AS dotacion_activa,   -- dato de cotizaciones
		COALESCE(CAST(fv.dotacion_real AS STRING) , '0' ) AS dotacion_real,
		COALESCE(CAST(fv.bit_dotacion_activa AS STRING),'0') AS bit_dotacion_activa,   
		-- informacion de la meta x supervisor : para el caso de rentas tiene otro calculo
		CAST(NULL AS STRING)  AS id_meta_comercial, 
		CAST(NULL AS NUMERIC)  AS mnt_meta_supervisor,
		-- informacion de las cotizaciones por jerarquia 
		fv.id_bitacora_jerarquia_fuerza_ventas,
		fv.mnt_cotiz_asesor_primas_anual_usd, 
		IF( fv.trm_id_tramite IS NOT NULL, 1 , 0 ) AS cnt_tramites,
		IF( com.trm_fecha_emitida IS NOT NULL , 1 , 0 ) AS cnt_polizas,
		-- Se considera prima presentada : si presenta el flag de presentada
		IF( com.trm_flg_presentada = '1', com.cot_mnt_prima_presentada_usd , 0 )  AS  mnt_prima_presentada_anualizada_usd,
		-- Se considera emitida : si tiene fecha emitida (fecha emision ) y flg_presentada = 1
		IF( com.trm_flg_presentada = '1' AND com.trm_fecha_emitida IS NOT NULL , com.cot_mnt_prima_presentada_usd , 0 ) AS  mnt_prima_emitida_bruta_anualizada_usd,
		-- Campos de las cotizaciones
		'COTIZACIONES' AS vista_pbi ,
		com.cot_id_cotizacion AS id_cotizacion,
 		current_date() AS fec_procesamiento,
	FROM 
		`{project_id_anl_msrl}.anl_comercial.productividad` com
		--`rs-nprd-dlk-dt-anlyt-msrl-1570.anl_comercial.productividad` com
	LEFT JOIN t_cliente_xperiodo cxp 
		ON ( cxp.id_persona_cliente  = com.id_persona_cliente and cxp.id_sub_canal = com.id_sub_canal ) 
	LEFT JOIN `{project_id_anl_msrl}.anl_comercial.funnel_ventas` fv 
	 		-- `rs-nprd-dlk-dt-anlyt-msrl-1570.anl_comercial.funnel_ventas` fv
		ON ( fv.trm_id_tramite = com.trm_id_tramite)
	WHERE 
		com.periodo >= var_start_date 
		AND com.des_canal    ='FUERZA DE VENTA' 
		AND com.id_sub_canal = 202              -- RENTAS VITALICIAS - Renta Vitalicia
		AND com.origen_data  = 'COTIZADOR WEB'  -- SOLO EL COTIZADOR WEB 
		AND flg_asesor_cotizacion = '1' 	    -- Mostrar solo los que producen 
)
SELECT *
FROM t_vista_gestion_ffvv_rentas
UNION ALL 
SELECT *
FROM t_vista_cotizaciones
;