DECLARE var_start_date DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE('America/Lima'), YEAR), INTERVAL {numyear} YEAR);

TRUNCATE TABLE `{project_id_anl_msrl}.delivery_jarvis.cr_funnel_ventas_ffvv`; 

INSERT INTO `{project_id_anl_msrl}.delivery_jarvis.cr_funnel_ventas_ffvv`
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
	CAST(fv.bit_id_canal AS string) AS id_canal,
	fv.bit_des_canal AS des_canal,
	CAST(fv.bit_id_subcanal AS string) AS id_subcanal,
	fv.bit_des_subcanal AS des_subcanal,
	fv.bit_localidad AS localidad,
	fv.bit_agencia AS agencia,
	fv.ciudad AS ciudad,
	fv.bit_gerente_agencia AS gerente_agencia, 
	fv.bit_supervisor AS supervisor,
	CAST(fv.bit_cod_sap_asesor AS string) AS codigo_sap_asesor,
	fv.bit_nombre_corto_asesor AS nombre_corto_asesor, 
	fv.bit_estado_gestion AS estado_gestion, 
	fv.bit_categoria AS categoria, 
	CAST(fv.dotacion_activa AS string) AS dotacion_activa,
	CAST(fv.dotacion_real AS string) AS dotacion_real,
    CAST(fv.bit_dotacion_activa AS string) AS bit_dotacion_activa,   -- nuevo campo para el filtro
	-- informacion de la meta x supervisor
	fv.id_meta_comercial, 
	fv.mnt_meta_supervisor,
	-- informacion de las cotizaciones por jerarquia 
	fv.id_bitacora_jerarquia_fuerza_ventas,
	fv.mnt_cotiz_asesor_primas_anual_usd, 
	-- totalizar importe de la prima presentada y emitida por asesor
	COUNT(distinct fv.trm_id_tramite) AS cantidad_tramites,
	COUNT(distinct fv.pol_id_poliza) AS cantidad_polizas,
	ROUND(SUM(fv.mnt_prima_presentada_anualizada_usd),2) AS mnt_prima_presentada_anualizada_usd,
	ROUND(SUM(fv.mnt_prima_emitida_bruta_anualizada_usd),2) AS mnt_prima_emitida_bruta_anualizada_usd,
	current_date() AS fec_procesamiento
FROM 
	`{project_id_anl_msrl}.anl_comercial.funnel_ventas` fv 
WHERE 
	--UPPER(TRIM(fv.bit_des_subcanal))='FFVV VIDA' 
	fv.bit_id_subcanal = 201  -- 'FFVV VIDA' , 'Vida' , 'FFVV Corredores' 
	--AND coalesce(fv.mnt_prima_presentada_anualizada_usd,0) > 0 -- se comenta para que se muestre las cotizaciones de las primas.
	AND fv.trm_mes_produccion >= var_start_date
GROUP BY 
	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38;