DECLARE var_start_date DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE('America/Lima'), YEAR), INTERVAL {numyear} YEAR);

TRUNCATE TABLE `{project_id_anl_msrl}.delivery_jarvis.cr_avance_ffvv_vida`; 

INSERT INTO `{project_id_anl_msrl}.delivery_jarvis.cr_avance_ffvv_vida`
SELECT  
    com.periodo,
    com.cot_id_asesoria AS id_bitacora,
    com.cot_des_estado_asesoria AS estado_asesoria,
    CAST(com.cod_sap_asesor AS string) AS codigo_asesor,
    com.nro_doc_asesor AS dni,
    com.cot_des_producto_recomendado AS productos_recomendar_producto,
    com.cot_id_cotizacion AS id_cotizacion,
    com.agencia AS agencia,
    com.localidad,
    com.motivo_estado,
    com.dotacion_activa,
    com.supervisor,
    com.gerente_agencia,
    com.nombre_asesor AS nombre_corto,
    com.cod_ax_asesor AS ax,
    com.trm_numero_tramite AS numero_tramite,
    com.des_producto AS producto_final,
    com.cot_fecha_presentada AS fecha_presentada,
    com.cot_fecha_estado_asesoria AS fecha_estado_asesoria,
    com.trm_flg_presentada AS flg_presentada,
    com.flg_asesor_cotizacion AS flg_asesor_cotizacion,
    com.cot_mnt_prima_presentada_usd,
    com.trm_fecha_emitida AS fecha_emitida,
    com.cot_tip_scoring AS scoring,
    com.cot_id_origen AS origen_lead_cliente,
    com.origen_data,
    current_date() AS fec_insercion,
    current_date() AS fec_modificacion,
    false AS bq__soft_deleted
FROM 
    `{project_id_anl_msrl}.anl_comercial.productividad` com
WHERE 
    com.periodo >= var_start_date 
    AND des_canal='FUERZA DE VENTA' 
    AND des_sub_canal='FFVV VIDA';