DECLARE var_periodo_proceso DEFAULT DATE_TRUNC(CAST('{periodo}' AS DATE), MONTH);

TRUNCATE TABLE `{project_id_anl_msrl}.anl_comercial.meta_ffvv`;

INSERT INTO `{project_id_anl_msrl}.anl_comercial.meta_ffvv`
SELECT
	a.mes_comercial,
	a.periodo,
	a.id_meta_comercial,
	a.id_canal,
	a.des_canal,
	a.id_subcanal,
	a.des_subcanal,
	a.dsc_origen,
	a.dsc_localidad,
	a.dsc_agencia,
	a.dsc_unidad,
	a.id_intermediario_rimac AS id_intermediario,
	a.id_persona,
	a.id_bitacora_jerarquia_fuerza_ventas,
	a.id_jerarquia_fuerza_ventas,
	a.cod_sap,
	a.dsc_nombre_asesor,
	a.dsc_cargo,
	a.dsc_indicador,
	a.dsc_unidad_meta,
	a.mnt_meta_comercial,
	CURRENT_DATE() AS fec_insercion,
	CURRENT_DATE() AS fec_modificacion,
	false AS bq__soft_deleted
FROM 
	`{project_id_stg}.stg_modelo_comercial.meta_comercial` a
WHERE 
	periodo >= var_periodo_proceso;