
CREATE OR REPLACE TABLE `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.meta_comercial_vitto` AS
WITH
t_canal AS (
    SELECT 
        pa.ideparam AS id_canal_inicial,
        CAST(pa.codnominal AS string) AS id_canal,
        pa.descripcion AS des_canal
    FROM 
        -- `{project_id_raw}.ue1dbaprodrdsumc001__ue1dprodmy01.UMC_PARAMETRO` pa
        `rs-nprd-dlk-dd-rwz-a406.ue1dbaprodrdsumc001__ue1dprodmy01.UMC_PARAMETRO` pa
    WHERE 
        pa.idetipoparam = 1
),
t_subcanal AS (
    SELECT 
        pa.ideparam AS id_subcanal_inicial,
        CAST(pa.codnominal AS string) AS id_subcanal,
        pa.descripcion AS des_subcanal
    FROM 
        -- `{project_id_raw}.ue1dbaprodrdsumc001__ue1dprodmy01.UMC_PARAMETRO` pa
        `rs-nprd-dlk-dd-rwz-a406.ue1dbaprodrdsumc001__ue1dprodmy01.UMC_PARAMETRO` pa
    WHERE 
        pa.idetipoparam = 2
),
t_bitacora_jerarquia_promotor_periodo AS 
(SELECT
des_periodo,
id_persona,
cod_sap,
nom_promotor
FROM 
`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.bitacora_jerarquia_promotores`
GROUP BY 
des_periodo,
id_persona,
cod_sap,
nom_promotor),
t_intermediario_rimac AS
(SELECT
CAST(id_intermediario AS STRING) id_intermediario
FROM 
`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.bitacora_mc` 
WHERE 
id_canal = 7 
AND dsc_estado_bitacora = 'ACT'
GROUP BY
id_intermediario),

t_bitacora_jerarquia_ffvv_periodo AS (
    SELECT
        jfv.dsc_periodo,
        CAST(jfv.cod_sap AS string) AS cod_sap, 
        array_agg(jfv.id_intermediario_rimac ORDER BY id_bitacora_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_intermediario_rimac,
        array_agg(jfv.id_jerarquia_fuerza_ventas ORDER BY id_bitacora_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_jerarquia_fuerza_ventas,
        array_agg(jfv.id_bitacora_jerarquia_fuerza_ventas ORDER BY id_bitacora_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_bitacora_jerarquia_fuerza_ventas, 
        array_agg(jfv.id_persona ORDER BY id_bitacora_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_persona
    FROM 
        -- `{project_id_stg}.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv
        `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv
    GROUP BY 
        jfv.dsc_periodo,
        jfv.cod_sap
),
t_bitacora_jerarquia_ffvv AS (
    SELECT
        CAST(jfv.cod_sap AS string) AS cod_sap, 
        array_agg(jfv.id_intermediario_rimac ORDER BY id_bitacora_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_intermediario_rimac,
        array_agg(jfv.id_jerarquia_fuerza_ventas ORDER BY id_bitacora_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_jerarquia_fuerza_ventas,
        array_agg(jfv.id_bitacora_jerarquia_fuerza_ventas ORDER BY id_bitacora_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_bitacora_jerarquia_fuerza_ventas,
        array_agg(jfv.id_persona ORDER BY id_bitacora_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_persona
    FROM 
        --`{project_id_stg}.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv
        `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv
    GROUP BY 
        jfv.cod_sap
),
t_metas AS (
    SELECT
        mes, -- YYYYMM
        PARSE_DATE('%Y%m', CAST(mes AS string)) AS periodo,
        -- definicion del canal y sub canal
        cn.id_canal AS id_canal,
        cn.des_canal,
        -- sub canal
        a.id_subcanal,
        ---
        TRIM(UPPER(ffvv)) AS ffvv,
        CASE
            WHEN TRIM(UPPER(lima_provincia)) = 'LIMA' THEN 'LIMA'
            WHEN SUBSTRING(TRIM(UPPER(lima_provincia)),1,4) = 'PROV' THEN 'PROVINCIA'
            ELSE '' 
        END AS dsc_origen,
        IF(TRIM(localidad) = 'NULL', '', TRIM(localidad)) AS dsc_localidad,
        IF(TRIM(agencia) = 'NULL', '', TRIM(agencia)) AS dsc_agencia,
        IF(TRIM(unidad) = 'NULL', '', TRIM(unidad)) AS dsc_unidad,
        -- informacion del asesor
        CASE
            WHEN TRIM(cod_sap) = 'NULL' THEN ''
            WHEN instr(cod_sap,'-') > 0 THEN TRIM(SUBSTRING(cod_sap,1,instr(cod_sap,'-')-1))
            ELSE TRIM(cod_sap)
        END AS cod_sap,
        IF(TRIM(nombre) = 'NULL', '', TRIM(nombre)) AS dsc_nombre_asesor,
        IF(TRIM(cargo) = 'NULL', '', TRIM(cargo)) AS dsc_cargo,
        IF(TRIM(indicador) = 'NULL', '', TRIM(indicador)) AS dsc_indicador,
        IF(TRIM(unidad_meta) = 'NULL', '',TRIM(unidad_meta)) AS dsc_unidad_meta,
        SAFE_CAST(meta AS decimal) AS meta
    FROM 
        --`{project_id_raw}.de__canales.metas_eerr` a
        `rs-nprd-dlk-dd-rwz-a406.de__canales.metas_eerr` a
        LEFT JOIN t_canal cn 
            ON (cn.id_canal = '2' and upper(trim(a.ffvv)) in ('VIDA','WORKSITE','RENTAS','TELEMARKETING')
                OR cn.id_canal = '3' and upper(trim(a.ffvv)) in ('CORREDOR'))
),
t_metas_comercial_parte1 as (
SELECT
    'ID-' || b.des_subcanal || '-' || a.cod_sap || '-' || a.periodo || '-' || a.dsc_origen || dsc_agencia || '-' ||	dsc_unidad || '-' || a.dsc_cargo || '-' || a.dsc_indicador AS id_meta_comercial,
    a.mes AS mes_comercial,
    a.periodo,
    -- informacion del canal y sub canal de la meta
    a.id_canal,
    a.des_canal,
    a.id_subcanal,
    b.des_subcanal,
    a.ffvv,
    --
    a.dsc_origen,
    a.dsc_localidad,
    a.dsc_agencia,
    a.dsc_unidad,
    COALESCE(jfvp.id_intermediario_rimac, jfv.id_intermediario_rimac) AS id_intermediario_rimac,
    COALESCE(jfvp.id_persona, jfv.id_persona) AS id_persona,
    COALESCE(jfvp.id_bitacora_jerarquia_fuerza_ventas, jfv.id_bitacora_jerarquia_fuerza_ventas) AS id_bitacora_jerarquia_fuerza_ventas,
    COALESCE(jfvp.id_jerarquia_fuerza_ventas, jfv.id_jerarquia_fuerza_ventas) AS id_jerarquia_fuerza_ventas,
    a.cod_sap,
    a.dsc_nombre_asesor,
    a.dsc_cargo,
    a.dsc_indicador,
    CASE
        WHEN UPPER(TRIM(a.dsc_indicador)) = 'PRIMA NETA' AND a.id_subcanal = '201' 
            THEN COALESCE(a.dsc_unidad_meta,'US$ ANUAL') --'VIDA'
        ELSE a.dsc_unidad_meta
    END AS dsc_unidad_meta,
    a.meta AS mnt_meta_comercial,
    CURRENT_DATE() AS fec_insercion,
    CURRENT_DATE() AS fec_modificacion,
    false AS bq__soft_deleted
FROM 
    t_metas a
    LEFT JOIN t_subcanal b 
        ON (b.id_subcanal= a.id_subcanal)
    LEFT JOIN t_bitacora_jerarquia_ffvv_periodo jfvp 
        ON (jfvp.dsc_periodo = CAST(a.mes AS string) AND jfvp.cod_sap = a.cod_sap)
    LEFT JOIN t_bitacora_jerarquia_ffvv jfv 
        ON (jfv.cod_sap = a.cod_sap)
),
t_metas_prom AS
(SELECT
mes, -- YYYYMM
PARSE_DATE('%Y%m', CAST(mes AS string)) AS periodo,
-- definicion del canal y sub canal
cn.id_canal AS id_canal,
cn.des_canal,
-- sub canal
a.id_subcanal,
---
TRIM(UPPER(ffvv)) AS ffvv,
CASE
  WHEN TRIM(UPPER(lima_provincia)) = 'LIMA' THEN 'LIMA'
  WHEN SUBSTRING(TRIM(UPPER(lima_provincia)),1,4) = 'PROV' THEN 'PROVINCIA'
  ELSE '' 
END AS dsc_origen,
IF(TRIM(localidad) = 'NULL', '', TRIM(localidad)) AS dsc_localidad,
IF(TRIM(agencia) = 'NULL', '', TRIM(agencia)) AS dsc_agencia,
IF(TRIM(unidad) = 'NULL', '', TRIM(unidad)) AS dsc_unidad,
-- informacion del asesor
CASE
  WHEN TRIM(cod_sap) = 'NULL' THEN ''
  WHEN instr(cod_sap,'-') > 0 THEN TRIM(SUBSTRING(cod_sap,1,instr(cod_sap,'-')-1))
  ELSE TRIM(cod_sap)
END AS cod_sap,
CASE
  WHEN TRIM(id_intermediario) = 'NULL' THEN ''
  ELSE TRIM(id_intermediario)
END AS id_intermediario,
IF(TRIM(nombre) = 'NULL', '', TRIM(nombre)) AS dsc_nombre_asesor,
IF(TRIM(cargo) = 'NULL', '', TRIM(cargo)) AS dsc_cargo,
IF(TRIM(indicador) = 'NULL', '', TRIM(indicador)) AS dsc_indicador,
IF(TRIM(unidad_meta) = 'NULL', '',TRIM(unidad_meta)) AS dsc_unidad_meta,
SAFE_CAST(meta AS decimal) AS meta
FROM 
`rs-nprd-dlk-dd-rwz-a406.de__canales.metas_eerr` a
LEFT JOIN t_canal cn 
ON (cn.id_canal = '7' and upper(trim(a.ffvv)) in ('CONCESIONARIOS'))
WHERE a.ID_SUBCANAL = '708'),
t_metas_comercial_parte2 
AS (
SELECT
'ID-' || b.des_subcanal || '-' || a.cod_sap || '-' || a.periodo || '-' || a.dsc_origen || dsc_agencia || '-' || dsc_unidad || '-' || a.dsc_cargo || '-' || a.dsc_indicador AS id_meta_comercial,
a.mes AS mes_comercial,
a.periodo,
-- informacion del canal y sub canal de la meta
a.id_canal,
a.des_canal,
a.id_subcanal,
b.des_subcanal,
a.ffvv,
--
a.dsc_origen,
a.dsc_localidad,
a.dsc_agencia,
a.dsc_unidad,
int.id_intermediario,
jfvp.id_persona,
'' AS id_bitacora_jerarquia_fuerza_ventas,
'' AS id_jerarquia_fuerza_ventas,
a.cod_sap,
jfvp.nom_promotor as dsc_nombre_asesor,
a.dsc_cargo,
a.dsc_indicador,
a.dsc_unidad_meta,
a.meta AS mnt_meta_comercial,
CURRENT_DATE() AS fec_insercion,
CURRENT_DATE() AS fec_modificacion,
false AS bq__soft_deleted
FROM 
t_metas_prom a
LEFT JOIN 
t_subcanal b 
ON (b.id_subcanal= a.id_subcanal)
LEFT JOIN 
t_bitacora_jerarquia_promotor_periodo jfvp 
ON (jfvp.des_periodo = CAST(a.mes AS string) AND jfvp.cod_sap = a.cod_sap)
LEFT JOIN
t_intermediario_rimac int
ON (a.id_intermediario = int.id_intermediario)
)
SELECT * FROM t_metas_comercial_parte1
where id_subcanal <> '708'
UNION ALL
SELECT * FROM t_metas_comercial_parte2