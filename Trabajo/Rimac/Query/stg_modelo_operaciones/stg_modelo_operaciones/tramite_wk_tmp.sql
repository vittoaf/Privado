DROP TABLE IF EXISTS  `{project_id_stgmsrl}.stg_modelo_operaciones.tramite_wk_tmp`;

CREATE TABLE  `{project_id_stgmsrl}.stg_modelo_operaciones.tramite_wk_tmp`
OPTIONS(                                                                 
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 DAY)
) 
AS
WITH tmp_monto_acuerdo
AS 
(
    SELECT C.IDEACUERDO,C.IDEPROD,C.IDEMAESTRO,C.idpmonacuerdo,C.STSACUERDO,S.NROSOLICITUD,Y.PRIMANETA,Y.PRIMABRUTA
    FROM `{project_id_raw}.bdsas__app_iaa_acuerdo.ACU_ACUERDO` C
    INNER JOIN `{project_id_raw}.bdsas__app_iaa_acuerdo.ACU_SOLICACUERDO` A ON c.IDECOTIZACION = A.IDEACUERDO
    INNER JOIN `{project_id_raw}.bdsas__app_iaa_acuerdo.ACU_SOLICSEG` S     ON S.IDESOLICSEG = A.IDESOLICSEG
    INNER JOIN `{project_id_raw}.bdsas__app_iaa_acuerdo.ACU_INSPLAN`  Y     ON C.IDEACUERDO = Y.IDEACUERDO
),
tmp_certificado
AS
(
    select
    a.nrosolicitud,
    a.ideacuerdo,
    a.idpmonacuerdo,                            ---MONEDA
    a.ideprod,                                  ---CODIGOPRODUCTO
    p.refexternaacx        as refexternaax,        ---CODIGO ACSELX
    p.dscproducto,                              ---DESCRIPCION PRODUCTO
    pol.NUMERO          as NumeroPoliza,        ---NUMERO DE POLIZA
    pol.fecsts,                                 ---FECHA DE EMISION
    a.STSACUERDO,                               ---ESTADO DE ACUERDO  --Emitido
    a.primabruta,
    pol.IDPTIPOVIGENCIA,
    pol.IDPTIPOACUSEG
    from tmp_monto_acuerdo a
    INNER JOIN `{project_id_raw}.bdsas__app_iaa_producto.PRO_PRODUCTO` p ON (p.ideprod = a.ideprod)
    INNER JOIN `{project_id_raw}.bdsas__app_iaa_acuerdo.ACU_ACUERDO` pol ON (pol.ideacuerdo = a.idemaestro)
),
tmp_ideroltercero
AS
(
select
        te.IDPTIPODOCUMENTO,
        te.NUMERODOC,
        rt.iderol,
        ra.iderolter,
        b.nrosolicitud,
        b.ideacuerdo,
        b.idpmonacuerdo,     --MONEDA
        b.ideprod,           --CODIGOPRODUCTO
        b.refexternaax,      --CODIGO ACSELX
        b.dscproducto,       --DESCRIPCION PRODUCTO
        b.NumeroPoliza,      -- NUMERO DE POLIZA
        b.fecsts,            --FECHA DE EMISION
        b.STSACUERDO,
        b.primabruta,        --b.IDPTIPOVIGENCIA,
        pvig.DESCRIPCION,
        b.IDPTIPOACUSEG,
        b.ideacuerdo  ideacuerdo_b,
        ra.ideacuerdo ideacuerdo_ra,
        cnl.id_canal as IdCanal
FROM tmp_certificado b
INNER JOIN `{project_id_raw}.bdsas__app_iaa_acuerdo.ACU_ROLACUERDO` ra ON b.ideacuerdo = ra.ideacuerdo 
INNER JOIN `{project_id_raw}.bdsas__app_iaa_tercero.TER_ROLTERCERO` rt ON rt.iderolter = ra.iderolter 
INNER JOIN `{project_id_raw}.bdsas__app_iaa_tercero.TER_TERCERO`    te ON te.idetercero = rt.idetercero 
LEFT JOIN `{project_id_stg}.stg_modelo_comercial.estructura_canal`   cnl ON (lower(trim(cnl.dsc_grupo_canal)) ='worksite')
LEFT JOIN `{project_id_raw}.bdsas__app_iaa_comunes.CFG_PARAMETRO`   pvig ON (pvig.idetippar='PRO_UNIDADPERIODO' AND pvig.CODIGOC = B.IDPTIPOVIGENCIA)
WHERE rt.iderol = 10
),
persona_data
AS
(
	SELECT 
		b.num_documento,
        UPPER(TRIM(b.tip_documento)) as tip_documento,
        ARRAY_AGG(a.id_persona			ORDER BY left(a.id_persona,2),a.id_persona DESC)[OFFSET(0)] AS id_persona,
        ARRAY_AGG(a.nom_completo		ORDER BY left(a.id_persona,2),a.id_persona DESC)[OFFSET(0)] AS nom_completo,
		ARRAY_AGG(a.cod_acselx			ORDER BY left(a.id_persona,2),a.id_persona DESC)[OFFSET(0)] AS cod_ax
	FROM `{project_id_stg}.stg_modelo_persona.persona` a
    CROSS JOIN UNNEST (a.documento_identidad) b
	WHERE 
		b.ind_documento_principal = '1' -- 1: DNI. Si se desea todos los documentos asociados, se quita este filtro.
		AND UPPER(TRIM(b.tip_documento)) IN ('DNI','RUC')
		AND IFNULL(a.bq__soft_deleted,false)=false
	GROUP BY b.num_documento,UPPER(TRIM(b.tip_documento))
),
producto_data
AS
(
	select
	coalesce(if(PO.id_origen='AX',PO.cod_producto,null),P.cod_producto) as cod_producto,
	MAX(P.id_producto) AS id_producto
	FROM `{project_id_stg}.stg_modelo_producto.producto` P,
	UNNEST (P.producto_origen) as PO
	GROUP BY coalesce(if(PO.id_origen='AX',PO.cod_producto,null),P.cod_producto)
)

SELECT
	('WK-' || CAST(ROW_NUMBER() OVER(ORDER BY FORMAT_DATE('%Y%m', s.feccreacion),f.nrosolicitud,trim(f.refexternaax)) AS String)
     ||'-' ||  FORMAT_DATE('%Y%m', s.feccreacion) ||'-'|| f.nrosolicitud || '-' || trim(f.refexternaax) ) as numtramite,
'WORKYBOT'                                as origen_data,
cast(s.feccreacion as timestamp)          as feccreacion,
'Emision'                                 as tipotramite,        
'Nueva Poliza'                            as tipoperacion,
cast(null as string)                      as glosa,
cast(s.fecingreso as timestamp)           as fecsolicitud , --de tabla de worksite
'No Aplica'                 as Area_Tramite,                  
'No Aplica'                 as emisor_encargado,
false                       AS bq__soft_deleted,
per.cod_ax                  AS numidcliente,
per.id_persona              AS id_contratante,
f.NUMERODOC                 AS doccliente,
per.nom_completo            AS nomcliente,
trim(f.refexternaax)        AS codproducto,
pro.id_producto             AS id_producto,
t.refexternaax              AS numidbroker, --Cod Rimac
f.NumeroPoliza              as numpoliza,    
CASE
WHEN f.NumeroPoliza LIKE '%|%' THEN CAST(SAFE_CAST(TRIM(REGEXP_REPLACE(SUBSTR(f.NumeroPoliza,STRPOS(f.NumeroPoliza, '|')+1),'[^0-9 ]','')) AS BIGINT) AS STRING)
WHEN f.NumeroPoliza LIKE '%-%' THEN CAST(SAFE_CAST(TRIM(REGEXP_REPLACE(SUBSTR(f.NumeroPoliza,STRPOS(f.NumeroPoliza, '-')+1),'[^0-9 ]','')) AS BIGINT) AS STRING)
ELSE CAST(SAFE_CAST(TRIM(REGEXP_REPLACE(f.NumeroPoliza,'[^0-9 ]','')) AS BIGINT) AS STRING)
END  as numpoliza_buscar,
'FUERZA DE VENTA' AS canalventa,
'FUERZA DE VENTA' AS canalventa_homologado,
--Campos Worksite: Inicio
f.idpmonacuerdo             AS Id_Moneda,                           --Nuevo por WK
ROUND(f.primabruta*12,2)    as mnt_prima_emitida_bruta_anualizada,  --Nuevo por WK
f.fecsts                    AS Fecha_Emision,
cl.cluster as cod_cluster,
cfg_cluster.DESCRIP_HOMOLOGADO as des_cluster,
s.estado_solicitud          AS est_estado_solicitud,
p.abre_parametro            AS des_estado_solicitud,
via.id_persona              AS id_persona_via,
via.id_persona || '-RUC-' || trim(s.ruc_via ) || '-' || trim(s.sub_via) AS id_sede_via
--Campos Worksite: Fin
FROM `{project_id_raw}.ue1dbaprodrds001__db_worksite.wkt_solicitud` s
INNER JOIN `{project_id_raw}.ue1dbaprodrds001__db_worksite.wkt_roltercero` rt ON rt.cod_roltercero = s.cod_rolasesor
INNER JOIN `{project_id_raw}.ue1dbaprodrds001__db_worksite.wkt_tercero` t ON t.cod_tercero = rt.cod_tercero
INNER JOIN tmp_ideroltercero f ON s.nro_solicitud = f.nrosolicitud
LEFT JOIN  persona_data per ON (per.tip_documento='DNI' AND SAFE_CAST(TRIM(REGEXP_REPLACE(per.num_documento,'[^0-9 ]','')) AS BIGINT)=SAFE_CAST(TRIM(REGEXP_REPLACE(f.NUMERODOC,'[^0-9 ]','')) AS BIGINT))
LEFT JOIN  persona_data via ON (via.tip_documento='RUC' AND SAFE_CAST(TRIM(REGEXP_REPLACE(via.num_documento,'[^0-9 ]','')) AS BIGINT)=SAFE_CAST(TRIM(REGEXP_REPLACE(s.ruc_via,'[^0-9 ]','')) AS BIGINT))
LEFT JOIN  producto_data pro ON (pro.cod_producto=trim(f.refexternaax))
LEFT JOIN `{project_id_raw}.de__canales.wor_pro_segmentacion_vias` cl ON (cl.ruc=SAFE_CAST(TRIM(REGEXP_REPLACE(s.ruc_via,'[^0-9 ]','')) AS BIGINT))
LEFT JOIN `{project_id_raw}.ue1dbaprodrds001__db_worksite.wkt_parametro` p  ON (p.cod_tipoparametro = 2 AND p.codigoc = s.estado_solicitud)
LEFT JOIN `{project_id_raw}.de__datalake.cfg_staging_maestra` cfg_cluster
	ON (
			cfg_cluster.ID_ORIGEN='TRMWKST' 
			AND cfg_cluster.TIPO_PARAMETRO='CLUSTER' 
			AND cfg_cluster.CODIGO_ORIGEN=cl.cluster
		);