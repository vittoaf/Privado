WITH 
NuevoCalendario as (
SELECT Periodo,FecCierre,FecSuscripcion,FecEmision
 FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.NuevoCalendario_Vitto`
),
persona_data
AS (
	SELECT 
		a.id_persona,
        ARRAY_AGG(b.num_documento		ORDER BY left(a.id_persona,2),a.id_persona DESC)[OFFSET(0)] AS num_documento,
        ARRAY_AGG(a.nom_completo		ORDER BY left(a.id_persona,2),a.id_persona DESC)[OFFSET(0)] AS nom_completo,
		ARRAY_AGG(a.cod_acselx			ORDER BY left(a.id_persona,2),a.id_persona DESC)[OFFSET(0)] AS cod_ax
	FROM `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
    CROSS JOIN UNNEST (a.documento_identidad) b
	WHERE 
		b.ind_documento_principal = '1' -- 1: DNI. Si se desea todos los documentos asociados, se quita este filtro.
		AND UPPER(TRIM(b.tip_documento))='DNI'
		AND IFNULL(a.bq__soft_deleted,false)=false
	GROUP BY a.id_persona
),
producto_data
AS
(
	select
	P.id_producto,
	MAX(coalesce(if(PO.id_origen='AX',PO.cod_producto,null),P.cod_producto)) as cod_producto,
	FROM `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_producto.producto` P,
	UNNEST (P.producto_origen) as PO
	GROUP BY P.id_producto
),
producto_ax_data
AS
(
	select
	coalesce(if(PO.id_origen='AX',PO.cod_producto,null),P.cod_producto) as cod_producto,
	MAX(P.id_producto) AS id_producto
	FROM `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_producto.producto` P,
	UNNEST (P.producto_origen) as PO
	GROUP BY coalesce(if(PO.id_origen='AX',PO.cod_producto,null),P.cod_producto)
),
-- Actualizar el intermediario para el caso de los tramites historicos de RRVV
jerarquia_ffvv_data_xcodsap AS (
	SELECT
		CAST(jfv.cod_sap AS STRING) AS cod_sap ,
		max(jfv.id_intermediario_rimac) AS id_intermediario_rimac   -- campo string
	FROM 
		--`rs-nprd-dlk-dt-stg-mlk-3d01.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv		
	WHERE 
		LOWER(jfv.dsc_tipo_puesto)='asesor'
        AND jfv.cod_sap <> 0
	GROUP BY 
		CAST(jfv.cod_sap AS STRING)
),
tramites_hist_rentas as (
    SELECT thr.nro_tramite_solicitud AS numtramite ,
           trim(thr.cod_sap_asesor_final) AS cod_sap_asesor_final ,
           jfv.id_intermediario_rimac AS id_intermediario ,
           trim(cast(cod_producto_final AS STRING))  AS  cod_producto_final 
    --FROM `rs-nprd-dlk-data-rwz-51a6.de__canales.tramites_hist_rentas` thr 
	FROM `rs-nprd-dlk-dd-rwz-a406.de__canales.tramites_hist_rentas` thr 
    LEFT JOIN jerarquia_ffvv_data_xcodsap jfv ON ( jfv.cod_sap  = TRIM(thr.cod_sap_asesor_final) )
    WHERE TRIM(CAST(cod_producto_final AS STRING)) IN ('8822','8827','8102','8917','8901')  -- solo productos del funnel de Rentas 
	  AND COALESCE( SAFE_CAST(thr.cod_sap_asesor_final AS NUMERIC) || '' , '') <> ''  -- que presente un codigo sap correcto
      AND thr.nro_tramite_solicitud <> 'PENDIENTE'
),
tmp_tramite AS (
SELECT 
		trm.numtramite,
		'LOTUS' AS origen_data,
		trm.feccreacion,
		trm.tipotramite,
		trm.tipooperacion,
		trm.glosa,
		trm.fecsolicitud,
		trm.areatramite,
		trm.emisorencargado,
		trm.BQ__SOFT_DELETED,
		trm.numidcliente, 
		CONCAT('AX-', SAFE_CAST(SAFE_CAST(trm.numidcliente AS numeric) AS string)) AS id_contratante, 
		trm.doccliente,
		trm.nomcliente,
		trm.codproducto,
		pro.id_producto AS id_producto, 
		-- identificador del intermediario o broker
		CASE 
			WHEN thr.numtramite IS NOT NULL THEN thr.id_intermediario   -- caso de los tramites de rentas historicos.
			WHEN UPPER(trm.nombroker) = 'SEGUROS DIRECTOS' THEN '43' 	-- hay casos que vienen con '00043' o '0'
			ELSE SAFE_CAST(SAFE_CAST(TRIM(trm.numidbroker) AS numeric) AS string) 
		END AS numidbroker,
		--
		TRIM(trm.numpoliza) as numpoliza,
		CASE 
			WHEN INSTR(TRIM(trm.numpoliza),'|') > 0 THEN TRIM(SUBSTRING(TRIM(trm.numpoliza),INSTR(TRIM(trm.numpoliza),'|')+1,20))
			WHEN INSTR(TRIM(trm.numpoliza),' ') > 0 THEN SAFE_CAST(SAFE_CAST(TRIM(SUBSTRING(TRIM(trm.numpoliza),INSTR(TRIM(trm.numpoliza),' ')+1,20)) AS numeric) AS string)
			WHEN SUBSTRING(TRIM(trm.numpoliza),1,1) = '0' THEN SAFE_CAST(SAFE_CAST(trm.numpoliza AS numeric) AS string)
			WHEN SUBSTRING(TRIM(trm.numpoliza),1,1) in ('V','U') THEN TRIM(regexp_replace(TRIM(trm.numpoliza),'[^0-9 ]',''))
			ELSE TRIM(trm.numpoliza) 
		END numpoliza_buscar, -- numero de poliza a buscar 
		trm.canalventa,
		CASE 
			WHEN trm.canalventa='FFVV' THEN 'FUERZA DE VENTA'
			WHEN trm.canalventa='Directo' THEN 'DIRECTO'
			WHEN trm.canalventa='Broker' THEN 'CORREDORES'
			ELSE 'NO DETERMINADO'
		END AS canalventa_homologado,
        cast(null as string) AS Id_Moneda,
        cast(null as NUMERIC) AS mnt_prima_emitida_bruta_anualizada,
        cast(null as DATE) AS Fecha_Emision,
        cast(null as string) AS cod_cluster,
		cast(null as string) AS des_cluster,
        cast(null as string) AS est_estado_solicitud,
        cast(trm.ESTADO as string) AS des_estado_solicitud,
        cast(null as string) AS id_persona_via,
        cast(null as  string) AS id_sede_via
	FROM `rs-nprd-dlk-dd-rwz-a406.bdwf__appnote.TRAMITE` trm
	LEFT JOIN tramites_hist_rentas thr on ( thr.numtramite = trm.numtramite )  -- tramites historicos de rentas
	LEFT JOIN producto_ax_data pro ON (pro.cod_producto=trim(trm.codproducto))
	WHERE 
		--1 = 1 -- (diferente de 8202) or (diferente glosa de journey express para 8917)
		 /*(case 
		 	WHEN trm.codproducto = '8917' AND trm.glosa LIKE '%JOURNEY EXPRESS%' THEN 0
  			WHEN trm.codproducto = '8202' THEN 0
  			ELSE 1 END) = 1*/
     trm.doccliente = '77272512')
  select origen_data,numtramite,tipooperacion,glosa,numidbroker,id_producto,numpoliza from tmp_tramite

  -----

  select codproducto_final,origen_data,nro_doc_cliente,id_estado_cotizacion
,des_estado_cotizacion,canal,cot_nro_poliza  from 
`rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_journey_tmp_vitto`
where (nro_doc_cliente = '29554615' and codproducto_final = '8202')
or (nro_doc_cliente = '001739744' and codproducto_final = '8917')
or (nro_doc_cliente = '10064074' and codproducto_final = '8102')
;

Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_cotweb_tmp_vitto` 
where (nro_doc_cliente = '10064074' and codproducto_final = '8102')
;

select codproducto_final,origen_data,nro_doc_cliente,codproducto_final,id_estado_cotizacion
,des_estado_cotizacion,pago_flg,numpol,canal,periodo from
`rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto`
where nro_doc_cliente = '10064074'

----


WITH
  aws AS (
  SELECT
    B.COD_SAP_FINAL,
    COUNT(DISTINCT B.NRO_POLIZA_FINAL) cantidad,
    (
    SELECT
      count (1)
    FROM
      `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.aws_bd_produccion` A
    WHERE
      MES_PRODUCCION = '202208'
      AND FECHA_EMISION_SISTEMA IS NULL
      AND A.COD_SAP_FINAL = B.COD_SAP_FINAL ) Fecha_Emision_Sistema_Nulo,
    STRUCT(
            ARRAY_AGG(IFNULL(B.NRO_TRAMITE,'')) AS nro_tramite,
            ARRAY_AGG(IFNULL(B.NRO_POLIZA_FINAL,'')) AS numpol,
            ARRAY_AGG(IFNULL(B.COD_PRODUCTO_FINAL,'')) AS Cod_producto,
            ARRAY_AGG(IFNULL(p.id_persona,'')) AS id_persona,
            ARRAY_AGG(IFNULL(B.NRO_DOC_CONTRATANTE_FINAL,'')) AS DNI
    
     )aws
  FROM
    `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.aws_bd_produccion` B
    left join (
            SELECT 
		a.id_persona, 
		b.tip_documento, 
		b.num_documento, 
		a.nom_completo
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		CROSS JOIN UNNEST (a.documento_identidad) b
    ) p on B.NRO_DOC_CONTRATANTE_FINAL = p.num_documento  and p.tip_documento in ('DNI','CE')
  WHERE
    B.MES_PRODUCCION = '202208'
    AND B.FECHA_EMISION_SISTEMA >= '2022-08-01'
    AND B.FECHA_EMISION_SISTEMA <= '2022-08-31'
  GROUP BY
    B.COD_SAP_FINAL),
  gcp AS (
  SELECT
    bit_cod_sap_asesor,
    COUNT(DISTINCT pol_id_poliza) cantidad,
    STRUCT(ARRAY_AGG(IFNULL(pol_id_poliza,'')) AS id_poliza,
      ARRAY_AGG(IFNULL(trm_id_tramite,'')) AS trm_id_tramite,
      ARRAY_AGG(IFNULL(trm_id_producto,'')) AS trm_id_producto,
      ARRAY_AGG(IFNULL(trm_id_contratante,'')) AS trm_id_contratante ) poliza
  FROM
    `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_cambios_vitto`
  WHERE
    trm_mes_produccion = "2022-08-01"
    AND bit_des_subcanal = "FFVV VIDA"
    AND bit_categoria <> "SIN DATOS"
  GROUP BY
    bit_cod_sap_asesor )
SELECT
  *
FROM
  aws
FULL JOIN
  gcp
ON
  aws.COD_SAP_FINAL = CAST(gcp.bit_cod_sap_asesor AS string)
WHERE
  gcp.cantidad <> aws.cantidad
  AND gcp.cantidad > 0