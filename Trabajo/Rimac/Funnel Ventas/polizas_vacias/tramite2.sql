
WITH persona_data
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
tramites_8202_8917_journeyexpress AS(
--A PARTIR DEL 202106:
--IDENTIFICAR TRAMITES DE PRODUCTO 8202 Y
--IDENTIFICAR TRAMITES DE PRODUCTO 8917 QUE TENGAN AL MENOS UN TRAMITE EN GLOSA JOURNEY EXPRESS 
SELECT 
trm.numtramite,
pe.id_persona,
trm.numpoliza,trm.CODPRODUCTO
FROM `rs-nprd-dlk-dd-rwz-a406.bdwf__appnote.TRAMITE` trm
LEFT JOIN (SELECT 
		a.id_persona,b.num_documento
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		CROSS JOIN UNNEST (a.documento_identidad) b
  where b.ind_documento_principal = '1' and TRIM(b.tip_documento) = 'DNI') pe ON pe.num_documento = trm.DOCCLIENTE 
WHERE 
 DATE_TRUNC(CAST(trm.FECCREACION AS DATE), MONTH) >= '2021-06-01' 
 AND (
 		trm.codproducto||IFNULL(trm.doccliente,'0') in (
   													Select codproducto||IFNULL(doccliente,'0') 
													FROM `rs-nprd-dlk-dd-rwz-a406.bdwf__appnote.TRAMITE`
   													WHERE
   													codproducto = '8917' AND glosa LIKE '%JOURNEY EXPRESS%'
		 										) 
 		OR trm.codproducto = '8202')
),
Cotizacion8917 as (
Select numpol_ext_cot,id_persona_cliente
 from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_cambio_vitto` 
where id_producto_final = 'AX-8917'
),
tramites_8202_8917_journeyexpress_eliminar AS (
	Select trm.numtramite 
	FROM tramites_8202_8917_journeyexpress trm 
	LEFT JOIN Cotizacion8917 c ON trm.numpoliza= c.numpol_ext_cot AND trm.id_persona=c.id_persona_cliente
	where 
		not (c.numpol_ext_cot is null and trm.numpoliza is not null and trm.codproducto='8917')
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
	LEFT JOIN tramites_8202_8917_journeyexpress_eliminar tje ON (tje.numtramite = trm.numtramite )
	WHERE 
		tje.numtramite IS NULL
		 /*(case 
		 	WHEN trm.codproducto = '8917' AND trm.glosa LIKE '%JOURNEY EXPRESS%' THEN 0
  			WHEN trm.codproducto = '8202' THEN 0
  			ELSE 1 END) = 1*/
	
	UNION ALL
	
	SELECT
	('JV-' || cot.id_cotizacion ||'-'||
	FORMAT_DATE('%Y%m', cot.fecha_estado_asesoria) ||
	if(cot.id_cotizacion_origen is null,'','-'||cot.id_cotizacion_origen) ) as numtramite,
	'JOURNEY DIGITAL' AS origen_data,
	cast(cot.fecha_estado_asesoria as timestamp) as feccreacion,
	'Emision' AS tipotramite,
	'Nueva Póliza' AS tipooperacion,
	cast(null as string) AS glosa,
	cast(cot.fecha_estado_asesoria as timestamp) as fecsolicitud,
	cast(null as string) AS areatramite,
	cast(null as string) AS emisorencargado,
	false AS bq__soft_deleted,
	replace(per.id_persona,'AX-','') AS numidcliente,
	cot.id_persona_cliente AS id_contratante,
	per.num_documento AS doccliente,
	per.nom_completo AS nomcliente,
	pro.cod_producto AS codproducto,
	cot.id_producto_final AS id_producto,
	cast(cot.id_intermediario as string) AS numidbroker,
	cot.id_cotizacion_origen AS numpoliza,
	cot.numpol_ext_cot AS numpoliza_buscar,
	'FUERZA DE VENTA' AS canalventa,
	'FUERZA DE VENTA' AS canalventa_homologado,
    cast(null as string) AS Id_Moneda,
    cast(null as NUMERIC) AS mnt_prima_emitida_bruta_anualizada,
    cast(null as DATE) AS Fecha_Emision,
    cast(null as string) AS cod_cluster,
	cast(null as string) AS des_cluster,
    cast(cot.id_estado_cotizacion as string) AS est_estado_solicitud,
    cast(cot.des_estado_cotizacion as string) AS des_estado_solicitud,
    cast(null as string) AS id_persona_via,
    cast(null as string) AS id_sede_via
	--FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_vitto` cot
	FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_cambio_vitto` cot
	LEFT JOIN persona_data per
	ON (TRIM(cot.id_persona_cliente)=TRIM(per.id_persona))
	LEFT JOIN producto_data pro
	ON (pro.id_producto=trim(cot.id_producto_final))
	WHERE cot.origen_tramite='JOURNEY DIGITAL'


),
tmp_estructura_canal AS (
	SELECT DISTINCT 
		id_canal, 
		dsc_canal
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.estructura_canal`
),
producto AS (
	SELECT 
		p.id_producto, 
		p.nom_producto, 
		p.cod_producto, 
		p.id_origen
	FROM (
		SELECT 
			pr.id_producto,
			po.nom_producto,
			po.cod_producto,
			po.id_origen
		FROM 
			`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_producto.producto` AS pr
			CROSS JOIN UNNEST (pr.producto_origen) AS po
		WHERE 
			pr.id_origen = "AX"
	) p
	WHERE 
		p.id_origen = "AX"
),
mes_prod_data AS (
	SELECT
		TRIM(a.mes_produccion) as mes_produccion,
		UPPER(TRIM(a.canal)) AS canal,
		ARRAY_AGG(DATE_ADD(DATE "1899-12-30", INTERVAL CAST(a.FECHA_FIN_SOLICITUD AS INTEGER) DAY) ORDER BY a.FECHA_FIN_SOLICITUD DESC)[OFFSET(0)] AS fec_fin_solicitud,
		ARRAY_AGG(DATE_ADD(DATE "1899-12-30", INTERVAL CAST(a.FECHA_FIN_TRAMITE AS INTEGER) DAY) ORDER BY a.FECHA_FIN_SOLICITUD DESC)[OFFSET(0)] AS fec_fin_tramite,
		ARRAY_AGG(DATE_ADD(DATE "1899-12-30", INTERVAL CAST(a.FECHA_FIN_EMISION AS INTEGER) DAY) ORDER BY a.FECHA_FIN_SOLICITUD DESC)[OFFSET(0)] AS fec_fin_emision
	FROM `rs-nprd-dlk-dd-rwz-a406.de__canales.tramites_mes_produccion` a
	--WHERE a.CANAL='VIDA'
	GROUP BY TRIM(a.mes_produccion),UPPER(TRIM(a.canal))
),
poliza_contratante AS (
	SELECT 
		pol.id_producto,
		pol.num_poliza, 
		pol.id_contratante,
		ARRAY_AGG(pol.id_poliza ORDER BY SAFE_CAST(id_poliza_origen AS numeric) DESC)[OFFSET(0)] AS id_poliza,
		ARRAY_AGG(pol.id_poliza_origen ORDER BY SAFE_CAST(id_poliza_origen AS numeric) DESC)[OFFSET(0)] AS id_poliza_origen,
		ARRAY_AGG(pol.fec_emision ORDER BY SAFE_CAST(id_poliza_origen AS numeric) DESC)[OFFSET(0)] AS fec_emision,
		ARRAY_AGG(pol.id_moneda ORDER BY SAFE_CAST(id_poliza_origen AS numeric) DESC)[OFFSET(0)] AS id_moneda,
		ARRAY_AGG(pol.mnt_prima_emitida_bruta_anualizada ORDER BY SAFE_CAST(id_poliza_origen AS numeric) DESC)[OFFSET(0)] AS mnt_prima_emitida_bruta_anualizada,
		ARRAY_AGG(
			CASE 
				WHEN SUBSTRING(pol.num_poliza,1,1) in ('V','U') THEN TRIM(regexp_replace(pol.num_poliza,'[^0-9 ]',''))
				WHEN SUBSTRING(pol.num_poliza,1,1) = '0' THEN SAFE_CAST(SAFE_CAST(pol.num_poliza AS numeric) AS string) 
				ELSE pol.num_poliza 
			END
			ORDER BY 
				SAFE_CAST(id_poliza_origen AS numeric) DESC
		)[OFFSET(0)] AS numpoliza_buscar 
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` pol
	WHERE 
		pol.id_contratante IS NOT NULL
	-- WHERE num_poliza in ('V-918723','753235', '262129','28966592','84749','0000005259','V-1756753') 
	GROUP BY 
		id_producto, 
		num_poliza, 
		id_contratante
),
poliza_unica AS (
	SELECT 
		pol.id_producto,
		pol.num_poliza, 
		ARRAY_AGG(pol.id_poliza ORDER BY SAFE_CAST(id_poliza_origen AS numeric) DESC)[OFFSET(0)] AS id_poliza,
		ARRAY_AGG(pol.id_poliza_origen ORDER BY SAFE_CAST(id_poliza_origen AS numeric) DESC)[OFFSET(0)] AS id_poliza_origen,
		ARRAY_AGG(pol.id_contratante ORDER BY SAFE_CAST(id_poliza_origen AS numeric) DESC)[OFFSET(0)] AS id_contratante,
		ARRAY_AGG(pol.fec_emision ORDER BY SAFE_CAST(id_poliza_origen AS numeric) DESC)[OFFSET(0)] AS fec_emision,
		ARRAY_AGG(pol.id_moneda ORDER BY SAFE_CAST(id_poliza_origen AS numeric) DESC)[OFFSET(0)] AS id_moneda,
		ARRAY_AGG(pol.mnt_prima_emitida_bruta_anualizada ORDER BY SAFE_CAST(id_poliza_origen AS numeric) DESC)[OFFSET(0)] AS mnt_prima_emitida_bruta_anualizada,
		ARRAY_AGG(
			CASE 
				WHEN SUBSTRING(pol.num_poliza,1,1) in ('V','U') THEN TRIM(regexp_replace(pol.num_poliza,'[^0-9 ]',''))
				WHEN SUBSTRING(pol.num_poliza,1,1) = '0' THEN SAFE_CAST(SAFE_CAST(pol.num_poliza AS numeric) AS string) 
				ELSE pol.num_poliza 
			END
			ORDER BY 
				SAFE_CAST(id_poliza_origen AS numeric) DESC
		)[OFFSET(0)] AS numpoliza_buscar 
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` pol
	GROUP BY 
		id_producto, 
		num_poliza 
), 
intermediario_mc AS (
	SELECT 
		cod_rimac,
		dsc_canal, 
		ARRAY_AGG(id_intermediario ORDER BY fec_insercion DESC)[OFFSET(0)] AS id_intermediario
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.intermediario_mc` 
	WHERE 
		TRIM(est_intermediario) ='ACTIVO'
	GROUP BY 
		cod_rimac, 
		dsc_canal
),
intermediario_unico AS (
	SELECT 
		cod_rimac,
		ARRAY_AGG(id_intermediario ORDER BY TRIM(est_intermediario), fec_insercion DESC)[OFFSET(0)] AS id_intermediario -- se adiciona el estado
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.intermediario_mc` 
	--WHERE TRIM(est_intermediario) ='ACTIVO'
	GROUP BY 
		cod_rimac
),
stg_tramite AS (
	SELECT 
		DATE_TRUNC(SAFE_CAST(a.feccreacion AS DATE), month) AS periodo_creacion,
		CASE WHEN 
              (DATE_TRUNC(SAFE_CAST(a.feccreacion AS DATE), month) >= '2021-06-01') 
          THEN 
            CASE 
              WHEN COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision) IS NOT NULL
              THEN DATE_TRUNC(SAFE_CAST(COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision) AS DATE), month)
            ELSE
              CASE 
                WHEN SAFE_CAST(a.feccreacion AS DATE) <= (Select CAST(DATE_ADD(DATE "1899-12-30", INTERVAL CAST(N.FECHA_FIN_SOLICITUD AS INTEGER) DAY)  AS DATE) 
                                                          FROM `rs-nprd-dlk-data-rwz-51a6.de__canales.tramites_mes_produccion` N 
                                                          WHERE format_date('%Y%m',SAFE_CAST(a.feccreacion AS TIMESTAMP)) = TRIM(N.mes_produccion)
                                                                AND upper(trim(N.canal)) = 'VIDA')
                THEN DATE_TRUNC(SAFE_CAST(a.feccreacion AS DATE), month)
              ELSE
                DATE_ADD(DATE_TRUNC(SAFE_CAST(a.feccreacion AS DATE), month),INTERVAL 1 MONTH)
              END
            END
		ELSE
        CASE
          WHEN COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision) IS NULL
            AND m.fec_fin_solicitud >= SAFE_CAST(COALESCE(a.fecsolicitud,a.feccreacion) AS DATE)
            AND m.fec_fin_tramite >= SAFE_CAST(a.feccreacion AS DATE)
          THEN DATE_TRUNC(SAFE_CAST(a.feccreacion AS DATE), month)
          WHEN COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision) IS NOT NULL
            AND m.fec_fin_solicitud >= SAFE_CAST(COALESCE(a.fecsolicitud,a.feccreacion) AS DATE)
            AND m.fec_fin_tramite >= SAFE_CAST(a.feccreacion AS DATE)
            AND m.fec_fin_emision >= COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision)
          THEN DATE_TRUNC(SAFE_CAST(a.feccreacion AS DATE), month)
          WHEN COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision) IS NOT NULL
            AND m.fec_fin_solicitud >= SAFE_CAST(COALESCE(a.fecsolicitud,a.feccreacion) AS DATE)
            AND m.fec_fin_tramite >= SAFE_CAST(a.feccreacion AS DATE)
            AND m.fec_fin_emision < COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision)
            AND m1.fec_fin_emision >= COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision)
          THEN DATE_ADD(DATE_TRUNC(SAFE_CAST(a.feccreacion AS DATE), month), INTERVAL 1 MONTH)
          WHEN COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision) IS NOT NULL
            AND m.fec_fin_solicitud >= SAFE_CAST(COALESCE(a.fecsolicitud,a.feccreacion) AS DATE)
            AND m.fec_fin_tramite >= SAFE_CAST(a.feccreacion AS DATE)
            AND m.fec_fin_emision < COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision)
            AND m1.fec_fin_emision < COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision)
            AND m2.fec_fin_emision >= COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision)
          THEN DATE_ADD(DATE_TRUNC(SAFE_CAST(a.feccreacion AS DATE), month), INTERVAL 2 MONTH)
          WHEN m.mes_produccion IS NULL
          THEN DATE_TRUNC(SAFE_CAST(a.feccreacion AS DATE), month)
          ELSE DATE_ADD(DATE_TRUNC(SAFE_CAST(a.feccreacion AS DATE), month), INTERVAL 1 MONTH)
        END 
		END AS mes_produccion,
		a.numtramite,
		a.origen_data,
		CASE 
			WHEN b.id_persona IS NOT NULL THEN b.id_persona 
			WHEN e.id_poliza IS NOT NULL AND e.id_contratante IS NOT NULL THEN e.id_contratante
			WHEN pu.id_poliza IS NOT NULL AND pu.id_contratante IS NOT NULL THEN pu.id_contratante
		END AS id_cliente,
        a.doccliente as num_documento_cliente,
		IF(d.id_intermediario IS NOT NULL,d.id_intermediario, iu.id_intermediario) AS id_intermediario, 
        a.numidbroker as des_id_intermediario_origen,
		a.tipotramite AS tipo_tramite,
		a.tipooperacion AS tipo_operacion,
		c.id_producto,
		a.glosa,
		COALESCE(e.id_poliza,pu.id_poliza) AS id_poliza,
		UPPER(TRIM(COALESCE(e.id_moneda,pu.id_moneda,a.id_moneda))) AS id_moneda,
		COALESCE(e.mnt_prima_emitida_bruta_anualizada,pu.mnt_prima_emitida_bruta_anualizada,a.mnt_prima_emitida_bruta_anualizada) AS mnt_prima_emitida_bruta_anualizada,
		tc.TASACAMBIO AS tasa_cambio,
		CASE UPPER(TRIM(COALESCE(e.id_moneda,pu.id_moneda,a.id_moneda)))
			WHEN 'SOL' THEN CAST(ROUND(COALESCE(e.mnt_prima_emitida_bruta_anualizada,pu.mnt_prima_emitida_bruta_anualizada,a.mnt_prima_emitida_bruta_anualizada)/tc.tasacambio,2) AS decimal)
			WHEN 'USD' THEN CAST(ROUND(COALESCE(e.mnt_prima_emitida_bruta_anualizada,pu.mnt_prima_emitida_bruta_anualizada,a.mnt_prima_emitida_bruta_anualizada),2) AS decimal)
		END AS mnt_prima_emitida_bruta_anualizada_usd,
		CASE UPPER(TRIM(COALESCE(e.id_moneda,pu.id_moneda,a.id_moneda)))
			WHEN 'SOL' THEN CAST(ROUND(COALESCE(e.mnt_prima_emitida_bruta_anualizada,pu.mnt_prima_emitida_bruta_anualizada,a.mnt_prima_emitida_bruta_anualizada),2) AS decimal)
			WHEN 'USD' THEN CAST(ROUND(COALESCE(e.mnt_prima_emitida_bruta_anualizada,pu.mnt_prima_emitida_bruta_anualizada,a.mnt_prima_emitida_bruta_anualizada)*tc.tasacambio,2) AS decimal)
		END AS mnt_prima_emitida_bruta_anualizada_sol,
		SAFE_CAST(a.fecsolicitud AS DATE) AS fec_solicitud,
		SAFE_CAST(a.feccreacion AS DATE) AS fec_creacion,
		COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision) AS fec_emision,
		a.areatramite AS area_tramite,
		SAFE_CAST(COALESCE(SAFE_CAST(f.id_canal AS string),'NO DETERMINADO') AS INTEGER) AS id_canal,
		a.emisorencargado AS emisor_encargado,
        a.cod_cluster,
		a.des_cluster,
		a.est_estado_solicitud,
		a.des_estado_solicitud,
		a.id_persona_via,
        a.id_sede_via,

		CURRENT_DATE() AS fec_insercion,
		CURRENT_DATE() AS fec_modificacion,
		a.BQ__SOFT_DELETED AS bq__soft_deleted
	FROM 
		tmp_tramite a 
		LEFT JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` b 
			ON a.id_contratante = b.id_persona 
		LEFT JOIN producto c 
			ON a.id_producto = c.id_producto 
		LEFT JOIN intermediario_mc d 
			ON TRIM(a.numidbroker) = TRIM(d.cod_rimac) 
				AND d.dsc_canal = a.canalventa_homologado
				AND a.canalventa_homologado <>'NO DETERMINADO' 
		LEFT JOIN intermediario_unico iu 
			ON TRIM(a.numidbroker) = TRIM(iu.cod_rimac) 
			-- AND a.canalventa_homologado = 'NO DETERMINADO' 
		LEFT JOIN poliza_contratante e 
			ON a.numpoliza_buscar = e.numpoliza_buscar 
				AND a.id_producto = e.id_producto 
				AND a.id_contratante = e.id_contratante
				AND a.id_contratante IS NOT NULL 
		LEFT JOIN poliza_unica pu
			ON a.numpoliza_buscar = pu.numpoliza_buscar 
				AND a.id_producto = pu.id_producto 
				AND  (a.id_contratante IS NULL or pu.id_contratante IS NULL OR b.id_persona IS NULL )
		LEFT JOIN tmp_estructura_canal f 
			ON a.canalventa_homologado = f.dsc_canal
		LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.prod__acselx.TASA_CAMBIO` tc
			ON UPPER(TRIM(TC.TIPOTASA))='I'
				AND UPPER(TRIM(TC.CODMONEDA))='USD'
				AND DATE(TC.FECHORACAMBIO)=COALESCE(e.fec_emision,pu.fec_emision,a.fecha_emision,SAFE_CAST(a.feccreacion AS DATE))
		LEFT JOIN mes_prod_data m
			ON (CAST(m.mes_produccion AS INTEGER) =CAST(FORMAT_DATE('%Y%m', SAFE_CAST(a.feccreacion AS DATE)) AS INTEGER)
				AND
				(
					(m.canal IN ('VIDA') AND a.origen_data IN ('LOTUS','JOURNEY DIGITAL'))
					OR
					(m.canal IN ('WORKSITE') AND a.origen_data IN ('WORKYBOT'))
				)	)
		LEFT JOIN mes_prod_data m1
			ON (CAST(m1.mes_produccion AS INTEGER)=CAST(FORMAT_DATE('%Y%m', DATE_ADD(DATE_TRUNC(SAFE_CAST(a.feccreacion AS DATE), MONTH), INTERVAL 1 MONTH)) AS INTEGER)
				AND
				(
					(m1.canal IN ('VIDA') AND a.origen_data IN ('LOTUS','JOURNEY DIGITAL'))
					OR
					(m1.canal IN ('WORKSITE') AND a.origen_data IN ('WORKYBOT'))
				)	)
		LEFT JOIN mes_prod_data m2
			ON (CAST(m2.mes_produccion AS INTEGER)=CAST(FORMAT_DATE('%Y%m', DATE_ADD(DATE_TRUNC(SAFE_CAST(a.feccreacion AS DATE), MONTH), INTERVAL 2 MONTH)) AS INTEGER)
				AND
				(
					(m2.canal IN ('VIDA') AND a.origen_data IN ('LOTUS','JOURNEY DIGITAL'))
					OR
					(m2.canal IN ('WORKSITE') AND a.origen_data IN ('WORKYBOT'))
				)	)
	WHERE 
		1 = 1
)
SELECT
	trm.periodo_creacion,
	trm.mes_produccion,
	trm.numtramite,
	trm.origen_data,
	trm.id_cliente,
    trm.num_documento_cliente,
	trm.id_intermediario,
    trm.des_id_intermediario_origen,
	trm.tipo_tramite,
	trm.tipo_operacion,
	trm.id_producto,
	trm.glosa,
	trm.id_poliza,
	trm.id_moneda,
	trm.mnt_prima_emitida_bruta_anualizada,
	trm.tasa_cambio,
	trm.mnt_prima_emitida_bruta_anualizada_sol,
	trm.mnt_prima_emitida_bruta_anualizada_usd,
	trm.fec_solicitud,
	trm.fec_creacion,
	trm.fec_emision,
	trm.area_tramite,
	trm.id_canal,
	trm.emisor_encargado,
    trm.cod_cluster,
	trm.des_cluster,
    trm.est_estado_solicitud,
    trm.des_estado_solicitud,
    trm.id_persona_via,
    trm.id_sede_via,
	trm.fec_insercion,
	trm.fec_modificacion,
	trm.bq__soft_deleted
FROM stg_tramite trm;