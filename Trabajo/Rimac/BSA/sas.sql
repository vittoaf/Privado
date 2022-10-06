CREATE OR REPLACE TABLE `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_group_by` AS --2,860,887 25,591,790
WITH COTIZACION AS (
SELECT *
FROM `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_acuerdo.ACU_ACUERDO`
WHERE idptipoacuseg='COT'
)
,ROL AS
(
	SELECT IDEROL,r.dscrol as rol,r.DESCRIPCIONAE,
	CASE WHEN r.DESCRIPCIONAE IS NULL THEN r.dscrol ELSE r.DESCRIPCIONAE end dscrol
from `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_ROL` R
)
, CERTIFICADO as (
SELECT 
	ideprod,
	idemaestro,
	idecotizacion,
	idecontratante,  
	idevendedor,
	stsacuerdo,
	refexterna,
	numero,
	max(ideacuerdo)ideacuerdo
FROM `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_acuerdo.ACU_ACUERDO`
WHERE idptipoacuseg='CER'
AND idecotizacion is not null
AND stsacuerdo != 1
GROUP BY 
	ideprod, 
	idemaestro,
	idecotizacion,
	idecontratante,  
	idevendedor,
	stsacuerdo,
	refexterna,
	numero
),AJUSTE_COT AS (
SELECT 
	CAST(numpol AS STRING)numpol,
	CAST(numerodoc_asesor AS STRING) numerodoc_asesor
FROM 
`rs-nprd-dlk-dd-rwz-a406.de__canales.bsa_base_ajustes_sas`
GROUP BY 
	CAST(numpol AS STRING),
	CAST(numerodoc_asesor AS STRING)
)
,TER_TITULAR AS ( 
	SELECT 
	A.ideacuerdo,          
	TD.numerodoc,
	C.abreviatura AS dsc_tipodoc
FROM `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_acuerdo.ACU_ROLACUERDO` A
INNER JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_ROLTERCERO` Y ON (A.iderolter = Y.iderolter)
INNER JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_TERCERO`    T ON (T.idetercero = Y.idetercero)
INNER JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_DOCTERCERO` TD ON (TD.idetercero = T.idetercero AND TD.idedoctercero = T.idedocterprincipal)
LEFT JOIN  `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_comunes.CFG_PARAMETRO` C ON ((C.idetippar = 'TER_TIPODOCUMENTO') AND C.codigoc = CAST(TD.idptipodocumento AS STRING))
WHERE Y.iderol = 10 --ROL TITULAR
AND A.stsrolacuerdo = 'ACT'
GROUP BY 
	A.ideacuerdo,
	TD.numerodoc,
	C.abreviatura
)
, POLIZA AS (
SELECT 
	SPLIT(numero, '|')[SAFE_OFFSET(0)] codproducto,
	SPLIT(numero, '|')[SAFE_OFFSET(1)] numpol,
	*
FROM `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_acuerdo.ACU_ACUERDO` a
WHERE idptipoacuseg='POL'
)
,TMP0 as(
SELECT 
	case when pol.numpol is not null then DATE_TRUNC(CAST(pol.fecemision AS DATE), MONTH) else DATE_TRUNC(CAST(a.feccreacion AS DATE), MONTH) end periodo,
	a.fecsts,
	a.ideacuerdo,
	a.ideprod,
	a.idecontratante,  
	a.idecorredor,
	a.idevendedor,  
	a.idpmonacuerdo,
	pol.numpol,
	pol.codproducto,
	pol.fecini,
	pol.fecfinplan,
	pol.fecrenovacion,
	pol.fecemision,
	pol.stsacuerdo ,
	c.dscrol as canal,
	d.nomcompleto as nombre_tercero,
	d.numerodoc as numerodoc_tercero,
	d.idptipodocumento as tipodoc_tercero,
	d.codexterno,
	PM.email,
	a.feccreacion,
	cer.numero,
	cer.ideacuerdo as ideacuerdo_cer,
	count(distinct a.ideacuerdo)cotizaciones 
FROM COTIZACION a
LEFT JOIN certificado cer on a.ideacuerdo=cer.idecotizacion
LEFT JOIN poliza pol on cer.idemaestro= pol.ideacuerdo
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_ROLTERCERO` b on a.idecanal=b.iderolter
INNER JOIN ROL c on b.iderol=c.iderol
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_TERCERO` d on b.idetercero=d.idetercero
LEFT JOIN  `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.PTO_MEDIOCONTACTO` PM ON (PM.IDEPUNTOCONTACTO = d.IDEMAILPRINCIPAL)
GROUP BY
	a.fecsts,
	a.ideacuerdo,
	a.ideprod,
	a.idecontratante,
	a.idecorredor,
	a.idevendedor,
	a.idpmonacuerdo,
	pol.numpol,
	pol.codproducto,
	pol.fecini,
	pol.fecfinplan,
	pol.fecrenovacion,
	pol.fecemision,
	pol.stsacuerdo,
	c.dscrol,
	d.nomcompleto,
	d.numerodoc,
	d.idptipodocumento,
	d.codexterno,
	PM.email,
	a.feccreacion,
	cer.numero,
	cer.ideacuerdo
)
, PRIMA AS (
SELECT 
	ideacuerdo,
	ARRAY_AGG(ideplan order by feccreacion desc)[OFFSET(0)] ideplan,
	ARRAY_AGG(primaneta order by feccreacion desc)[OFFSET(0)] primaneta,
	ARRAY_AGG(primabruta order by feccreacion desc)[OFFSET(0)] primabruta,
	ARRAY_AGG(idpmoneda order by feccreacion desc)[OFFSET(0)] idpmoneda
FROM `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_acuerdo.ACU_INSPLAN`
WHERE ideacuerdo not in(
SELECT 
	ideacuerdo 
FROM `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_acuerdo.ACU_INSPLAN`
WHERE indactivo='S')
GROUP BY 
	ideacuerdo
UNION ALL
SELECT
	ideacuerdo,
	ARRAY_AGG(ideplan order by FECMODIF desc)[OFFSET(0)] ideplan,
	ARRAY_AGG(primaneta order by FECMODIF desc)[OFFSET(0)] primaneta,
	ARRAY_AGG(primabruta order by FECMODIF desc)[OFFSET(0)] primabruta,
	ARRAY_AGG(idpmoneda order by FECMODIF desc)[OFFSET(0)] idpmoneda
FROM `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_acuerdo.ACU_INSPLAN`
WHERE ideacuerdo IN(
SELECT 
	ideacuerdo 
FROM `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_acuerdo.ACU_INSPLAN`
WHERE indactivo='S')
AND indactivo='S'
GROUP BY ideacuerdo
)
,FINAL as(
SELECT 
	a.*,
	b.ideplan,
	b.primaneta,
	b.primabruta,
	b.idpmoneda,
FROM tmp0 a 
LEFT JOIN prima b
ON a.ideacuerdo_cer=b.ideacuerdo
) 
,FINAL_COT AS (
SELECT 
	CONCAT(CAST(a.ideacuerdo AS STRING), IF(IFNULL(a.numero,'') = '','','-'||a.numero))	as id_cotizacion_origen,
	CAST(NULL AS STRING) 										as	origen_lead_cliente,
	CAST(a.codexterno AS STRING) 								as codigo_asesor,
	a.email 													as email_asesor,
	CASE 
		WHEN AC.NUMPOL IS NOT NULL THEN AC.numerodoc_asesor 
			ELSE c.numerodoc 
		END  												    as nro_doc_asesor,
	CASE
		WHEN AC.NUMPOL IS NOT NULL THEN 'DNI'
			ELSE TRIM(TDOC.abreviatura)
		END  			 										as tip_doc_asesor,
	TIT.numerodoc 												as nro_doc_cliente,
	TRIM(TIT.dsc_tipodoc) 										as tip_doc_cliente,
	CAST('SAS' AS STRING) 										as origen_data,
	CAST(SUBSTR(CAST(A.feccreacion AS STRING), 1, 10) AS DATE)  as fecha_registro,
	d.dscproducto 												as producto_recomendado,
	d.refexterna 												as codproducto_final,
	a.stsacuerdo 												as id_estado_cotizacion,
	cfg_prm_acu.descripcion 									as des_estado_cotizacion,
	CAST(NULL AS STRING)  										as semaforo_precotizacion,
	CAST(NULL AS STRING)  										as semaforo_cotizacion,
	CAST(NULL AS STRING)  										as id_asesoria_origen,
	CAST(NULL AS INT64)  										as id_estado_asesoria,
	CAST(NULL AS STRING)  										as des_estado_asesoria,
	CASE
		WHEN a.idpmonacuerdo = 'SOL' THEN 'PEN'
			ELSE TRIM(a.idpmonacuerdo)
		END 													as des_moneda_presentada,
	F.idpfrecuencia 											as frecuencia_pago,
	d.refexterna 												as cod_prod,
	CAST(NULL AS INT64)  										as id_pago,
	CAST(NULL AS FLOAT64)  										as importepp,
	CAST(NULL AS FLOAT64)  										as importep,
	CAST(NULL AS FLOAT64)  										as importe,
	CAST(NULL AS FLOAT64)  										as monto_pago,
	CAST(NULL AS FLOAT64)  										as prima_ahorro,
	CAST(NULL AS FLOAT64)  										as monto_descuento,
	CAST(a.primaneta AS NUMERIC)								as importe_cot,
	CAST(a.primabruta AS NUMERIC)								as prima_anual_usd,
	'0' 														AS pago_flg,
	CAST(NULL AS DATE)	 										as fec_crea_pago,
	CAST(SUBSTR(CAST(A.fecsts AS STRING), 1, 10) AS DATE) 		as fec_actualizacion_estado_asesoria,
	cast(A.fecsts as DATETIME) 									as fechora_actualizacion_estado_asesoria,
	a.periodo,
	cfg_origenCanal.ID_HOMOLOGADO 								as canal,
	a.numpol 													as numpol
FROM FINAL a
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_ROLTERCERO` b on a.idevendedor=b.iderolter
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_TERCERO` c on b.idetercero=c.idetercero
LEFT JOIN  `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.PTO_MEDIOCONTACTO` PM ON (PM.IDEPUNTOCONTACTO = c.IDEMAILPRINCIPAL)
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_producto.PRO_PRODUCTO` d on a.ideprod=d.ideprod
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_DOCTERCERO` TD ON (TD.idetercero = c.idetercero AND TD.idedoctercero = c.idedocterprincipal)
LEFT JOIN  `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_comunes.CFG_PARAMETRO` TDOC ON (TDOC.idetippar = 'TER_TIPODOCUMENTO' AND TRIM(TDOC.codigoc) = CAST(TD.idptipodocumento AS STRING))
LEFT JOIN TER_TITULAR       TIT ON (A.ideacuerdo = TIT.ideacuerdo) 
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_comunes.CFG_PARAMETRO` cfg_prm_acu	ON (cfg_prm_acu.idetippar = 'ACU_STSACUERDO_COTIZACION' AND cfg_prm_acu.codigon = A.stsacuerdo	)
LEFT JOIN (
			SELECT 
				IDEACUERDO,
				ARRAY_AGG(idpfrecuencia ORDER BY NUMOPER DESC)[OFFSET(0)] AS idpfrecuencia
  			FROM `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_finanzas.PRV_PLANIFICADORFINANCIERO`
			GROUP BY 
				IDEACUERDO
			) F ON A.ideacuerdo = F.IDEACUERDO
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_origenCanal
	ON (
			cfg_origenCanal.ID_ORIGEN='COTSAS' 
			AND cfg_origenCanal.TIPO_PARAMETRO='TIPOCANAL' 
			AND cfg_origenCanal.CODIGO_ORIGEN=a.canal
		)
LEFT JOIN AJUSTE_COT AC ON AC.NUMPOL = A.NUMPOL
),
CERTIFICADO_DATA as (
SELECT 
	ideprod,
	idecanal,
	idemaestro,
	idecotizacion,
	idecontratante,  
	idevendedor,
	stsacuerdo,
	refexterna,
	feccreacion,
	numero,
	max(ideacuerdo)ideacuerdo
FROM `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_acuerdo.ACU_ACUERDO`
WHERE idptipoacuseg='CER'
AND idecotizacion is null
AND stsacuerdo != 1
GROUP BY 
	ideprod, 
	idecanal,
	idemaestro,
	idecotizacion,
	idecontratante,  
	idevendedor,
	stsacuerdo,
	refexterna,
	feccreacion,
	numero
)
,TMP0_CERT as(
SELECT 
	case when pol.numpol is not null then DATE_TRUNC(CAST(pol.fecemision AS DATE), MONTH) else DATE_TRUNC(CAST(cer.feccreacion AS DATE), MONTH) end periodo,
	cer.ideacuerdo,
	cer.ideprod,
	cer.idecontratante,  
	pol.idecorredor,
	pol.idevendedor,  
	pol.idpmonacuerdo,
	pol.numpol,
	pol.codproducto,
	pol.fecini,
	pol.fecfinplan,
	pol.fecrenovacion,
	pol.fecemision,
	pol.stsacuerdo, 
	cer.stsacuerdo as stsacuerdo_cer, 
	cer.idecontratante as idecontratante_cer, 
	cer.idevendedor as idevendedor_cer,
	cer.ideacuerdo as ideacuerdo_cer,
	cer.refexterna as refexterna_cer,
	pol.fecsts as dia,
	c.dscrol as canal,
	d.nomcompleto as nombre_tercero,
	d.numerodoc as numerodoc_tercero,
	d.idptipodocumento as tipodoc_tercero,
	d.codexterno,
	PM.email,
	cer.feccreacion,
	pol.fecsts,
	cer.numero,
	count(distinct cer.ideacuerdo)cotizaciones 
FROM certificado_data cer
LEFT JOIN poliza pol on cer.idemaestro= pol.ideacuerdo
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_ROLTERCERO` b on cer.idecanal=b.iderolter
INNER JOIN ROL c on b.iderol=c.iderol
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_TERCERO` d on b.idetercero=d.idetercero
LEFT JOIN  `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.PTO_MEDIOCONTACTO` PM ON (PM.IDEPUNTOCONTACTO = d.IDEMAILPRINCIPAL)
GROUP BY 
	cer.ideacuerdo,
	cer.ideprod,
	cer.idecontratante,  
	pol.idecorredor,
	pol.idevendedor,  
	pol.idpmonacuerdo,
	pol.numpol,
	pol.codproducto,
	pol.fecini,
	pol.fecfinplan,
	pol.fecrenovacion,
	pol.fecemision,
	pol.stsacuerdo,
	cer.stsacuerdo,
	cer.idecontratante,
	cer.idevendedor,
	cer.ideacuerdo,
	cer.refexterna,
	pol.fecsts,
	c.dscrol,
	d.nomcompleto,
	d.numerodoc,
	d.idptipodocumento,
	d.codexterno,
	PM.email,
	cer.feccreacion,
	pol.fecsts,
	cer.numero
)
,PREVIA AS(
SELECT 
	a.*,
	b.ideplan,
	b.primaneta,
	b.primabruta,
	b.idpmoneda,
FROM tmp0_cert a 
LEFT JOIN prima b
ON  a.ideacuerdo_cer=b.ideacuerdo
)
,FINAL_CERT AS(
	SELECT 
	CONCAT(CAST(a.ideacuerdo AS STRING), IF(IFNULL(a.numero,'') = '','','-'||a.numero))	as id_cotizacion_origen,
	CAST(NULL AS STRING) 										as	origen_lead_cliente,
	CAST(a.codexterno AS STRING) 								as codigo_asesor,
	a.email 													as email_asesor,
		CASE 
		WHEN AC.NUMPOL IS NOT NULL THEN AC.numerodoc_asesor 
			ELSE c.numerodoc 
		END  												    as nro_doc_asesor,
		CASE
		WHEN AC.NUMPOL IS NOT NULL THEN 'DNI'
			ELSE TRIM(TDOC.abreviatura)
		END  			 										as tip_doc_asesor,
	TIT.numerodoc 												as nro_doc_cliente,
	TRIM(TIT.dsc_tipodoc) 										as tip_doc_cliente,
	CAST('SAS' AS STRING) 										as origen_data,
	CAST(SUBSTR(CAST(A.feccreacion AS STRING), 1, 10) AS DATE)  as fecha_registro,
	d.dscproducto 												as producto_recomendado,
	d.refexterna 												as codproducto_final,
	a.stsacuerdo 												as id_estado_cotizacion,
	cfg_prm_acu.descripcion 									as des_estado_cotizacion,
	CAST(NULL AS STRING)  										as semaforo_precotizacion,
	CAST(NULL AS STRING)  										as semaforo_cotizacion,
	CAST(NULL AS STRING)  										as id_asesoria_origen,
	CAST(NULL AS INT64)  										as id_estado_asesoria,
	CAST(NULL AS STRING)  										as des_estado_asesoria,
	CASE
		WHEN a.idpmonacuerdo = 'SOL' THEN 'PEN'
			ELSE TRIM(a.idpmonacuerdo)
		END 													as des_moneda_presentada,
	F.idpfrecuencia 											as frecuencia_pago,
	d.refexterna 												as cod_prod,
	CAST(NULL AS INT64)  										as id_pago,
	CAST(NULL AS FLOAT64)  										as importepp,
	CAST(NULL AS FLOAT64)  										as importep,
	CAST(NULL AS FLOAT64)  										as importe,
	CAST(NULL AS FLOAT64)  										as monto_pago,
	CAST(NULL AS FLOAT64)  										as prima_ahorro,
	CAST(NULL AS FLOAT64)  										as monto_descuento,
	CAST(a.primaneta AS NUMERIC)								as importe_cot,
	CAST(a.primabruta AS NUMERIC) 								as prima_anual_usd,
	'0' 														AS pago_flg,
	CAST(NULL AS DATE)	 										as fec_crea_pago,
	CAST(SUBSTR(CAST(A.fecsts AS STRING), 1, 10) AS DATE) 		as fec_actualizacion_estado_asesoria,
	cast(A.fecsts as DATETIME) 									as fechora_actualizacion_estado_asesoria,
	a.periodo,
	cfg_origenCanal.ID_HOMOLOGADO 								as canal,
	a.numpol 													as numpol,
	a.refexterna_cer
FROM previa a
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_ROLTERCERO` b on a.idevendedor_cer=b.iderolter
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_TERCERO` c on b.idetercero=c.idetercero
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.PTO_MEDIOCONTACTO` PM ON (PM.IDEPUNTOCONTACTO = c.IDEMAILPRINCIPAL)
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_producto.PRO_PRODUCTO` d on a.ideprod=d.ideprod
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_tercero.TER_DOCTERCERO` TD ON (TD.idetercero = c.idetercero AND TD.idedoctercero = c.idedocterprincipal)
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_comunes.CFG_PARAMETRO` TDOC ON (TDOC.idetippar = 'TER_TIPODOCUMENTO' AND TRIM(TDOC.codigoc) = CAST(TD.idptipodocumento AS STRING))
LEFT JOIN TER_TITULAR       TIT ON (A.ideacuerdo = TIT.ideacuerdo) 
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_comunes.CFG_PARAMETRO` cfg_prm_acu	ON (cfg_prm_acu.idetippar = 'ACU_STSACUERDO_CERTIFICADO' AND cfg_prm_acu.codigon = A.stsacuerdo	)
LEFT JOIN (
			SELECT 
				IDEACUERDO,
				ARRAY_AGG(idpfrecuencia ORDER BY NUMOPER DESC)[OFFSET(0)] AS idpfrecuencia
  			FROM `rs-nprd-dlk-dd-rwz-a406.bdsas__app_iaa_finanzas.PRV_PLANIFICADORFINANCIERO`
			GROUP BY 
				IDEACUERDO
			) F ON A.ideacuerdo = F.IDEACUERDO
LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_origenCanal
	ON (
			cfg_origenCanal.ID_ORIGEN='COTSAS' 
			AND cfg_origenCanal.TIPO_PARAMETRO='TIPOCANAL' 
			AND cfg_origenCanal.CODIGO_ORIGEN=a.canal
		)
LEFT JOIN AJUSTE_COT AC ON AC.NUMPOL = A.NUMPOL
),
sas as (
SELECT
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,
	a.prima_anual_usd,
	a.pago_flg,
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol
	--,'COT' as origen
FROM final_cot a
WHERE PERIODO >= '2021-01-01'
GROUP BY
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,
	a.prima_anual_usd,
	a.pago_flg,
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol

UNION ALL

SELECT 
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,						
	a.prima_anual_usd,
	a.pago_flg,				
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol
	--,'CERT' as origen
FROM final_cert a
LEFT JOIN final_cot c ON (trim(a.numpol)=trim(c.numpol))
WHERE c.numpol IS NULL
AND a.cod_prod in('2001','2101')
AND a.numpol is not null
AND a.refexterna_cer is not null
AND a.PERIODO >= '2021-01-01'
GROUP BY
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,						
	a.prima_anual_usd,
	a.pago_flg,				
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol),
sas_id_polizas as (
Select 	
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,
	a.prima_anual_usd,
	a.pago_flg,
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol,
	po.id_poliza,
	count(a.numpol) over(partition by a.numpol) cant_numpol,
	SPLIT(a.id_cotizacion_origen,'|')[ORDINAL(ARRAY_LENGTH(SPLIT(a.id_cotizacion_origen,'|')))]  num_certificado
from sas a left join (
				select 
				cod_producto_origen,
				num_poliza,
				MAX(ind_poliza_vigente||id_poliza)id_poliza_concat,
				RIGHT(MAX(ind_poliza_vigente||id_poliza),LENGTH(MAX(ind_poliza_vigente||id_poliza))-1) id_poliza,
				from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` p
				where cod_producto_origen = '2101'
				GROUP BY cod_producto_origen,num_poliza
) po 
ON po.num_poliza = a.NUMPOL 
AND po.cod_producto_origen = a.codproducto_final 
group by 
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,
	a.prima_anual_usd,
	a.pago_flg,
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol,
	po.id_poliza),
sas_id_certificado_caso1 as (
Select 
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,
	a.prima_anual_usd,
	a.pago_flg,
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol,
	a.id_poliza,
	ARRAY_AGG(IFNULL(ce.id_certificado,'')) id_certificado
FROM sas_id_polizas a
LEFT JOIN  `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.certificado` ce  ON a.id_poliza = ce.id_poliza
where a.codproducto_final = '2101' and a.numpol is not null and a.cant_numpol = 1
group by 
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,
	a.prima_anual_usd,
	a.pago_flg,
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol,
	a.id_poliza),
sas_id_certificado_caso2 as (
Select
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,
	a.prima_anual_usd,
	a.pago_flg,
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol,
	a.id_poliza,
	ARRAY_AGG(IFNULL(ce.id_certificado,'')) id_certificado 
FROM sas_id_polizas a
LEFT JOIN  `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.certificado` ce  ON a.id_poliza = ce.id_poliza AND ce.id_certificado_origen = a.num_certificado
where a.codproducto_final = '2101' and a.numpol is not null and a.cant_numpol > 1
group by 
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,
	a.prima_anual_usd,
	a.pago_flg,
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol,
	a.id_poliza
)
Select * from sas_id_certificado_caso1
UNION ALL
Select * from sas_id_certificado_caso2
UNION ALL
Select 
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,
	a.prima_anual_usd,
	a.pago_flg,
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol,
	a.id_poliza,
	NULL id_certificado
FROM sas_id_polizas a
WHERE 
	(CASE 
		WHEN a.codproducto_final <> '2101' THEN 1
		WHEN a.codproducto_final = '2101' and a.numpol is null THEN 1
		ELSE 0 END ) = 1

/*
Select
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,
	a.prima_anual_usd,
	a.pago_flg,
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol,
	a.id_poliza,
	ARRAY_AGG(IFNULL(ce.id_certificado,'')) id_certificado
from sas_id_polizas a
LEFT JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.certificado` ce 
ON 
				IF(a.cant_numpol = 1, 
					a.id_poliza = ce.id_poliza,
					a.id_poliza = ce.id_poliza 
					AND ce.id_certificado_origen = a.num_certificado
					)
Group by
	a.id_cotizacion_origen,		
	a.origen_lead_cliente,				
	a.codigo_asesor,					
	a.email_asesor,					
	a.nro_doc_asesor,					
	a.tip_doc_asesor,					
	a.nro_doc_cliente,					
	a.tip_doc_cliente,					
	a.origen_data,						
	a.fecha_registro,				
	a.producto_recomendado,			
	a.codproducto_final,				
	a.id_estado_cotizacion,			
	a.des_estado_cotizacion,			
	a.semaforo_precotizacion,			
	a.semaforo_cotizacion,				
	a.id_asesoria_origen,				
	a.id_estado_asesoria,				
	a.des_estado_asesoria,				
	a.des_moneda_presentada,			
	a.frecuencia_pago,					
	a.cod_prod,						
	a.id_pago,
	a.importepp,
	a.importep,
	a.importe,							
	a.monto_pago,					
	a.prima_ahorro,
	a.monto_descuento,
	a.importe_cot,
	a.prima_anual_usd,
	a.pago_flg,
	a.fec_crea_pago,
	a.fec_actualizacion_estado_asesoria,
	a.fechora_actualizacion_estado_asesoria,
	a.periodo,	
	a.canal,   
	a.numpol,
	a.id_poliza*/