
CREATE OR REPLACE TABLE `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_finrisk_tmp_vitto` AS
WITH  ESTADO_RENTA_VITALICIA AS (
select ID_COD, DESC_COD, 
CASE WHEN DESC_COD = 'Finalizada' THEN 1 
	 WHEN DESC_COD = 'Ganada Completada' THEN 2
	 WHEN DESC_COD = 'Ganada Contingencia' THEN 3
	 WHEN DESC_COD = 'Ganada Recalculada' THEN 4
	 WHEN DESC_COD = 'Ganada' THEN 5 ELSE 6 END REGLA,
FROM `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.COT_CODIGOS` g
order by 2
),
ESTADO_RENTA_GARANTIZADA AS (
select ID_COD, DESC_COD, 
CASE WHEN DESC_COD = 'Emitida' THEN 1 ELSE 2 END REGLA,
FROM `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.PRIVADA_CODIGOS` g
order by 2
),
COT_RENTA_VITALICIA AS (
select
 a.COTIZA
  ,       a.ID_COT as NROCOTIZACION
  ,       a.FECHA_COTIZACION
  ,       g.DESC_COD as ESTADO_COTIZACION
  , 	    g.REGLA
  ,       CASE WHEN a.RESULTADO = 'RSG' THEN 1 ELSE 2 END R_GANADA
  ,       h.DESC_COD as TIPO_DOCUMENTO_CLIENTE
  ,       b.NUM_DOC as NUMERO_DOCUMENTO_CLIENTE
  ,       b.APELL_PAT as APELL_PATERNO_CLIENTE
  ,       b.APELL_MAT as APELL_MATERNO_CLIENTE
  ,       b.NOMBRE1|| case when b.NOMBRE2 is null then '' else ' '||b.NOMBRE2 end as NOMBRES_CLIENTE
  ,       b.FECHA_NAC As FECHA_NACIMIENTO
  ,       b.ID_ASESOR_ASOCIADO as ID_ASESOR
  ,       usu.nombre as NOMBRES_ASESOR
  ,       usu.APELLIDO as APELLIDO_ASESOR  
  ,       Pe.Desc_Cod as PERFIL_ASESOR
  ,       usu.Codigo_Trab as CODIGO_TRAB
  ,       usu.DNI as DNI_ASESOR
  ,       usu.mail as EMAIL_ASESOR
  ,       Mo.Item_Cod as MONEDA_COTIZACION
  ,       a.FONDO_RV as PRIMA_COTIZACION
  ,       a.PENSION_MELERRV 
  ,       a.PENSION_MELERRVD 
  ,       a.PENSION_MELERRT 
  ,       a.TV  as TASA_VENTA
  ,       Gan.Num_Poliza as NUMERO_POLIZA
  ,       b.NUM_OPER as NUMERO_SOLICITUD
  ,       Prod.ITEM_COD as CODIGO_PRODUCTO
  ,       Prod.DESC_COD as NOMBRE_PRODUCTO   
from    `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.COT_COTIZACIONES`   a
inner join  `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.COT_SOL_MELER` b on a.ID_SOL_MELER = b.ID_SOL_MELER
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.COT_PROD` d  on a.id_sol_meler = d.id_sol_meler and a.num_opcion  = d.num_opcion
left join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.COT_GANADAS` Gan  on Gan.ID_COT=a.ID_COT and Gan.ID_VALIDO='S'
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.COT_CODIGOS` c on b.id_sexo      = c.id_cod
inner join ESTADO_RENTA_VITALICIA g on g.ID_COD=b.ID_ESTADO_SOLIC
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.COT_CODIGOS` h on h.ID_COD=b.ID_DOC
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.COT_USUARIOS` usu on b.ID_ASESOR_ASOCIADO=usu.ID_USUARIO
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.COT_CODIGOS` Pe on Pe.ID_COD=usu.ID_PERFIL
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.COT_CODIGOS` Mo on Mo.ID_COD=b.ID_MONEDA
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.COT_CODIGOS` Prod on Prod.ID_COD=d.ID_MODALIDAD
 where   a.ID_VIGENTE   = 'S'
)
,
 COT_RENTA_GARANTIZADA AS (
select
          a.ID_COTIZACION as NROCOTIZACION
  ,       b.OPCION as OPCION_NROCOTIZACION
  ,       a.FECHA_MAX_COTIZACION as FECHA_COTIZACION
  ,       g.DESC_COD as ESTADO_COTIZACION
  , 	    g.REGLA
  ,       1 as R_GANADA
  ,       g.DESC_COD as ESTADO_OPCION
  ,       h.DESC_COD as TIPO_DOCUMENTO_CLIENTE
  ,       a.nat_num_doc  as NUMERO_DOCUMENTO_CLIENTE -- AGREGUE 
  ,  	a.NAT_FECHANAC AS FECNACIMIENTO   --AGREGUE
  ,       a.NAT_APELL_PAT as APELL_PATERNO_CLIENTE
  ,       a.NAT_APELL_MAT as APELL_MATERNO_CLIENTE
  ,       a.NAT_NOMBRES as NOMMBRES_CLIENTE
  ,       a.ID_AGENTEVENTAS as ID_ASESOR
  ,       usu.nombre as NOMBRE_ASESOR
  ,       usu.APELLIDO as APELLIDO_ASESOR  
  ,       Pe.Desc_Cod as PERFIL_ASESOR
  ,       usu.Codigo_Trab as CODIGO_ASESOR
  ,       usu.DNI as DNI_ASESOR
  ,       usu.mail as EMAIL_ASESOR
  ,       Mo.Item_Cod as MONEDA_COTIZACION
  ,       b.PRIMA_COMERCIAL as PRIMA_COTIZACION
  ,       b.PAGO as PAGO_PENSION
  ,       b.TV  as TASA_VENTA
  ,       b.TASA_TECNICA 
  ,       pl.id_plan as CODIGO_PRODUCTO
  ,       pl.NOMBRE as NOMBRE_PRODUCTO 
  from  
`rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.PRIVADA_COTIZACION` a 
inner join (select max(vers) Max_vers_cot,ID_COTIZACION from `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.PRIVADA_COTIZACION` group by ID_COTIZACION) Ul_Cot on Ul_Cot.Max_vers_cot=a.Vers and Ul_Cot.ID_COTIZACION=a.ID_COTIZACION
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.PRIVADA_COTIZACION_OPCION` b on b.id_cotizacion=a.Id_cotizacion
inner join (select max(VERSOPCION) Max_vers_cotop,ID_COTIZACION, Opcion from `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.PRIVADA_COTIZACION_OPCION` group by ID_COTIZACION,OPCION) Ul_CotOp on Ul_CotOp.Max_vers_cotop=b.VERSOPCION and Ul_CotOp.ID_COTIZACION=b.ID_COTIZACION and Ul_CotOp.OPCION=b.OPCION
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.PRIVADA_PLAN` pl on pl.id_plan=b.id_plan and pl.vers=b.vers_plan
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.PRIVADA_CODIGOS` c on a.Aseg_id_sexo      = c.id_cod
inner join ESTADO_RENTA_GARANTIZADA  g on g.ID_COD=a.ID_ESTADO
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.PRIVADA_CODIGOS`  h on h.ID_COD=a.ASEG_TIPO_DOC
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.PRIVADA_CODIGOS`  i on i.ID_COD=b.ID_ESTADOOPCION
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.COT_USUARIOS` usu on a.ID_AGENTEVENTAS=usu.ID_USUARIO 
left join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.PRIVADA_CODIGOS`  Pe on usu.ID_PERFIL_PRIVADA = Pe.ID_COD
inner join `rs-nprd-dlk-dd-rwz-a406.bdweb__app_cotirrvv.PRIVADA_CODIGOS`  Mo on Mo.ID_COD=b.ID_MONEDAPRIMA  
where  1=1
 order by a.ID_COTIZACION desc
)
SELECT 
CAST( NROCOTIZACION	as string)	id_cotizacion_origen,
ESTADO_COTIZACION	estadocotizacion,
regla,
R_GANADA,
'N.D'			origen_lead_cliente,
CAST(ID_ASESOR	as string)	codigo_asesor,
EMAIL_ASESOR	email_asesor,
CASE WHEN LENGTH(CAST (DNI_ASESOR AS STRING)) <8 then LPAD(CAST(DNI_ASESOR AS STRING),8,'0') 
ELSE CAST (DNI_ASESOR AS STRING) END nro_doc_asesor,
'DNI'			tip_doc_asesor, --'NO EXITES'	
CASE WHEN LENGTH(NUMERO_DOCUMENTO_CLIENTE) =8 AND TIPO_DOCUMENTO_CLIENTE = 'Carnet Extranjeria' THEN 
 LPAD(NUMERO_DOCUMENTO_CLIENTE,9,'0') ELSE NUMERO_DOCUMENTO_CLIENTE END nro_doc_cliente,
cfg_tipdoc.id_homologado   tip_doc_cliente ,
'FINRISK'			origen_data,
cast(FECHA_COTIZACION AS DATE)			fecha_registro,
CODIGO_PRODUCTO			producto_recomendado,
cfg_prod.id_homologado	codproducto_final, 	
CAST(cfg_estcot.id_homologado AS INT64) 	    id_estado_cotizacion, 
cfg_estcot.descrip_homologado  des_estado_cotizacion,
CAST(NULL AS STRING)			semaforo_precotizacion,
CAST(NULL AS STRING)			semaforo_cotizacion,
CAST(NULL AS STRING)  		as id_asesoria_origen,
CAST(NULL AS INT64)  			as id_estado_asesoria,
CAST(NULL AS STRING)  		as des_estado_asesoria,
cfg_moneda.id_homologado  des_moneda_presentada,
SAFE_CAST(NULL AS STRING) AS frecuencia_pago,
SAFE_CAST(NULL AS STRING) AS cod_prod,
SAFE_CAST(NULL AS INT64) AS id_pago,
SAFE_CAST(NULL AS FLOAT64) AS importepp,
SAFE_CAST(NULL AS FLOAT64) AS importep,
SAFE_CAST(NULL AS FLOAT64) AS importe,
SAFE_CAST(NULL AS FLOAT64) AS monto_pago,
SAFE_CAST(NULL AS FLOAT64) AS prima_ahorro,
SAFE_CAST(NULL AS FLOAT64) AS monto_descuento,
SAFE_CAST(NULL AS FLOAT64) AS importe_cot,
CAST (PRIMA_COTIZACION	AS NUMERIC)		prima_anual_usd,
'0'			pago_flg,
CAST(NULL AS DATE) AS 			fec_crea_pago,
CAST(FECHA_COTIZACION AS DATE)			fec_actualizacion_estado_asesoria,
CAST(FECHA_COTIZACION as DATETIME) 	fechora_actualizacion_estado_asesoria,
DATE_TRUNC(CAST(FECHA_COTIZACION AS DATE), MONTH)  	periodo,
'FUERZA VENTA' as canal,
CAST(numero_poliza AS STRING)as numpol,
TASA_VENTA as tasa_venta
FROM COT_RENTA_VITALICIA rv
left join   `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_tipdoc
on (rv.TIPO_DOCUMENTO_CLIENTE  = cfg_tipdoc.CODIGO_ORIGEN
and  cfg_tipdoc.id_origen='COT_FINRISK' and cfg_tipdoc.tipo_parametro='TIPDOCUMENTO')
left join   `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_prod
on (rv.CODIGO_PRODUCTO  = cfg_prod.CODIGO_ORIGEN
and  cfg_prod.id_origen='COT_FINRISK' and cfg_prod.tipo_parametro='CODPROD_RENTA_VITALICIA')
left join   `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_estcot
on (rv.ESTADO_COTIZACION  = cfg_estcot.CODIGO_ORIGEN
and  cfg_estcot.id_origen='COT_FINRISK' and cfg_estcot.tipo_parametro='STSCOTI_RENTA_VITALICIA')
left join   `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_moneda
on (rv.MONEDA_COTIZACION  = cfg_moneda.CODIGO_ORIGEN
and  cfg_moneda.id_origen='COT_FINRISK' and cfg_moneda.tipo_parametro='MONCOTI')
union all
SELECT 
CAST(NROCOTIZACION	as string)	id_cotizacion_origen,
ESTADO_COTIZACION	estadocotizacion,
regla,
R_GANADA,
'N.D'			origen_lead_cliente,
CAST(ID_ASESOR as string)		codigo_asesor,
EMAIL_ASESOR	email_asesor,
CASE WHEN LENGTH(CAST (DNI_ASESOR AS STRING)) <8 then LPAD(CAST(DNI_ASESOR AS STRING),8,'0') 
ELSE CAST (DNI_ASESOR AS STRING) END nro_doc_asesor,
'DNI'			tip_doc_asesor,   --'NO EXITES'	
CASE  WHEN LENGTH( CAST(NUMERO_DOCUMENTO_CLIENTE AS STRING)) < 8 AND TIPO_DOCUMENTO_CLIENTE = 'Carnet Extranjeria' 
THEN  LPAD(CAST(NUMERO_DOCUMENTO_CLIENTE AS STRING),9,'0')
WHEN LENGTH( CAST(NUMERO_DOCUMENTO_CLIENTE AS STRING)) < 8 AND TIPO_DOCUMENTO_CLIENTE = 'DNI' THEN 
LPAD(CAST(NUMERO_DOCUMENTO_CLIENTE AS STRING),8,'0')
ELSE  CAST(NUMERO_DOCUMENTO_CLIENTE AS STRING) END nro_doc_cliente,
cfg_tipdoc.id_homologado tip_doc_cliente,
'FINRISK'			origen_data,
cast(FECHA_COTIZACION AS DATE)			fecha_registro,
cast(CODIGO_PRODUCTO	AS STRING)		producto_recomendado,
cfg_prod.id_homologado	codproducto_final,	
CAST(cfg_estcot.id_homologado AS INT64) 	id_estado_cotizacion, 
cfg_estcot.descrip_homologado  des_estado_cotizacion,		
CAST(NULL AS STRING)			semaforo_precotizacion,
CAST(NULL AS STRING)			semaforo_cotizacion,
CAST(NULL AS STRING)  		as id_asesoria_origen,
CAST(NULL AS INT64)  			as id_estado_asesoria,
CAST(NULL AS STRING)  		as des_estado_asesoria,
cfg_moneda.id_homologado  des_moneda_presentada,
SAFE_CAST(NULL AS STRING) AS frecuencia_pago,
SAFE_CAST(NULL AS STRING) AS cod_prod,
SAFE_CAST(NULL AS INT64) AS id_pago,
SAFE_CAST(NULL AS FLOAT64) AS importepp,
SAFE_CAST(NULL AS FLOAT64) AS importep,
SAFE_CAST(NULL AS FLOAT64) AS importe,
SAFE_CAST(NULL AS FLOAT64) AS monto_pago,
SAFE_CAST(NULL AS FLOAT64) AS prima_ahorro,
SAFE_CAST(NULL AS FLOAT64) AS monto_descuento,
SAFE_CAST(NULL AS FLOAT64) AS importe_cot,
cast(PRIMA_COTIZACION AS NUMERIC)	prima_anual_usd,
'0'			pago_flg,
CAST(NULL AS DATE) AS 			fec_crea_pago,
CAST(FECHA_COTIZACION AS DATE)			fec_actualizacion_estado_asesoria,
CAST(FECHA_COTIZACION as DATETIME) 			fechora_actualizacion_estado_asesoria,
DATE_TRUNC(CAST(FECHA_COTIZACION AS DATE), MONTH)  	periodo,
'FUERZA VENTA' as canal,
SAFE_CAST(NULL AS STRING) AS numpol,
TASA_VENTA as tasa_venta
FROM COT_RENTA_GARANTIZADA rg
left join   `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_tipdoc
on (rg.TIPO_DOCUMENTO_CLIENTE  = cfg_tipdoc.CODIGO_ORIGEN
and  cfg_tipdoc.id_origen='COT_FINRISK' and cfg_tipdoc.tipo_parametro='TIPDOCUMENTO')
left join   `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_prod
on ( cast(rg.CODIGO_PRODUCTO as string)  = cfg_prod.CODIGO_ORIGEN
and  cfg_prod.id_origen='COT_FINRISK' and cfg_prod.tipo_parametro='CODPROD_RENTA_GARANTIZADA')
left join   `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_estcot
on (rg.ESTADO_COTIZACION  = cfg_estcot.CODIGO_ORIGEN
and  cfg_estcot.id_origen='COT_FINRISK' and cfg_estcot.tipo_parametro='STSCOTI_RENTA_GARANTIZADA')
left join   `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_moneda
on (rg.MONEDA_COTIZACION  = cfg_moneda.CODIGO_ORIGEN
and  cfg_moneda.id_origen='COT_FINRISK' and cfg_moneda.tipo_parametro='MONCOTI')