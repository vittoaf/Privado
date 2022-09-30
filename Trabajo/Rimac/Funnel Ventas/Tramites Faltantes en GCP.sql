WITH
Persona_data as (
  SELECT 
		a.id_persona, 
		b.tip_documento, 
		b.num_documento, 
		a.nom_completo
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		CROSS JOIN UNNEST (a.documento_identidad) b
),
 AWS AS (
SELECT 
NRO_TRAMITE,NRO_POLIZA_FINAL,COD_PRODUCTO_FINAL,NRO_DOC_CONTRATANTE_FINAL,CONTRATANTE_FINAL,p.id_persona,p.tip_documento
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.aws_bd_produccion` aws left join persona_data p on aws.NRO_DOC_CONTRATANTE_FINAL = p.num_documento
WHERE MES_PRODUCCION = '202208'
), GCP AS (
SELECT IFNULL(trm_id_tramite,'VACIO') trm_id_tramite,trm_id_contratante,trm_id_producto,nombre_completo_contratante
 FROM `rs-nprd-dlk-dt-anlyt-msrl-1570.anl_comercial.funnel_ventas` 
WHERE trm_mes_produccion = "2022-08-01"
and bit_des_subcanal = "FFVV VIDA" and bit_categoria <> "SIN DATOS"
  ),
Cruce as (
SELECT AWS.*,gcp.nombre_completo_contratante ,
CASE
WHEN AWS.NRO_TRAMITE IS NULL AND GCP.trm_id_tramite IS NOT NULL THEN 'GCP'
WHEN GCP.trm_id_tramite IS NULL AND AWS.NRO_TRAMITE IS NOT NULL THEN 'AWS'
ELSE 'AMBOS'
END full_join,
CASE
WHEN AWS.NRO_TRAMITE IS NULL AND GCP.trm_id_tramite IS NOT NULL THEN IF(GCP.trm_id_tramite = 'VACIO','VACIO','TIENE NRO TRAMITE')
WHEN GCP.trm_id_tramite IS NULL AND AWS.NRO_TRAMITE IS NOT NULL THEN IF(AWS.NRO_TRAMITE = 'PENDIENTE','PENDIENTE','TIENE NRO TRAMITE')
ELSE 'AMBOS'
END sub_full_join
FROM AWS FULL JOIN GCP ON AWS.NRO_TRAMITE=GCP.trm_id_tramite)

Select * ,
(Select GCP.nombre_completo_contratante from GCP WHERE (gcp.nombre_completo_contratante=CONTRATANTE_FINAL) AND GCP.trm_id_producto= 'AX-'||COD_PRODUCTO_FINAL) Por_Nombre_y_producto_GCP,
(Select GCP.trm_id_tramite from GCP WHERE ( GCP.trm_id_contratante= id_persona) AND GCP.trm_id_producto= 'AX-'||COD_PRODUCTO_FINAL) Por_id_contratante_y_producto_GCP,

SUM((Select COUNT(1) from GCP WHERE (gcp.nombre_completo_contratante=CONTRATANTE_FINAL) AND GCP.trm_id_producto= 'AX-'||COD_PRODUCTO_FINAL)) OVER(),
SUM((Select COUNT(1) from GCP WHERE (GCP.trm_id_contratante= id_persona) AND GCP.trm_id_producto= 'AX-'||COD_PRODUCTO_FINAL)) OVER()
 from cruce where sub_full_join = 'TIENE NRO TRAMITE' AND full_join = 'AWS'
 order by 12,11

 ------------------

 WITH
Persona_data as (
  SELECT 
		a.id_persona, 
		--b.tip_documento, 
		b.num_documento, 
		a.nom_completo
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		CROSS JOIN UNNEST (a.documento_identidad) b
),
 AWS AS (
SELECT 
NRO_TRAMITE,NRO_POLIZA_FINAL,COD_PRODUCTO_FINAL,NRO_DOC_CONTRATANTE_FINAL,CONTRATANTE_FINAL,p.id_persona
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.aws_bd_produccion` aws left join persona_data p on aws.NRO_DOC_CONTRATANTE_FINAL = p.num_documento
WHERE MES_PRODUCCION = '202208'
), GCP AS (
SELECT IFNULL(trm_id_tramite,'VACIO') trm_id_tramite,trm_id_contratante,trm_id_producto,nombre_completo_contratante
 FROM `rs-nprd-dlk-dt-anlyt-msrl-1570.anl_comercial.funnel_ventas` 
WHERE trm_mes_produccion = "2022-08-01"
and bit_des_subcanal = "FFVV VIDA" and bit_categoria <> "SIN DATOS"
  ),
Cruce as (
SELECT AWS.* ,
CASE
WHEN AWS.NRO_TRAMITE IS NULL AND GCP.trm_id_tramite IS NOT NULL THEN 'GCP'
WHEN GCP.trm_id_tramite IS NULL AND AWS.NRO_TRAMITE IS NOT NULL THEN 'AWS'
ELSE 'AMBOS'
END full_join,
CASE
WHEN AWS.NRO_TRAMITE IS NULL AND GCP.trm_id_tramite IS NOT NULL THEN IF(GCP.trm_id_tramite = 'VACIO','VACIO','TIENE NRO TRAMITE')
WHEN GCP.trm_id_tramite IS NULL AND AWS.NRO_TRAMITE IS NOT NULL THEN IF(AWS.NRO_TRAMITE = 'PENDIENTE','PENDIENTE','TIENE NRO TRAMITE')
ELSE 'AMBOS'
END sub_full_join
FROM AWS FULL JOIN GCP ON AWS.NRO_TRAMITE=GCP.trm_id_tramite)

Select * 
 from cruce where sub_full_join = 'PENDIENTE'  AND full_join = 'AWS'
 and NOT exists (Select GCP.trm_id_tramite from GCP WHERE (gcp.nombre_completo_contratante=CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) AND GCP.trm_id_producto= 'AX-'||COD_PRODUCTO_FINAL) order by id_persona


 ----------
 ----------

 
 WITH COTI AS (
Select id_cotizacion_origen, nro_doc_cliente,codproducto_final,pago_flg ,Origen 
from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_unionall_vitto` 
 ),
Persona_data as (
  SELECT 
		a.id_persona, 
		--b.tip_documento, 
		b.num_documento, 
		a.nom_completo
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		CROSS JOIN UNNEST (a.documento_identidad) b
),
 AWS AS (
SELECT 
NRO_TRAMITE,NRO_POLIZA_FINAL,COD_PRODUCTO_FINAL,NRO_DOC_CONTRATANTE_FINAL,CONTRATANTE_FINAL,p.id_persona,
ASESOR,LOCALIDAD,GU,GA,PRODUCTO_FINAL,ESTADO_TRAMITE_MEDICION,ESTADO_TRAMITE_FINAL,FECHA_EMISION_SISTEMA,FECHA_RECEPCION_SOLICITUD,FECHA_CREACION_TRAMITE_LOTUS
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.aws_bd_produccion` aws left join persona_data p on aws.NRO_DOC_CONTRATANTE_FINAL = p.num_documento
WHERE MES_PRODUCCION = '202208'
), GCP AS (
SELECT IFNULL(trm_id_tramite,'VACIO') trm_id_tramite,trm_id_contratante,trm_id_producto,nombre_completo_contratante
 FROM `rs-nprd-dlk-dd-az-d8bc.anl_comercial.funnel_ventas` 
WHERE trm_mes_produccion = "2022-08-01"
and bit_des_subcanal = "FFVV VIDA" and bit_categoria <> "SIN DATOS"
  ),
Cruce as (
SELECT AWS.* ,
CASE
WHEN AWS.NRO_TRAMITE IS NULL AND GCP.trm_id_tramite IS NOT NULL THEN 'GCP'
WHEN GCP.trm_id_tramite IS NULL AND AWS.NRO_TRAMITE IS NOT NULL THEN 'AWS'
ELSE 'AMBOS'
END full_join,
CASE
WHEN AWS.NRO_TRAMITE IS NULL AND GCP.trm_id_tramite IS NOT NULL THEN IF(GCP.trm_id_tramite = 'VACIO','VACIO','TIENE NRO TRAMITE')
WHEN GCP.trm_id_tramite IS NULL AND AWS.NRO_TRAMITE IS NOT NULL THEN IF(AWS.NRO_TRAMITE = 'PENDIENTE','PENDIENTE','TIENE NRO TRAMITE')
ELSE 'AMBOS'
END sub_full_join
FROM AWS FULL JOIN GCP ON AWS.NRO_TRAMITE=GCP.trm_id_tramite),
TramitesFaltantes as (
Select * 
 from cruce where sub_full_join = 'TIENE NRO TRAMITE'  AND full_join = 'AWS'
 and NOT  exists (Select GCP.trm_id_tramite from GCP WHERE (gcp.nombre_completo_contratante=CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) AND GCP.trm_id_producto= 'AX-'||COD_PRODUCTO_FINAL) 
 UNION ALL
Select * 
 from cruce where sub_full_join = 'PENDIENTE'  AND full_join = 'AWS'
 and NOT  exists (Select GCP.trm_id_tramite from GCP WHERE (gcp.nombre_completo_contratante=CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) AND GCP.trm_id_producto= 'AX-'||COD_PRODUCTO_FINAL) )

 Select *, IFNULL((Select STRING_AGG(DISTINCT Origen ," | ") from coti
 WHERE coti.nro_doc_cliente=TF.NRO_DOC_CONTRATANTE_FINAL and coti.codproducto_final=TF.COD_PRODUCTO_FINAL),
 (Select STRING_AGG(DISTINCT Origen ," | ") from coti
 WHERE coti.nro_doc_cliente=TF.NRO_DOC_CONTRATANTE_FINAL)
 ) Origen ,
 (Select STRING_AGG(DISTINCT codproducto_final ," | ") from coti
 WHERE coti.nro_doc_cliente=TF.NRO_DOC_CONTRATANTE_FINAL) Productos,

  (Select STRING_AGG(DISTINCT des_estado_cotizacion ," | ") from	`rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_journey_tmp_vitto` j
  where j.nro_doc_cliente = TF.NRO_DOC_CONTRATANTE_FINAL and J.codproducto_final=TF.COD_PRODUCTO_FINAL)
  FROM tramitesFaltantes TF 


  ---- 19/19

  
 WITH
Persona_data as (
  SELECT 
		a.id_persona, 
		b.tip_documento, 
		b.num_documento, 
		a.nom_completo
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		CROSS JOIN UNNEST (a.documento_identidad) b
),
 AWS AS (
SELECT 
NRO_TRAMITE,NRO_POLIZA_FINAL,COD_PRODUCTO_FINAL,NRO_DOC_CONTRATANTE_FINAL,CONTRATANTE_FINAL,p.id_persona,
(Select STRING_AGG(id_poliza,"| ") from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.num_poliza=AWS.NRO_POLIZA_FINAL and PO.id_contratante=p.id_persona and PO.id_producto='AX-'||COD_PRODUCTO_FINAL) Id_Poliza
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.aws_bd_produccion` aws left join persona_data p on aws.NRO_DOC_CONTRATANTE_FINAL = p.num_documento  and p.tip_documento in ('DNI','CE')
WHERE MES_PRODUCCION = '202208'
), GCP AS (
SELECT IFNULL(trm_id_tramite,'VACIO') trm_id_tramite,trm_id_contratante,trm_id_producto,nombre_completo_contratante,pol_id_poliza
 FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` 
WHERE trm_mes_produccion = "2022-08-01"
and bit_des_subcanal = "FFVV VIDA" and bit_categoria <> "SIN DATOS"
  ),
Cruce as (
SELECT AWS.*,IF(Exists (
 select * from GCP where AWS.NRO_TRAMITE=GCP.trm_id_tramite ) ,TRUE,IF(
Exists (
 select * from GCP where ((gcp.nombre_completo_contratante=AWS.CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) 
                          AND GCP.trm_id_producto= 'AX-'||AWS.COD_PRODUCTO_FINAL
                          AND AWS.Id_Poliza=gcp.pol_id_poliza)) ,TRUE ,FALSE))Estan_En_Funnel,

(Select STRING_AGG(cast(gcp.trm_mes_produccion as string),'| ') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` gcp Where 
 (((gcp.nombre_completo_contratante=AWS.CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) AND GCP.trm_id_producto= 'AX-'||AWS.COD_PRODUCTO_FINAL AND AWS.Id_Poliza=gcp.pol_id_poliza)
 or gcp.trm_id_tramite = AWS.NRO_TRAMITE)
 AND GCP.bit_des_subcanal = "FFVV VIDA" and GCP.bit_categoria <> "SIN DATOS"  ) PeriodoGCP,

 (Select STRING_AGG(cast(gcp.pol_id_poliza as string),'| ') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` gcp Where 
 (((gcp.nombre_completo_contratante=AWS.CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) AND GCP.trm_id_producto= 'AX-'||AWS.COD_PRODUCTO_FINAL)
 or gcp.trm_id_tramite = AWS.NRO_TRAMITE)
 AND GCP.bit_des_subcanal = "FFVV VIDA" and GCP.bit_categoria <> "SIN DATOS"  ) Id_Poliza_Funnel,

 (Select STRING_AGG(COT.origen_tramite,' - ') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_vitto` COT 
 WHERE COT.id_persona_cliente = id_persona
and COT.id_producto_final = 'AX-'||AWS.COD_PRODUCTO_FINAL) origen_tramite,

(Select STRING_AGG(numpol,'-') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto` U
where nro_doc_cliente = AWS.NRO_DOC_CONTRATANTE_FINAL
and codproducto_final = AWS.COD_PRODUCTO_FINAL) numpol_UNION,


(Select STRING_AGG(Po.Id_poliza,'-') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto` U INNER JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` Po
 ON Po.num_poliza=U.numpol
where U.nro_doc_cliente = AWS.NRO_DOC_CONTRATANTE_FINAL
and U.codproducto_final = AWS.COD_PRODUCTO_FINAL) id_poliza_union,

(	SELECT 
STRING_AGG(
		CASE 
			WHEN INSTR(TRIM(trm.numpoliza),'|') > 0 THEN TRIM(SUBSTRING(TRIM(trm.numpoliza),INSTR(TRIM(trm.numpoliza),'|')+1,20))
			WHEN INSTR(TRIM(trm.numpoliza),' ') > 0 THEN SAFE_CAST(SAFE_CAST(TRIM(SUBSTRING(TRIM(trm.numpoliza),INSTR(TRIM(trm.numpoliza),' ')+1,20)) AS numeric) AS string)
			WHEN SUBSTRING(TRIM(trm.numpoliza),1,1) = '0' THEN SAFE_CAST(SAFE_CAST(trm.numpoliza AS numeric) AS string)
			WHEN SUBSTRING(TRIM(trm.numpoliza),1,1) in ('V','U') THEN TRIM(regexp_replace(TRIM(trm.numpoliza),'[^0-9 ]',''))
			ELSE TRIM(trm.numpoliza) 
		END,'|') numpoliza_buscar, -- numero de poliza a buscar 
	FROM `rs-nprd-dlk-dd-rwz-a406.bdwf__appnote.TRAMITE` trm
	
  WHERE trm.codproducto=AWS.COD_PRODUCTO_FINAL
        AND trm.doccliente = AWS.NRO_DOC_CONTRATANTE_FINAL
   ),
  
  
(	SELECT 
STRING_AGG(
		PO.id_poliza,'|') numpoliza_buscar, -- numero de poliza a buscar 
	FROM `rs-nprd-dlk-dd-rwz-a406.bdwf__appnote.TRAMITE` trm
	LEFT JOIN  `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO
  
  ON  PO.num_poliza = (CASE 
			WHEN INSTR(TRIM(trm.numpoliza),'|') > 0 THEN TRIM(SUBSTRING(TRIM(trm.numpoliza),INSTR(TRIM(trm.numpoliza),'|')+1,20))
			WHEN INSTR(TRIM(trm.numpoliza),' ') > 0 THEN SAFE_CAST(SAFE_CAST(TRIM(SUBSTRING(TRIM(trm.numpoliza),INSTR(TRIM(trm.numpoliza),' ')+1,20)) AS numeric) AS string)
			WHEN SUBSTRING(TRIM(trm.numpoliza),1,1) = '0' THEN SAFE_CAST(SAFE_CAST(trm.numpoliza AS numeric) AS string)
			WHEN SUBSTRING(TRIM(trm.numpoliza),1,1) in ('V','U') THEN TRIM(regexp_replace(TRIM(trm.numpoliza),'[^0-9 ]',''))
			ELSE TRIM(trm.numpoliza) 
		END)

  WHERE trm.codproducto=AWS.COD_PRODUCTO_FINAL
        AND trm.doccliente = AWS.NRO_DOC_CONTRATANTE_FINAL
   )

FROM AWS )
Select * from cruce
where 
 Estan_En_Funnel  = FALSE
 and periodogcp is null
-- Id_Poliza_Funnel is null
-- and NRO_POLIZA_FINAL is not null
-- and Id_Poliza is not null
 order by 10 ,11


---
---22/09

  
  
 WITH
Persona_data as (
  SELECT 
		a.id_persona, 
		b.tip_documento, 
		b.num_documento, 
		a.nom_completo
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		CROSS JOIN UNNEST (a.documento_identidad) b
),
 AWS AS (
SELECT 
NRO_TRAMITE,NRO_POLIZA_FINAL,COD_PRODUCTO_FINAL,NRO_DOC_CONTRATANTE_FINAL,CONTRATANTE_FINAL,p.id_persona,
(Select STRING_AGG(id_poliza,"| ") from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.num_poliza=AWS.NRO_POLIZA_FINAL and PO.id_contratante=p.id_persona and PO.id_producto='AX-'||COD_PRODUCTO_FINAL) Id_Poliza
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.aws_bd_produccion` aws left join persona_data p on aws.NRO_DOC_CONTRATANTE_FINAL = p.num_documento  and p.tip_documento in ('DNI','CE')
WHERE MES_PRODUCCION = '202208'
), GCP AS (
SELECT IFNULL(trm_id_tramite,'VACIO') trm_id_tramite,trm_id_contratante,trm_id_producto,nombre_completo_contratante,pol_id_poliza
-- FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` 
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_cambios_vitto` 
WHERE trm_mes_produccion = "2022-08-01"
and bit_des_subcanal = "FFVV VIDA" and bit_categoria <> "SIN DATOS"
  ),
Cruce as (
SELECT AWS.*,IF(Exists (
 select * from GCP where AWS.NRO_TRAMITE=GCP.trm_id_tramite ) ,TRUE,IF(
Exists (
 select * from GCP where ((gcp.nombre_completo_contratante=AWS.CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) 
                          AND GCP.trm_id_producto= 'AX-'||AWS.COD_PRODUCTO_FINAL
                          AND AWS.Id_Poliza=gcp.pol_id_poliza)) ,TRUE ,FALSE))Estan_En_Funnel,

(Select STRING_AGG(cast(gcp.trm_mes_produccion as string),'| ') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_cambios_vitto` gcp Where 
 (((gcp.nombre_completo_contratante=AWS.CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) AND GCP.trm_id_producto= 'AX-'||AWS.COD_PRODUCTO_FINAL AND AWS.Id_Poliza=gcp.pol_id_poliza)
 or gcp.trm_id_tramite = AWS.NRO_TRAMITE)
 AND GCP.bit_des_subcanal = "FFVV VIDA" and GCP.bit_categoria <> "SIN DATOS"  ) PeriodoGCP,

 (Select STRING_AGG(cast(gcp.pol_id_poliza as string),'| ') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_cambios_vitto` gcp Where 
 (((gcp.nombre_completo_contratante=AWS.CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) AND GCP.trm_id_producto= 'AX-'||AWS.COD_PRODUCTO_FINAL)
 or gcp.trm_id_tramite = AWS.NRO_TRAMITE)
 AND GCP.bit_des_subcanal = "FFVV VIDA" and GCP.bit_categoria <> "SIN DATOS"  ) Id_Poliza_Funnel,

 (Select STRING_AGG(COT.origen_tramite,' - ') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_cambio_vitto` COT 
 WHERE COT.id_persona_cliente = id_persona
and COT.id_producto_final = 'AX-'||AWS.COD_PRODUCTO_FINAL) origen_tramite_cotizacion,

(Select STRING_AGG(numpol,'-') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_union_tmp_vitto` U
where nro_doc_cliente = AWS.NRO_DOC_CONTRATANTE_FINAL
and codproducto_final = AWS.COD_PRODUCTO_FINAL) numpol_UNION,



(	SELECT 
STRING_AGG(
		CASE 
			WHEN INSTR(TRIM(trm.numpoliza),'|') > 0 THEN TRIM(SUBSTRING(TRIM(trm.numpoliza),INSTR(TRIM(trm.numpoliza),'|')+1,20))
			WHEN INSTR(TRIM(trm.numpoliza),' ') > 0 THEN SAFE_CAST(SAFE_CAST(TRIM(SUBSTRING(TRIM(trm.numpoliza),INSTR(TRIM(trm.numpoliza),' ')+1,20)) AS numeric) AS string)
			WHEN SUBSTRING(TRIM(trm.numpoliza),1,1) = '0' THEN SAFE_CAST(SAFE_CAST(trm.numpoliza AS numeric) AS string)
			WHEN SUBSTRING(TRIM(trm.numpoliza),1,1) in ('V','U') THEN TRIM(regexp_replace(TRIM(trm.numpoliza),'[^0-9 ]',''))
			ELSE TRIM(trm.numpoliza) 
		END,'|') numpoliza_buscar, -- numero de poliza a buscar 
	FROM `rs-nprd-dlk-dd-rwz-a406.bdwf__appnote.TRAMITE` trm
	
  WHERE trm.codproducto=AWS.COD_PRODUCTO_FINAL
        AND trm.doccliente = AWS.NRO_DOC_CONTRATANTE_FINAL
   ),
  
  

EXISTS (Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.tramite_cambios_vitto` U 
where U.num_documento_cliente = AWS.NRO_DOC_CONTRATANTE_FINAL
and U.id_producto =  'AX-'||COD_PRODUCTO_FINAL )existe_tablatramite_contratante_producto ,

EXISTS (Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.tramite_cambios_vitto` U 
where U.id_poliza = AWS.Id_Poliza ) existe_tablatramite,
(Select STRING_AGG(Distinct U.origen_data,'|') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.tramite_cambios_vitto` U 
where U.id_poliza = AWS.Id_Poliza ) tablatramite_ORIGEN_DATA,

EXISTS (
 SELECT * FROM
 `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_journey_tmp_vitto` 
 WHERE cot_nro_poliza = AWS.NRO_POLIZA_FINAL) COT_JOURNEY,

  (
 SELECT STRING_AGG(cot_nro_poliza,'|') FROM
 `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_journey_tmp_vitto` 
 WHERE AWS.COD_PRODUCTO_FINAL = codproducto_final
 AND AWS.NRO_DOC_CONTRATANTE_FINAL = nro_doc_cliente ) COT_JOURNEY_pol,

 
  (
 SELECT STRING_AGG(cot_nro_poliza,'|') FROM
 `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_journey_tmp_vitto` 
 WHERE AWS.COD_PRODUCTO_FINAL = codproducto_final
 AND AWS.NRO_DOC_CONTRATANTE_FINAL = nro_doc_cliente ) COT_JOURNEY_pol,

 (
select STRING_AGG(C.id_tramite,'|') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_cambio_vitto` C
where C.numpol_ext_cot = AWS.NRO_POLIZA_FINAL) id_tramite_cotizacion,
 (
select STRING_AGG(C.origen_data,'|') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_cambio_vitto` C
where C.numpol_ext_cot = AWS.NRO_POLIZA_FINAL) origen_data_cotizacion,

(
Select 
	STRING_AGG( (case 
		 	WHEN trm.codproducto = '8917' AND trm.glosa LIKE '%JOURNEY EXPRESS%' THEN '8917 Y JOURNEY EXPRESS'
  			WHEN trm.codproducto = '8202' THEN  '8202'
  			ELSE 'NORMAL' END) , ' | ') 

  from `rs-nprd-dlk-dd-rwz-a406.bdwf__appnote.TRAMITE` trm
	
	where --trm.codproducto = AWS.COD_PRODUCTO_FINAL and TRIM(trm.DOCCLIENTE)  = AWS.NRO_DOC_CONTRATANTE_FINAL
  trm.NUMPOLIZA = AWS.NRO_POLIZA_FINAL

) NuevoFiltro_Tramite_Raw


FROM AWS )
Select * from cruce
--where 
--NRO_DOC_CONTRATANTE_FINAL	='72617279'
 --COD_PRODUCTO_FINAL = '8202'
 --Estan_En_Funnel  = FALSE
 --and periodogcp is null
--Id_Poliza_Funnel is null
--and NRO_POLIZA_FINAL is not null
-- and Id_Poliza is not null