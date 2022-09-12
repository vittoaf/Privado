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