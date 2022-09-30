WITH Persona_data as (
  SELECT 
		a.id_persona, 
		b.tip_documento, 
		b.num_documento, 
		a.nom_completo
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		CROSS JOIN UNNEST (a.documento_identidad) b
),AWS AS (
SELECT 
NRO_TRAMITE,NRO_POLIZA_FINAL,COD_PRODUCTO_FINAL,NRO_DOC_CONTRATANTE_FINAL,CONTRATANTE_FINAL,p.id_persona,
ASESOR,LOCALIDAD,GU,GA,PRODUCTO_FINAL,ESTADO_TRAMITE_MEDICION,ESTADO_TRAMITE_FINAL,FECHA_EMISION_SISTEMA,FECHA_CREACION_TRAMITE_LOTUS,FECHA_RECEPCION_SOLICITUD,

EXISTS (Select * from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.num_poliza=AWS.NRO_POLIZA_FINAL and PO.id_contratante=p.id_persona and PO.id_producto='AX-'||COD_PRODUCTO_FINAL) CruceCompleto_Exists,
(Select STRING_AGG(id_poliza,"| ") from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.num_poliza=AWS.NRO_POLIZA_FINAL and PO.id_contratante=p.id_persona and PO.id_producto='AX-'||COD_PRODUCTO_FINAL) CruceCompleto,

EXISTS(Select*from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.num_poliza=AWS.NRO_POLIZA_FINAL and PO.id_producto='AX-'||COD_PRODUCTO_FINAL) CrucePolizaProducto_Exists,
(Select STRING_AGG(id_poliza,"| ") from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.num_poliza=AWS.NRO_POLIZA_FINAL and PO.id_producto='AX-'||COD_PRODUCTO_FINAL) CrucePolizaProducto,


EXISTS(Select * from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.id_contratante=p.id_persona and PO.id_producto='AX-'||COD_PRODUCTO_FINAL
and ind_poliza_vigente = 1) CruceContratanteProducto_Exists,
(Select STRING_AGG(id_poliza,"| ") from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.id_contratante=p.id_persona and PO.id_producto='AX-'||COD_PRODUCTO_FINAL
and ind_poliza_vigente = 1) CruceContratanteProducto,


 EXISTS (Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` gcp Where gcp.trm_id_tramite = AWS.NRO_TRAMITE
 AND GCP.trm_mes_produccion='2022-08-01' AND GCP.bit_des_subcanal = "FFVV VIDA" and GCP.bit_categoria <> "SIN DATOS"  ) CruceGCPTramite_Exists,

  (Select gcp.pol_id_poliza from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` gcp Where gcp.trm_id_tramite = AWS.NRO_TRAMITE
 AND GCP.trm_mes_produccion='2022-08-01' AND GCP.bit_des_subcanal = "FFVV VIDA" and GCP.bit_categoria <> "SIN DATOS"  ) CruceGCPTramite,


 EXISTS(Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` gcp Where 
 ((gcp.nombre_completo_contratante=AWS.CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) AND GCP.trm_id_producto= 'AX-'||AWS.COD_PRODUCTO_FINAL)
 AND GCP.trm_mes_produccion='2022-08-01' AND GCP.bit_des_subcanal = "FFVV VIDA" and GCP.bit_categoria <> "SIN DATOS"  ) CruceGCPContratanteProducto_Exists,

 (Select STRING_AGG(distinct gcp.pol_id_poliza,'| ') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` gcp Where 
 ((gcp.nombre_completo_contratante=AWS.CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) AND GCP.trm_id_producto= 'AX-'||AWS.COD_PRODUCTO_FINAL)
 AND GCP.trm_mes_produccion='2022-08-01' AND GCP.bit_des_subcanal = "FFVV VIDA" and GCP.bit_categoria <> "SIN DATOS"  ) CruceGCPContratanteProducto,

 (Select STRING_AGG(cast(gcp.trm_mes_produccion as string),'| ') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` gcp Where 
 (((gcp.nombre_completo_contratante=AWS.CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) AND GCP.trm_id_producto= 'AX-'||AWS.COD_PRODUCTO_FINAL)
 or gcp.trm_id_tramite = AWS.NRO_TRAMITE)
 AND GCP.bit_des_subcanal = "FFVV VIDA" and GCP.bit_categoria <> "SIN DATOS"  ) Periodo_GCP,

 (Select STRING_AGG(cast(fec_emision as string),"| ") from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.num_poliza=AWS.NRO_POLIZA_FINAL and PO.id_producto='AX-'||COD_PRODUCTO_FINAL and PO.id_contratante=p.id_persona) Poliza_FechaEmision,

(Select STRING_AGG(cast(gcp.trm_fec_creacion as string),'| ') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` gcp Where 
 (((gcp.nombre_completo_contratante=AWS.CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) AND GCP.trm_id_producto= 'AX-'||AWS.COD_PRODUCTO_FINAL)
 or gcp.trm_id_tramite = AWS.NRO_TRAMITE)
 AND GCP.bit_des_subcanal = "FFVV VIDA" and GCP.bit_categoria <> "SIN DATOS"  ) GCP_Fec_Creacion,

(Select STRING_AGG(cast(gcp.pol_fec_emision as string),'| ') from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` gcp Where 
 (((gcp.nombre_completo_contratante=AWS.CONTRATANTE_FINAL OR GCP.trm_id_contratante= id_persona ) AND GCP.trm_id_producto= 'AX-'||AWS.COD_PRODUCTO_FINAL)
 or gcp.trm_id_tramite = AWS.NRO_TRAMITE)
 AND GCP.bit_des_subcanal = "FFVV VIDA" and GCP.bit_categoria <> "SIN DATOS"  ) GCP_Fec_Emision,

FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.aws_bd_produccion` aws 
left join persona_data p on p.tip_documento in ('DNI','CE') AND aws.NRO_DOC_CONTRATANTE_FINAL = p.num_documento
WHERE AWS.MES_PRODUCCION = '202208'
)select * from aws



