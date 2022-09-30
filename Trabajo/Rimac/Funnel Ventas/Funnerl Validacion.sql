WITH GCP AS (Select trm_id_tramite,nom_intermediario,pol_id_poliza,trm_id_contratante,
nombre_completo_contratante,trm_fec_creacion,pol_fec_emision,trm_fec_solicitud,trm_id_producto,
Exists(Select * from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.id_contratante=gcp.trm_id_contratante and PO.id_producto=gcp.trm_id_producto and po.id_poliza=gcp.pol_id_poliza) CruceCompleto_Exists,
(Select id_poliza from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.id_contratante=gcp.trm_id_contratante and PO.id_producto=gcp.trm_id_producto and po.id_poliza=gcp.pol_id_poliza) CruceCompleto,
Exists(Select * from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.id_contratante=gcp.trm_id_contratante and PO.id_producto=gcp.trm_id_producto) CruceContratante_Producto_Exists,
(Select STRING_AGG(id_poliza,'|') from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` PO where PO.id_contratante=gcp.trm_id_contratante and PO.id_producto=gcp.trm_id_producto) CruceContratante_Producto
 
 from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` gcp 
Where  GCP.trm_mes_produccion='2022-08-01' AND GCP.bit_des_subcanal = "FFVV VIDA" and GCP.bit_categoria <> "SIN DATOS")
Select *,IFNULL(pol_id_poliza,'0')=IFNULL(CruceCompleto,'0') CoincidenciaCompleta,
IFNULL(pol_id_poliza,'0')=IFNULL(CruceContratante_Producto,'0') CoincidenciaContratanteProducto,
 from gcp
where IFNULL(pol_id_poliza,'0')<>IFNULL(CruceContratante_Producto,'0') 
and CruceCompleto_exists =false
order by 3,10 desc

/*SELECT COUNT(*)   from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` 
WHERE trm_mes_produccion IS NULL*/