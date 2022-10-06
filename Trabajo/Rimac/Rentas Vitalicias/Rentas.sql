WITH Persona as (SELECT 
		a.id_persona, 
		b.tip_documento, 
		b.num_documento, 
		a.nom_completo
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		CROSS JOIN UNNEST (a.documento_identidad) b)
SELECT COD_PRODUCTO_FINAL,NRO_POLIZA_FINAL,NRO_DOC_CONTRATANTE_FINAL,persona.id_persona, 
STRUCT(ARRAY_AGG(IFNULL(p.id_poliza,'')) as id_poliza,
      ARRAY_AGG(IFNULL(p.id_contratante,'')) as id_contratante,
      ARRAY_AGG(IFNULL(p.id_producto,'')) as id_producto )modelo_poliza 
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.aws_rentas` r
left join `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` p ON (p.num_poliza = r.NRO_POLIZA_FINAL or p.num_poliza = '00'||r.NRO_POLIZA_FINAL) and  p.id_producto = 'AX-'||r.COD_PRODUCTO_FINAL
LEFT JOIN persona ON persona.num_documento = r.NRO_DOC_CONTRATANTE_FINAL and persona.tip_documento in ('DNI','CE')
group by COD_PRODUCTO_FINAL,NRO_POLIZA_FINAL,NRO_DOC_CONTRATANTE_FINAL,persona.id_persona;
