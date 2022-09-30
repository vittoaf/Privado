with persona_data
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
Tramite8917 as (
     select NUMPOLIZA,GLOSA,DOCCLIENTE,pe.id_persona,CODPRODUCTO--,NUMTRAMITE
	FROM `rs-nprd-dlk-dd-rwz-a406.bdwf__appnote.TRAMITE` trm
	LEFT JOIN persona_data pe ON pe.num_documento = trm.DOCCLIENTE 
	where CODPRODUCTO = '8917' ),
Cotizacion8917 as (
Select numpol_ext_cot,id_persona_cliente,origen_data,id_producto_final,flg_presentada,
origen_tramite,id_tramite
 from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_cambio_vitto` 
where id_producto_final = 'AX-8917'
)
Select * from Tramite8917 trm 
inner join Cotizacion8917 c ON trm.numpoliza= c.numpol_ext_cot AND trm.id_persona=c.id_persona_cliente
where trm.numpoliza='1300001387'
order by 1 desc
