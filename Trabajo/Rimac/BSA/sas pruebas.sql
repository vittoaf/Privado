Select count(1) Cantidad,'array' Tabla from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_array`
UNION ALL
Select count(1) Cantidad, 'array_ind' from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_ind_array`
UNION ALL
Select count(1) Cantidad, 'original' from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto`

Select numpol,cod_prod,count(distinct id_poliza) from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_hernan_array`
group by numpol,cod_prod
order by 3 desc

Select id_poliza,numpol,cod_prod,count(1) from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_hernan`
group by id_poliza,numpol,cod_prod
having count(1)>1;

Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_hernan`
where id_poliza = 'AX-12419549' and id_certificado = 'AX-12419549-1'

Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_hernan`
where numpol = '1010475' and cod_prod = '2101';

Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_hernan_array`
where numpol = '1010475' and cod_prod = '2101';

Select * from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto`
where numpol = '1010475' and cod_prod = '2101'

SELECT *
--po.id_poliza,po.num_poliza,po.id_producto,ARRAY_AGG(IFNULL(id_certificado,'')) id_certificado 
FROM `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` po
where num_poliza = '175630' AND cod_producto_origen = '2101'

----

Select count(1) Cantidad,'array' Tabla from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_array`
UNION ALL
Select count(1) Cantidad, 'array_ind' from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_ind_array`
UNION ALL
Select count(1) Cantidad, 'group_by' from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_group_by`
UNION ALL
Select count(1) Cantidad, 'original' from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto`;


----

SELECT *
FROM `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` po
where id_poliza = 'AX-13336890'

SELECT * FROM `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.certificado`
 WHERE id_poliza = 'AX-13336890'  order by fec_inicio_vigencia

 
With nuevo as (
select id_cotizacion_origen,count(1) Cantidad
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_group_by`
where codproducto_final = '2101'
 group by  id_cotizacion_origen
)
, actual as (
select id_cotizacion_origen,count(1) Cantidad
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto` 
where codproducto_final = '2101'
 group by  id_cotizacion_origen
)
Select numpol,string_agg(distinct id_poliza,'; ')id_poliza from  `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_group_by`
where id_cotizacion_origen in 
(Select a.id_cotizacion_origen 
from actual a full join nuevo n on a.id_cotizacion_origen = n.id_cotizacion_origen
where a.Cantidad <> n.Cantidad)
group by numpol
;

With nuevo as (
select codproducto_final,count(1) Cantidad
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_group_by`
where codproducto_final = '2101'
 group by  codproducto_final
)
, actual as (
select codproducto_final,count(1) Cantidad
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto` 
where codproducto_final = '2101'
 group by  codproducto_final
)
Select * 
from actual a full join nuevo n on a.codproducto_final = n.codproducto_final
where a.Cantidad <> n.Cantidad
;
---------------------
Select EstadoCertificado,count(1)Cantidad from (
Select a.id_poliza,IF(COUNT(b)<=1,"1 CERTIFICADO","+ 1 CERTIFICADO") EstadoCertificado
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_group_by`  a
cross join unnest (a.id_certificado) b
where a.codproducto_final = '2101'
group by a.id_poliza )
group by EstadoCertificado

Select IFNULL(EstadoCertificado,'Total') NroCertificado,count(1)Cantidad,
round(count(1) * 100 / SUM(count(1)) OVER(partition by if(EstadoCertificado is null,'1','0')),0)  as Porcentaje
 from (
Select a.id_poliza,IF(COUNT(b)<=1,"1 CERTIFICADO","+ 1 CERTIFICADO") EstadoCertificado
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_group_by`  a
cross join unnest (a.id_certificado) b
where a.codproducto_final = '2101'
group by a.id_poliza )
group by ROLLUP(EstadoCertificado)
order by 2

Select Ifnull(NroCertificado,'Total')NroCerticado,count(1) Cantidad ,
Round(Count(1)*100 / SUM(Count(1)) OVER (PARTITION BY If(NroCertificado is null,'1','0') ),0) Porcentaje
FROM (
Select a.id_poliza,
CASE WHEN a.id_poliza IS NULL THEN 'Sin id_poliza'
    ELSE IF(ARRAY_LENGTH(id_certificado)<=1,'Polizas con 1 Certificado','Polizas con + 1 Certificado') 
    END AS NroCertificado
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_group_by`  a
where a.codproducto_final = '2101')
GROUP BY ROLLUP(NroCertificado)
order by 2 asc
---
Select IF(a.numpol is null,'Numpol Null','Numpol No Null') Numpol,
      if(id_poliza is null, 'Poliza Null','Poliza No Null') Poliza,
      count(1) Cantidad
from `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_group_by`  a
where a.codproducto_final = '2101' and a.numpol is not null
group by Rollup(IF(a.numpol is null,'Numpol Null','Numpol No Null'),
      if(id_poliza is null, 'Poliza Null','Poliza No Null') )

      ----


      
Select numpol,periodo periodo_cotizacion,
IF(numpol is null,false,true) Tiene_Nro_Poliza,
EXISTS( Select id_poliza from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` po
  Where po.num_poliza = a.numpol
      ) Tiene_id_poliza,
EXISTS( Select cod_producto_origen from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` po
  Where po.num_poliza = a.numpol
        and po.cod_producto_origen = '2101'
      ) Tiene_producto_2101,
EXISTS( Select * from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` po
  Where po.num_poliza = a.numpol
        and po.cod_producto_origen = '2101'
        AND (
		po.fec_inicio_vigencia BETWEEN 
		DATE_ADD(DATE_TRUNC(CAST(a.periodo AS DATE),MONTH), INTERVAL -3 MONTH)
		AND 
		DATE_ADD(DATE_ADD(DATE_TRUNC(CAST(a.periodo AS DATE),MONTH), INTERVAL 4 MONTH), INTERVAL -1 DAY)
        )
      ) Tiene_fec_ini_vigencia_3meses_antes_despues,
  EXISTS( Select * from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` po
  Where po.num_poliza = a.numpol
        and po.cod_producto_origen = '2101'
        AND (
		po.fec_inicio_vigencia BETWEEN 
		DATE_ADD(DATE_TRUNC(CAST(a.periodo AS DATE),MONTH), INTERVAL -3 MONTH)
		AND 
		DATE_ADD(DATE_ADD(DATE_TRUNC(CAST(a.periodo AS DATE),MONTH), INTERVAL 4 MONTH), INTERVAL -1 DAY)
        )
        and po.fec_inicio_vigencia >= a.periodo
      ) Tiene_fec_ini_vigencia_mayor_periodo,

      cast(DATE_ADD(DATE_TRUNC(CAST(a.periodo AS DATE),MONTH), INTERVAL -3 MONTH) as string) || ' ≤ ' ||
      (Select 
        STRING_AGG(CAST(po.fec_inicio_vigencia AS STRING),' | ')
        from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` po
        Where po.num_poliza = a.numpol
        and po.cod_producto_origen = '2101'
      ) ||
      ' ≤ ' || 
      DATE_ADD(DATE_ADD(DATE_TRUNC(CAST(a.periodo AS DATE),MONTH), INTERVAL 4 MONTH), INTERVAL -1 DAY),

      (Select 
        STRING_AGG(CAST(po.fec_inicio_vigencia AS STRING),' | ')
        from `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` po
        Where po.num_poliza = a.numpol
        and po.cod_producto_origen = '2101'
      ) || ' ≥ '||CAST(a.periodo AS DATE)

FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_group_by`  a
where codproducto_final = '2101' and id_poliza is null 
and numpol is not null 
----

select a.id_cotizacion_origen,a.numpol,a.id_poliza,b
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_sas_tmp_vitto_bsa_group_by` a
cross join unnest(a.id_certificado) b
where codproducto_final = '2101'
AND NUMPOL IS NOT NULL
and b = ''
order by NUMPOL,1 