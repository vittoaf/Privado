select 'ACTUAL' Estado,COUNT(pol_id_poliza) NO_NULOS,COUNT(*) TOTAL from 
`rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto_anterior` 
where trm_id_tramite like 'JV-%'
union all
select 'MODIFICADO',COUNT(pol_id_poliza),COUNT(*)  from 
`rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto` 
where trm_id_tramite like 'JV-%'
UNION ALL
SELECT 'DD',COUNT(pol_id_poliza),COUNT(*)  FROM `rs-nprd-dlk-dd-az-d8bc.anl_comercial.funnel_ventas` 
WHERE trm_mes_produccion = "2022-08-01"
and trm_id_tramite like 'JV-%';