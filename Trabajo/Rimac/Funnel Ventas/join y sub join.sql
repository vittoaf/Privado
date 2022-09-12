WITH AWS AS (
SELECT 
NRO_TRAMITE
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.aws_bd_produccion` 
WHERE MES_PRODUCCION = '202208'
), GCP AS (
SELECT IFNULL(trm_id_tramite,'VACIO') trm_id_tramite
 FROM `rs-nprd-dlk-dt-anlyt-msrl-1570.anl_comercial.funnel_ventas` 
WHERE trm_mes_produccion = "2022-08-01"
and bit_des_subcanal = "FFVV VIDA" and bit_categoria <> "SIN DATOS"
  ),
Cruce as (
SELECT * ,
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
Select full_join,sub_full_join,count(*) Cantidad from cruce group by full_join,sub_full_join
order by 1
--------------------
WITH AWS AS (
SELECT 
NRO_TRAMITE
FROM `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.aws_bd_produccion` 
WHERE MES_PRODUCCION = '202208'
), GCP AS (
SELECT IFNULL(trm_id_tramite,'VACIO') trm_id_tramite
 FROM `rs-nprd-dlk-dt-anlyt-msrl-1570.anl_comercial.funnel_ventas` 
WHERE trm_mes_produccion = "2022-08-01"
and bit_des_subcanal = "FFVV VIDA" and bit_categoria <> "SIN DATOS"
  ),
Cruce as (
SELECT * ,
CASE
WHEN AWS.NRO_TRAMITE IS NULL AND GCP.trm_id_tramite IS NOT NULL THEN 'GCP'
WHEN GCP.trm_id_tramite IS NULL AND AWS.NRO_TRAMITE IS NOT NULL THEN 'AWS'
ELSE 'AMBOS'
END full_join
FROM AWS FULL JOIN GCP ON AWS.NRO_TRAMITE=GCP.trm_id_tramite)
Select full_join,count(*) Cantidad from cruce group by full_join
order by 1
