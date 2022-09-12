CREATE  OR REPLACE TABLE `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.NuevoCalendario_Vitto`
AS
With Calendario as (
Select '2022-01-27' FecCierre,'2022-01-28' FecSuscripcion,'2022-01-31'FecEmision
UNION ALL
Select '2022-02-24','2022-02-25','2022-02-28'
UNION ALL
Select '2022-03-29','2022-03-30','2022-03-31'
UNION ALL
Select '2022-04-27','2022-04-28','2022-04-29'
UNION ALL
Select '2022-05-27','2022-05-30','2022-05-31'
UNION ALL
Select '2022-06-27','2022-06-28','2022-06-30'
UNION ALL
Select '2022-07-25','2022-07-26','2022-07-27'
UNION ALL
Select '2022-08-29','2022-08-30','2022-08-31'
UNION ALL
Select '2022-09-28','2022-09-29','2022-09-30'
UNION ALL
Select '2022-10-27','2022-10-28','2022-10-31'
UNION ALL
Select '2022-11-28','2022-11-29','2022-11-30'
UNION ALL
Select '2022-12-28','2022-12-29','2022-12-30')

Select cast(FecCierre as TIMESTAMP) FecCierre,
cast(FecSuscripcion as TIMESTAMP) FecSuscripcion,
cast(FecEmision as TIMESTAMP) FecEmision FROM Calendario