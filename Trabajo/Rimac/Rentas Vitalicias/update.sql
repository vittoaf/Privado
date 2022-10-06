UPDATE `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.aws_rentas` aws
SET AWS.NRO_DOC_CONTRATANTE_FINAL = RIGHT('000'||aws.NRO_DOC_CONTRATANTE_FINAL,8)
where length(NRO_DOC_CONTRATANTE_FINAL) <> 8