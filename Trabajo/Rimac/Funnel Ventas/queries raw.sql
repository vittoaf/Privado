select id_persona
from  `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_persona`
where nro_documento like '%001739744%' or
nro_documento like '%47821651%' or nro_documento like '%001739744%' or nro_documento like '%08081798%' 
or nro_documento like '%10064074%' or nro_documento like '%45810851%' or nro_documento like '%43472765%' 
or nro_documento like '%41684655%' or nro_documento like '%72359676%' or nro_documento like '%48409113%' 
or nro_documento like '%44728329%' or nro_documento like '%46850250%' or nro_documento like '%72654928%' 
or nro_documento like '%45252116%' or nro_documento like '%42084777%' or nro_documento like '%45482328%' 
or nro_documento like '%44008833%'
order by nro_documento

----
BUSCAR POR UN ID PersoNA 

WITH pre_cot_data AS (
	SELECT 
		tmp_pre.id_adn,MAX(tmp_pre.id_precotizacion) AS id_precotizacion
	FROM 
		--`rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` tmp_pre
		--`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` tmp_pre
		`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` tmp_pre
	GROUP BY 
		tmp_pre.id_adn
),

pre_pag_data AS (
	SELECT 
		tmp_pag.id_cotizacion, 
		MAX(tmp_pag.id_pago) AS id_pago
	FROM 
		--`rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` tmp_pag
		--`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` tmp_pag
		`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` tmp_pag
	WHERE
		(tmp_pag.estado > -1) 
		OR (tmp_pag.estado = -1 AND tmp_pag.mensaje ='Se inactiva por toc negativo')
	GROUP BY 
		tmp_pag.id_cotizacion
)

SELECT
	CAST(cot.id_cotizacion AS STRING) AS `id_cotizacion_origen`,
	IFNULL(cfg_origenld.DESCRIP_HOMOLOGADO,'N.D.') AS `origen_lead_cliente`,
	CAST(usu.idusuario AS STRING) AS `codigo_asesor`,
	usu.correo AS `email_asesor`,
	seg.documento AS `nro_doc_asesor`,
	IFNULL(REPLACE(cfg_tipdoca.DESCRIP_ORIGEN,'.',''),'N.D.') AS `tip_doc_asesor`,
	per.nro_documento AS `nro_doc_cliente`,seg.codigo_acselx,pag.num_obligacion,
	IFNULL(REPLACE(cfg_tipdocc.DESCRIP_ORIGEN,'.',''),'N.D.') AS `tip_doc_cliente`,
	'JOURNEY' AS origen_data,

FROM 
	--`rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_bitacora` b
	--`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_bitacora` b
	`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_bitacora` b

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_agendamiento` a
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_agendamiento` a
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_agendamiento` a
		ON (
			(b.id_cliente = a.id_cliente) 
			AND (b.usu_crea = a.id_usuario)
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_estado` est
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_estado` est
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_estado` est
		ON (
			est.id_estado = b.id_estado
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_persona` per
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_persona` per
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_persona` per
		ON (
			a.id_persona = per.id_persona
			and per.id_persona = 230529
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_seg_usuario` seg
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_seg_usuario` seg
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_seg_usuario` seg
		ON (
			a.id_usuario = seg.idusuario
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_usuario` usu
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_usuario` usu
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_usuario` usu
		ON (
			a.id_usuario = usu.idusuario
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_adn` adn
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_adn` adn
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_adn` adn
		ON (
			b.id_adn = adn.id_adn
		)
	LEFT JOIN pre_cot_data max_pre
		ON (
			max_pre.id_adn = adn.id_adn
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` pre
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` pre
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` pre
		ON (
			pre.id_adn = adn.id_adn 
			AND pre.id_precotizacion=max_pre.id_precotizacion
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_cotizacion` cot
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_cotizacion` cot
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_cotizacion` cot
		ON (
			b.id_cotizacion = cot.id_cotizacion
		)
	LEFT JOIN pre_pag_data max_pag
		ON (
			max_pag.id_cotizacion = b.id_cotizacion
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` pag
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` pag
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` pag
		ON (
			pag.id_cotizacion = b.id_cotizacion 
			AND pag.id_pago = max_pag.id_pago
		)
	/*BEGIN: Se agrego para logica de flg_presentada*/
	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_primer_pago` pripag
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_primer_pago` pripag
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_primer_pago` pripag
		ON (
			pripag.id_cotizacion = b.id_cotizacion
		)

	/*END: Se agrego para logica de flg_presentada*/
	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.ue2dbaprodrdjovv001__db_journeyvv.jvv_producto` pro
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_producto` pro
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_producto` pro
		ON (
			cot.`cod_producto` = pro.`nombre`
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.de__datalake.cfg_staging_maestra` cfg_estado
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_estado
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_estado
		ON (
			cfg_estado.ID_ORIGEN='JOURNEY' 
			AND cfg_estado.TIPO_PARAMETRO='ESTADOCOTI' 
			AND cfg_estado.CODIGO_ORIGEN=CAST(cot.estado AS STRING)
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.de__datalake.cfg_staging_maestra` cfg_tipdoca
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_tipdoca
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_tipdoca
		ON (
			cfg_tipdoca.ID_ORIGEN='JOURNEY' 
			AND cfg_tipdoca.TIPO_PARAMETRO='TIPODOC' 
			AND cfg_tipdoca.CODIGO_ORIGEN=seg.tipodocumento
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.de__datalake.cfg_staging_maestra` cfg_tipdocc
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_tipdocc
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_tipdocc
		ON (
			cfg_tipdocc.ID_ORIGEN='JOURNEY' 
			AND cfg_tipdocc.TIPO_PARAMETRO='TIPODOC' 
			AND cfg_tipdocc.CODIGO_ORIGEN=CAST(per.id_tipodocumento AS STRING)
		)

	--LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.de__datalake.cfg_staging_maestra` cfg_origenld
	--LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_origenld
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.de__datalake.cfg_staging_maestra` cfg_origenld
		ON (
			cfg_origenld.ID_ORIGEN='JOURNEY' 
			AND cfg_origenld.TIPO_PARAMETRO='ORIGENLEAD' 
			AND cfg_origenld.CODIGO_ORIGEN=UPPER(per.origen)
		)
WHERE
	((b.`id_estado` > 0) OR ((b.`id_estado` = 0) AND (b.`id_adn` IS NOT NULL)))
	AND (
		usu.correo NOT IN (
			'xt1568@rimac.com.pe',
			'daniella.poppe@rimac.com.pe',
			'andrea.garavito@rimac.com.pe',
			'yoisy.vasquez@rimac.com.pe',
			'yumiko.asato@rimac.com.pe',
			'lcastanedar@rimac.com.pe',
			'mirella.concha@rimac.com.pe',
			'carlos.zeballos@rimac.com.pe',
			'xt2385@rimac.com.pe',
			'paloma.urbina@rimac.com.pe',
			'xt1765@rimac.com.pe',
			'wendy.su@rimac.com.pe',
			'xt1931@rimac.com.pe',
			'xt1702@rimac.com.pe',
			'cesar.acuna@rimac.com.pe',
			'miguel.rodriguezb@rimac.com.pe',
			'miguel.corahua@rimac.com.pe',
			'abelzu@rimac.com.pe',
			'pedro.valle@rimac.com.pe',
			'eatanta@indracompany.com',
			'xt1874@rimac.com.pe',
			'david.inga@rimac.com.pe',
			'xt1403@rimac.com.pe',
			'antonela.mendezh@rimac.com.pe',
			'shayla.gomez@rimac.com.pe',
			'xt0033@rimac.com.pe',
			'mfarfanr@rimac.com.pe',
			'maria.altet@rimac.com.pe',
			'jorge.vilca@rimac.com.pe',
			'melissa.ponce@rimac.com.pe',
			'xt2649@rimac.com.pe',
			'xt2844@rimac.com.pe',
			'ruben.valdivieso@rimac.com.pe',
			'xt2603@rimac.com.pe'
		) 
	)
	AND
	(
		(
			(
				CAST(TRIM(REGEXP_REPLACE(seg.documento,'[^0-9]','')) AS BIGINT) = CAST(TRIM(REGEXP_REPLACE(per.nro_documento,'[^0-9]','')) AS BIGINT) 
				AND pag.num_obligacion IS NOT NULL
			) -- estado_pago='Procesado'
			or
			(
				CAST(TRIM(REGEXP_REPLACE(seg.documento,'[^0-9]','')) AS BIGINT) <> CAST(TRIM(REGEXP_REPLACE(per.nro_documento,'[^0-9]','')) AS BIGINT) 
			)
		)
		AND per.nro_documento NOT LIKE '999999%'
		--AND seg.codigo_acselx NOT IN ('0000000','0','1962466','2828128','5147034','7403602','8843744') --Cambio solicitado por JHAIR Osorio
		AND seg.codigo_acselx NOT IN ('0000000','0')
	)
	and CASE
		WHEN (b.id_estado > 0 AND b.id_estado < 4 AND pre.id_precotizacion IS NOT NULL)
			THEN DATE_TRUNC(CAST(pre.fec_crea AS DATE), MONTH) 
		ELSE DATE_TRUNC(CAST(b.fec_modif AS DATE), MONTH)
	END  = '2022-08-01'
--	order by periodo desc


-----

select * from `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_persona` per
	where per.id_persona in (217496,193478,22713,230473,231464,218722,138131,230265,230449,222172,174781,211252,139270,97163,231564,230529);

select id_persona from `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_agendamiento` a
	where a.id_persona in (217496,193478,22713,230473,231464,218722,138131,230265,230449,222172,174781,211252,139270,97163,231564,230529)
  and id_cliente = 296386
   order by id_persona;

select * from `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_bitacora` b
where id_cliente in (274260,21853,212883,135034,297676,296711,182016,183350,296917,225903,248944,271463,279511,297763,281134,285586,296031,296280,296312,296386,297598,297730) 

--and usu_crea = 373;

Select * from `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_estado` est

select * from `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_seg_usuario` seg
where seg.idusuario =373;

select * from `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_usuario` usu
where  usu.idusuario = 373;

	
select adn.id_adn from
`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_adn` adn
where adn.id_adn
		in ( 
select id_adn from `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_bitacora` b
where id_cliente = 279511 and usu_crea = 373);


SELECT 
		tmp_pre.id_adn,MAX(tmp_pre.id_precotizacion) AS id_precotizacion
	FROM 
		`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` tmp_pre
  where
  tmp_pre.id_adn in (
    select adn.id_adn from
`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_adn` adn
where adn.id_adn
		in ( 
select id_adn from `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_bitacora` b
where id_cliente = 279511 and usu_crea = 373)
  )
	GROUP BY 
		tmp_pre.id_adn;


Select * from `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` pre
right join

(SELECT 
		tmp_pre.id_adn,MAX(tmp_pre.id_precotizacion) AS id_precotizacion
	FROM 
		`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` tmp_pre
  where
  tmp_pre.id_adn in (
    select adn.id_adn from
`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_adn` adn
where adn.id_adn
		in ( 
select id_adn from `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_bitacora` b
where id_cliente = 279511 and usu_crea = 373)
  )
	GROUP BY 
		tmp_pre.id_adn) max_pre
		ON (
			pre.id_adn = max_pre.id_adn 
			AND pre.id_precotizacion=max_pre.id_precotizacion
		);


---

WITH pre_cot_data AS (
	SELECT 
		tmp_pre.id_adn,MAX(tmp_pre.id_precotizacion) AS id_precotizacion
	FROM 
		`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` tmp_pre
	GROUP BY 
		tmp_pre.id_adn
),

pre_pag_data AS (
	SELECT 
		tmp_pag.id_cotizacion, 
		MAX(tmp_pag.id_pago) AS id_pago
	FROM 
		`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_pago` tmp_pag
	WHERE
		(tmp_pag.estado > -1) 
		OR (tmp_pag.estado = -1 AND tmp_pag.mensaje ='Se inactiva por toc negativo')
	GROUP BY 
		tmp_pag.id_cotizacion
)
select distinct per.nro_documento,b.id_cliente,b.usu_crea,b.id_estado,a.id_cliente,a.id_usuario,est.id_estado,a.id_persona,
pre.id_precotizacion,DATE_TRUNC(CAST(pre.fec_crea AS DATE),MONTH),DATE_TRUNC(CAST(b.fec_modif AS DATE), MONTH),
CASE
		WHEN (b.id_estado > 0 AND b.id_estado < 4 AND pre.id_precotizacion IS NOT NULL)
			THEN DATE_TRUNC(CAST(pre.fec_crea AS DATE), MONTH) 
		ELSE DATE_TRUNC(CAST(b.fec_modif AS DATE), MONTH)
	END
/*b.id_estado,pre.id_precotizacion,DATE_TRUNC(CAST(pre.fec_crea AS DATE),MONTH),DATE_TRUNC(CAST(b.fec_modif AS DATE), MONTH),
CASE
		WHEN (b.id_estado > 0 AND b.id_estado < 4 AND pre.id_precotizacion IS NOT NULL)
			THEN DATE_TRUNC(CAST(pre.fec_crea AS DATE), MONTH) 
		ELSE DATE_TRUNC(CAST(b.fec_modif AS DATE), MONTH)
	END*/
FROM 
	`rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_bitacora` b

	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_agendamiento` a
		ON (
			(b.id_cliente = a.id_cliente) 
			AND (b.usu_crea = a.id_usuario)
		)

	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_estado` est
		ON (
			est.id_estado = b.id_estado
		)

	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_persona` per
		ON (
			a.id_persona = per.id_persona
			and per.id_persona = 230529

		)

	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_seg_usuario` seg
		ON (
			a.id_usuario = seg.idusuario
		)

	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_usuario` usu
		ON (
			a.id_usuario = usu.idusuario
		)
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_adn` adn
		ON (
			b.id_adn = adn.id_adn
		)
	LEFT JOIN pre_cot_data max_pre
		ON (
			max_pre.id_adn = adn.id_adn
		)
	LEFT JOIN `rs-nprd-dlk-dd-rwz-a406.ue2dbaprodrdjovv001__db_journeyvv.jvv_precotizacion` pre
		ON (
			pre.id_adn = adn.id_adn 
			AND pre.id_precotizacion=max_pre.id_precotizacion
		)

  /*where CASE
		WHEN (b.id_estado > 0 AND b.id_estado < 4 AND pre.id_precotizacion IS NOT NULL)
			THEN DATE_TRUNC(CAST(pre.fec_crea AS DATE), MONTH) 
		ELSE DATE_TRUNC(CAST(b.fec_modif AS DATE), MONTH)
	END  = '2022-08-01'*/
	--where per.nro_documento is not null
	--order by 6 desc
	where a.id_persona = 230529
