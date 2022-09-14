--DROP TABLE IF EXISTS `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto`;
--DROP TABLE IF EXISTS `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto_anterior`;

DECLARE var_periodo_proceso DEFAULT DATE_TRUNC(CAST('2022-08-01' AS DATE), MONTH);

CREATE OR REPLACE TABLE `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.funnel_ventas_vitto`
AS
WITH 
t_producto AS (
	SELECT 
		p.id_producto, 
		p.nom_producto,
		ARRAY_AGG(p.cod_riesgo ORDER BY p.id_ramo_contable)[OFFSET(0)] AS cod_riesgo,	
		ARRAY_AGG(p.nom_riesgo ORDER BY p.id_ramo_contable)[OFFSET(0)] AS nom_riesgo,	
		ARRAY_AGG(p.agrupacion_n1 ORDER BY p.id_ramo_contable)[OFFSET(0)] AS agrupacion_n1,	
		ARRAY_AGG(p.agrupacion_n2 ORDER BY p.id_ramo_contable)[OFFSET(0)] AS agrupacion_n2,	
		ARRAY_AGG(p.agrupacion_n3 ORDER BY p.id_ramo_contable)[OFFSET(0)] AS agrupacion_n3
	FROM (
		SELECT 
			p.id_producto, 
			po.nom_producto, 
			po.cod_producto, 
			po.id_origen, 
			p1.nom_riesgo, 
			p1.agrupacion_n1, 
			p1.agrupacion_n2, 
			p1.agrupacion_n3,
			p1.id_ramo_contable, 
			p1.cod_plan AS cod_plan_prod, 
			p1.cod_riesgo, 
			p.nom_corto
		FROM 
			`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_producto.producto` AS p
			CROSS JOIN UNNEST(p.producto_origen) AS po
			CROSS JOIN UNNEST(p.producto_jerarquia) AS p1
		WHERE 
			p.id_origen = "AX"
	) p
	WHERE 
		p.id_origen = 'AX'
	GROUP BY 
		id_producto, 
		nom_producto
),
t_tramite_nuevas_polizas_renov as (
	SELECT 		
	  	t.numtramite ,
		ARRAY_AGG(t.mes_produccion_modificado ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as mes_produccion ,
		ARRAY_AGG(t.periodo_creacion ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as periodo_creacion ,
		ARRAY_AGG(t.origen_data ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as origen_data ,
		ARRAY_AGG(t.id_intermediario ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as id_intermediario ,
		ARRAY_AGG(t.id_poliza ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as id_poliza ,
		ARRAY_AGG(t.id_moneda ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as id_moneda ,
		ARRAY_AGG(t.mnt_prima_emitida_bruta_anualizada ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as mnt_prima_emitida_bruta_anualizada ,
		ARRAY_AGG(t.tasa_cambio ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as tasa_cambio ,
		ARRAY_AGG(t.mnt_prima_emitida_bruta_anualizada_usd ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as mnt_prima_emitida_bruta_anualizada_usd ,
		ARRAY_AGG(t.id_cliente ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as id_cliente ,
		ARRAY_AGG(t.fec_creacion ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as fec_creacion ,
		ARRAY_AGG(t.fec_solicitud ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as fec_solicitud ,
		ARRAY_AGG(t.id_producto ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as id_producto,
		ARRAY_AGG(SPLIT(t.numtramite,'-')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(t.numtramite, '-')) - 1)] ORDER BY t.mnt_prima_emitida_bruta_anualizada DESC)[OFFSET(0)] as id_cotizacion_journey
	FROM 
	      `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.tramite_vitto` t
		  --`rs-nprd-dlk-dt-stg-msrl-d0d7.stg_modelo_operaciones.tramite` t
	WHERE (LOWER(t.tipo_operacion) LIKE '%nueva%p%liza%' OR LOWER(t.tipo_operacion) LIKE 'renovaci%') -- nueva poliza o renovacion para la venta presentada
		AND t.origen_data IN ('LOTUS','JOURNEY DIGITAL')
		AND t.mes_produccion  >= var_periodo_proceso 
	GROUP BY t.numtramite		
),
t_tramites AS (
	SELECT 
		t.mes_produccion, 
		t.periodo_creacion AS periodo,
		t.numtramite AS id_tramite,
		t.origen_data, 
		t.id_intermediario,
		it.nom_intermediario, 
		TRIM(it.cod_rimac) as cod_rimac , 
		t.id_poliza AS pol_id_poliza,
		t.id_moneda,
		t.mnt_prima_emitida_bruta_anualizada	, 
		t.tasa_cambio,
		t.mnt_prima_emitida_bruta_anualizada_usd,
		pol.fec_emision AS pol_fec_emision, 
		pol.tip_forma_pago AS pol_tip_forma_pago, 
		pol.id_origen AS pol_id_origen, 
		t.id_cliente AS id_contratante, 
		cont.nom_completo AS des_nombre_completo_contratante, 
		t.fec_creacion,
		t.fec_solicitud,
		t.id_producto,
		t.id_cotizacion_journey,
		prod.nom_producto, 
		prod.nom_riesgo,
		prod.agrupacion_n1,
		prod.agrupacion_n2,
		prod.agrupacion_n3 
	FROM t_tramite_nuevas_polizas_renov t -- tramites que no se duplican
	LEFT JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.intermediario_mc` it 
		ON (it.id_intermediario = t.id_intermediario)
	LEFT JOIN t_producto prod 
		ON (prod.id_producto = t.id_producto) 
	LEFT JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.poliza` pol 
		ON (pol.id_poliza = t.id_poliza) 
	LEFT JOIN `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` cont 
		ON (cont.id_persona = t.id_cliente)
), 
t_bitacora_jerarquia_ffvv_asesor AS (
	SELECT
		jfv.id_intermediario_rimac AS id_intermediario,
		jfv.dsc_periodo,
		ARRAY_AGG(jfv.id_bitacora_jerarquia_fuerza_ventas ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_bitacora_jerarquia_fuerza_ventas,
		ARRAY_AGG(jfv.id_jerarquia_fuerza_ventas ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_jerarquia_fuerza_ventas,
		ARRAY_AGG(jfv.id_persona ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_persona,
		ARRAY_AGG(jfv.id_canal ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_canal,
		ARRAY_AGG(UPPER(TRIM(jfv.dsc_canal)) ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_canal,
		ARRAY_AGG(jfv.id_subcanal ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_subcanal,
		ARRAY_AGG(jfv.dsc_subcanal ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_subcanal, 
		ARRAY_AGG(jfv.dsc_localidad ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_localidad,
		ARRAY_AGG(jfv.dsc_agencia ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_agencia,
		ARRAY_AGG(jfv.dsc_sede ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_sede,
		ARRAY_AGG(jfv.dsc_estado_gestion ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_estado_gestion,
		ARRAY_AGG(jfv.dsc_nombre_jefe2 ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_nombre_jefe2, 
		ARRAY_AGG(jfv.dsc_periodo || '|' || jfv.cod_sap_jefe1 ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS llave_supervisor, -- llave por supervisor
		ARRAY_AGG(jfv.cod_sap_jefe1 ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS cod_sap_jefe1, 
		ARRAY_AGG(jfv.dsc_nombre_jefe1 ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_nombre_jefe1, 
		ARRAY_AGG(jfv.cod_sap ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS cod_sap,
		ARRAY_AGG(jfv.tip_documento ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS tip_documento,
		ARRAY_AGG(jfv.num_documento ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS num_documento,
		ARRAY_AGG(jfv.dsc_nombre_corto ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_nombre_corto,
		ARRAY_AGG(jfv.id_persona_jefe1 ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_persona_jefe1,
		ARRAY_AGG(jfv.id_persona_jefe2 ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_persona_jefe2,
		ARRAY_AGG(jfv.dsc_categoria ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_categoria,
		ARRAY_AGG(jfv.ind_dotacion_activa ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS ind_dotacion_activa,
		ARRAY_AGG(UPPER(TRIM(jfv.dsc_supervision)) ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_supervision  -- para obtener flg_financiero y flg_especialista
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv
	WHERE 
		TRIM(UPPER(jfv.dsc_tipo_puesto))='ASESOR'
		AND TRIM(UPPER(jfv.dsc_canal)) = 'FUERZA DE VENTA' 
		AND jfv.dsc_periodo >= FORMAT_DATE("%Y%m", CAST(var_periodo_proceso AS date)) 
	GROUP BY 
		jfv.id_intermediario_rimac,
		jfv.dsc_periodo
),
t_cot_mnt_data AS (
	SELECT
		a.id_cotizacion_origen,
		ARRAY_AGG(a.id_tramite ORDER BY a.id_tramite DESC,a.id_cotizacion_origen DESC)[OFFSET(0)] AS id_tramite,
		ARRAY_AGG(a.flg_presentada ORDER BY a.id_tramite DESC,a.id_cotizacion_origen DESC)[OFFSET(0)] AS flg_presentada,
		ARRAY_AGG(a.prima_anual_presentada_usd ORDER BY a.id_tramite DESC,a.id_cotizacion_origen DESC)[OFFSET(0)] AS prima_anual_presentada_usd,
		ARRAY_AGG(TRIM(UPPER(a.origen_data)) ORDER BY a.id_tramite DESC,a.id_cotizacion_origen DESC)[OFFSET(0)] AS origen_data,
		-- campo que almacena el valor 'JOURNEY DIGITAL' o 'LOTUS'
		ARRAY_AGG(TRIM(UPPER(a.origen_tramite)) ORDER BY a.id_tramite DESC,a.id_cotizacion_origen DESC)[OFFSET(0)] AS origen_tramite
	FROM 
	     `rs-nprd-dlk-dt-stg-mica-4de1.delivery_canales.cotizacion_vitto` a
	    --`rs-nprd-dlk-dt-stg-msrl-d0d7.stg_modelo_prospeccion.cotizacion` a
	WHERE TRIM(UPPER(a.origen_data)) IN ('JOURNEY','COTIZADOR WEB')
	GROUP BY a.id_cotizacion_origen
),
t_tramites_cotizacion AS (
	SELECT  tc.id_tramite ,
			ARRAY_AGG(tc.id_cotizacion_origen ORDER BY tc.flg_presentada DESC , tc.id_cotizacion_origen DESC)[OFFSET(0)] AS id_cotizacion_origen,
			ARRAY_AGG(tc.flg_presentada ORDER BY tc.flg_presentada DESC , tc.id_cotizacion_origen DESC)[OFFSET(0)] AS flg_presentada,
			ARRAY_AGG(tc.cot_prima_anual_presentada_usd ORDER BY tc.flg_presentada DESC , tc.id_cotizacion_origen DESC)[OFFSET(0)] AS cot_prima_anual_presentada_usd
	FROM (
			SELECT  t.id_tramite ,
					cot.id_cotizacion_origen ,
					cot.flg_presentada , 
					cot.prima_anual_presentada_usd AS cot_prima_anual_presentada_usd 
			FROM t_tramites t
			JOIN t_cot_mnt_data cot ON ( t.origen_data = 'LOTUS' AND cot.id_tramite = t.id_tramite  AND cot.id_tramite IS NOT NULL )
			UNION ALL 
			SELECT  t.id_tramite ,
					cot.id_cotizacion_origen,				 
					cot.flg_presentada , 
					cot.prima_anual_presentada_usd AS cot_prima_anual_presentada_usd 
			FROM t_tramites t
			-- JOIN t_cot_mnt_data cot ON ( cot.origen_data = 'JOURNEY DIGITAL' AND  t.id_cotizacion_journey  = cot.id_cotizacion_origen ) -- se ha verificado que el id_tramite no se repite en las cotizaciones.
			-- El campo origen_data tiene el valor 'JOURNEY' y se cambia por el campo al "origen_tramite", ya que en este campo se almaneza el valor JOURNEY DIGITAL
			JOIN t_cot_mnt_data cot ON ( cot.origen_tramite = 'JOURNEY DIGITAL' AND  t.id_cotizacion_journey  = cot.id_cotizacion_origen )  
			) tc
	GROUP BY tc.id_tramite
),
t_funnel_ventas AS (
	SELECT 
		a.mes_produccion AS trm_mes_produccion, 
		a.periodo AS trm_periodo,
		a.id_tramite AS trm_id_tramite, 
		a.origen_data AS trm_origen_data, 
		a.id_intermediario AS trm_id_intermediario,
		a.nom_intermediario, 
		a.pol_id_poliza AS pol_id_poliza,
		-- prima anualizada de la poliza
		a.mnt_prima_emitida_bruta_anualizada_usd AS pol_prima_emitida_bruta_anualizada_usd, 
		-- Importe de la prima anual de la cotizacion 
		b.flg_presentada, 
		b.cot_prima_anual_presentada_usd AS mnt_cot_prima_anual_presentada_usd, 
		--- prima presentada 
		-- *****************
		-- Se dara prioridad 1 a la informacion que viene de la Cotizacion y luego al importe de la poliza_anualizada
		CASE
			-- informacion de la cotizacion (stg_modelo_prospeccion.cotizacion)
			WHEN b.flg_presentada = '1' AND COALESCE(b.cot_prima_anual_presentada_usd,0)>0  --  prioridad para presentada
				THEN b.cot_prima_anual_presentada_usd -- Si presenta la cotizacion el flag de presentado
			-- informacion de la prima de la poliza 
			ELSE a.mnt_prima_emitida_bruta_anualizada_usd 
		END mnt_prima_presentada_anualizada_usd, 
		-- prima emitida : si la poliza del tramite presenta fecha de emision, entonces fue emitido el tramite
		-- ************** 
		CASE
			-- si no presenta fecha de emision 
			WHEN a.pol_fec_emision IS NULL 
				THEN 0 -- Si no se encuentra emitida la poliza no tiene prima emitida 
			-- Informacion de la cotizacion (stg_modelo_prospeccion.cotizacion)
			WHEN b.flg_presentada = '1' AND COALESCE(b.cot_prima_anual_presentada_usd,0)>0 AND a.pol_fec_emision IS NOT NULL 
				THEN b.cot_prima_anual_presentada_usd -- prima emitida igual a la presentada
			ELSE a.mnt_prima_emitida_bruta_anualizada_usd 
		END mnt_prima_emitida_bruta_anualizada_usd,
		-------------
		a.pol_fec_emision AS pol_fec_emision, 
		a.pol_tip_forma_pago AS pol_tip_forma_pago, 
		a.pol_id_origen AS pol_id_origen, 
		a.id_contratante AS trm_id_contratante, 
		a.des_nombre_completo_contratante AS nombre_completo_contratante, 
		a.fec_creacion AS trm_fec_creacion,
		a.fec_solicitud AS trm_fec_solicitud,
		a.id_producto AS trm_id_producto,
		a.nom_producto AS des_producto, 
		a.nom_riesgo AS trm_des_riesgo,
		a.agrupacion_n1 AS trm_agrupacion_n1,
		a.agrupacion_n2 AS trm_agrupacion_n2,
		a.agrupacion_n3 AS trm_agrupacion_n3,
		c.id_bitacora_jerarquia_fuerza_ventas,
		c.id_persona AS bit_id_persona_asesor,
		c.id_canal AS bit_id_canal,
		c.dsc_canal AS bit_des_canal,
		c.id_subcanal AS bit_id_subcanal,
		c.dsc_subcanal AS bit_des_subcanal,
		c.dsc_localidad AS bit_localidad,
		c.dsc_agencia AS bit_agencia,
		c.dsc_sede AS bit_sede, 
		CASE 
			WHEN SUBSTRING(UPPER(TRIM(c.dsc_sede)),1,4) = 'LIMA' 
				THEN 'LIMA'
			WHEN SUBSTRING(UPPER(TRIM(c.dsc_sede)),1,9) = 'PROVINCIA' 
				THEN TRIM(SUBSTRING(TRIM(SUBSTRING(c.dsc_sede, INSTR(c.dsc_sede,'-')+1,30)), 1, IF(INSTR(c.dsc_sede,' ')>1,INSTR(c.dsc_sede,' ')-1,30)))
			ELSE 'NO DETERMINADO' 
		END AS ciudad, 
		---------- 
		IF( INSTR(c.dsc_supervision,'FINANCIERO') > 0 , 'S', 'N' ) as  ind_asesor_financiero ,   
        IF( c.dsc_supervision LIKE 'ESPECIALISTA%' , 'S', 'N' ) as  ind_asesor_especialista ,    
		----------		
		c.dsc_nombre_jefe2 AS bit_gerente_agencia, 
		c.dsc_nombre_jefe1 AS bit_supervisor,
		c.cod_sap AS bit_cod_sap_asesor,
		c.dsc_nombre_corto AS bit_nombre_corto_asesor, 
		c.dsc_estado_gestion AS bit_estado_gestion, 
		c.dsc_categoria AS bit_categoria, 
		c.ind_dotacion_activa AS bit_dotacion_activa
	FROM 
		t_tramites a
		LEFT JOIN t_tramites_cotizacion b 
		    ON ( b.id_tramite = a.id_tramite) -- se ha verificado que el id_tramite no se repita en las cotizaciones.
		LEFT JOIN t_bitacora_jerarquia_ffvv_asesor c 
			ON (c.dsc_periodo = FORMAT_DATE("%Y%m", a.mes_produccion)-- el mes de produccion es YYYYMM y el dsc_periodo tambien tiene el formato YYYYMM
				AND c.id_intermediario = a.cod_rimac )
	WHERE 
		c.dsc_canal = 'FUERZA DE VENTA' 
), 
-- metas
t_metas_prima_neta_xsupervisor AS (
	SELECT
        a.periodo,
        a.id_subcanal, 
        b.cod_sap AS cod_sap_asesor, 
        MAX(a.id_meta_comercial ) AS id_meta_comercial ,
		MAX(a.mes_comercial  ) AS mes_comercial , -- formato YYYYMM
		MAX(a.id_canal ) AS id_canal, 
		MAX(a.des_canal ) AS des_canal,
		MAX(a.des_subcanal ) AS des_subcanal,
		MAX(a.cod_sap  ) AS cod_sap_sup, 
		MAX(a.dsc_nombre_asesor  ) AS dsc_nombre_sup,
		MAX(a.dsc_cargo ) AS dsc_cargo,
		MAX(a.dsc_indicador ) AS dsc_indicador,
		MAX(a.dsc_unidad_meta ) AS dsc_unidad_meta,
		MAX(SAFE_CAST(a.mes_comercial AS string) || '|' || a.cod_sap ) AS llave_supervisor,
		MAX(a.mnt_meta_comercial ) AS mnt_meta_supervisor 
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.meta_comercial` a
		LEFT JOIN t_bitacora_jerarquia_ffvv_asesor b 
			ON (b.llave_supervisor = CAST(a.mes_comercial AS string) || '|' || a.cod_sap)
	WHERE 
		UPPER(TRIM(a.des_canal)) = 'FUERZA DE VENTA'
		-- Cambios en la meta , según el origen del subcanal
		AND (  
		     ( UPPER(TRIM(a.dsc_indicador)) = 'PRIMA NETA' AND a.id_subcanal = '201' ) OR      -- FFVV VIDA 
			 ( INSTR( UPPER(TRIM(a.dsc_indicador)), 'PRIMA') > 0  AND a.id_subcanal = '202' )  -- RENTA VITALICIA : para PRIMA RG , PRIMA VAS , PRIMA IG, PRIMA RRVV
		    )
		AND UPPER(TRIM(a.dsc_cargo)) LIKE '5. GTE DE UNID%'
		AND safe_CAST(a.mes_comercial AS string) >= FORMAT_DATE("%Y%m", CAST(var_periodo_proceso AS date)) 
    GROUP BY 
        a.periodo,
        a.id_subcanal, 
        b.cod_sap
),
--- cotizaciones primas por jerarquia del asesor 
t_bitacora_jerarquia_ffvv_asesor_xcodsap AS (
	SELECT
		jfv.cod_sap AS cod_sap_asesor,
		jfv.dsc_periodo,
		ARRAY_AGG(jfv.id_bitacora_jerarquia_fuerza_ventas ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_bitacora_jerarquia_fuerza_ventas,
		ARRAY_AGG(jfv.id_intermediario_rimac ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_intermediario,
		ARRAY_AGG(jfv.dsc_nombres	|| ' ' || jfv.dsc_apellido_paterno || ' ' || coalesce(jfv.dsc_apellido_materno,'') ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS nom_intermediario,
		ARRAY_AGG(jfv.id_persona ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_persona_asesor,		
		ARRAY_AGG(jfv.id_canal ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_canal,
		ARRAY_AGG(UPPER(TRIM(jfv.dsc_canal)) ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_canal,
		ARRAY_AGG(jfv.id_subcanal ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS id_subcanal,
		ARRAY_AGG(jfv.dsc_subcanal ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_subcanal,
		ARRAY_AGG(jfv.dsc_localidad ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_localidad,
		ARRAY_AGG(jfv.dsc_agencia ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_agencia,
		ARRAY_AGG(jfv.dsc_sede ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_sede,
		ARRAY_AGG(jfv.dsc_estado_gestion ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_estado_gestion,
		ARRAY_AGG(jfv.dsc_nombre_jefe2 ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS gerente_agencia   ,
		ARRAY_AGG(jfv.dsc_nombre_jefe1 ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS supervisor   ,   
		ARRAY_AGG(jfv.dsc_nombre_corto ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS nombre_corto_asesor   ,  
		ARRAY_AGG(jfv.dsc_categoria ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_categoria   ,  
		ARRAY_AGG(jfv.ind_dotacion_activa ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS ind_dotacion_activa ,
		ARRAY_AGG(UPPER(TRIM(jfv.dsc_supervision)) ORDER BY id_jerarquia_fuerza_ventas)[OFFSET(0)] AS dsc_supervision
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_comercial.bitacora_jerarquia_fuerza_ventas` jfv
	WHERE 
		TRIM(UPPER(jfv.dsc_tipo_puesto))='ASESOR'
		AND TRIM(UPPER(jfv.dsc_canal)) = 'FUERZA DE VENTA'
		AND jfv.dsc_periodo >= FORMAT_DATE("%Y%m", CAST(var_periodo_proceso AS date)) 
	GROUP BY 
		jfv.cod_sap,
		jfv.dsc_periodo
),
t_cotizaciones_prima_prod_xasesor AS (
	SELECT 
		a.periodo, 
		a.id_canal, 
		a.des_canal, 
		a.id_sub_canal, 
		a.des_sub_canal, 
		a.cod_sap_asesor, 
		MAX(b.id_bitacora_jerarquia_fuerza_ventas) AS id_bitacora_jerarquia_fuerza_ventas,
		SUM(a.cot_mnt_prima_presentada_usd) AS mnt_cotiz_asesor_primas_anual_usd 
	FROM 
		`rs-nprd-dlk-dd-az-d8bc.anl_comercial.productividad` a
		LEFT JOIN t_bitacora_jerarquia_ffvv_asesor_xcodsap b 
			ON (b.dsc_periodo = FORMAT_DATE("%Y%m", a.periodo) 
				AND b.cod_sap_asesor = a.cod_sap_asesor 
				AND b.id_subcanal = a.id_sub_canal)
	WHERE 
		UPPER(TRIM(a.des_canal)) = 'FUERZA DE VENTA'
		AND a.periodo >= var_periodo_proceso 
	GROUP BY 
		a.periodo, 
		a.id_canal, 
		a.des_canal, 
		a.id_sub_canal, 
		a.des_sub_canal, 
		a.cod_sap_asesor 
),
t_data_bitacora_sin_tramites AS (
    SELECT 
        parse_date('%Y%m', a.dsc_periodo)  AS trm_mes_produccion ,
        parse_date('%Y%m', a.dsc_periodo)  AS trm_periodo ,  
        SAFE_CAST(NULL AS STRING) AS trm_id_tramite, 
        SAFE_CAST(NULL AS STRING) AS trm_origen_data, 
        SAFE_CAST(a.id_intermediario AS INT64) AS id_intermediario ,
        a.nom_intermediario, 
        SAFE_CAST(NULL AS STRING) AS pol_id_poliza,
        SAFE_CAST(NULL AS NUMERIC) AS pol_prima_emitida_bruta_anualizada_usd, 
        SAFE_CAST(NULL AS STRING) AS flg_presentada,
        SAFE_CAST(NULL AS NUMERIC) AS mnt_cot_prima_anual_presentada_usd,
        SAFE_CAST(NULL AS NUMERIC) AS mnt_prima_presentada_anualizada_usd, 
        SAFE_CAST(NULL AS NUMERIC) AS mnt_prima_emitida_bruta_anualizada_usd,
        SAFE_CAST(NULL AS DATE) AS pol_fec_emision, 
        SAFE_CAST(NULL AS STRING) AS pol_tip_forma_pago, 
        SAFE_CAST(NULL AS STRING) AS pol_id_origen, 
        SAFE_CAST(NULL AS STRING) AS trm_id_contratante, 
        SAFE_CAST(NULL AS STRING) AS nombre_completo_contratante, 
        SAFE_CAST(NULL AS DATE) AS trm_fec_creacion,
        SAFE_CAST(NULL AS DATE) AS trm_fec_solicitud,
        SAFE_CAST(NULL AS STRING) AS trm_id_producto,
        SAFE_CAST(NULL AS STRING) AS des_producto, 
        SAFE_CAST(NULL AS STRING) AS trm_des_riesgo,
        SAFE_CAST(NULL AS STRING) AS trm_agrupacion_n1,
        SAFE_CAST(NULL AS STRING) AS trm_agrupacion_n2,
        SAFE_CAST(NULL AS STRING) AS trm_agrupacion_n3,
        a.id_bitacora_jerarquia_fuerza_ventas,
        a.id_persona_asesor as bit_id_persona_asesor,
        a.id_canal as bit_id_canal,
        a.dsc_canal as bit_des_canal,
        a.id_subcanal as bit_id_subcanal,
        a.dsc_subcanal as bit_des_subcanal,
        a.dsc_localidad as bit_localidad,
        a.dsc_agencia as bit_agencia,
        a.dsc_sede as bit_sede,
        CASE 
            WHEN SUBSTRING(UPPER(TRIM(a.dsc_sede)),1,4) = 'LIMA' 
                THEN 'LIMA'
            WHEN SUBSTRING(UPPER(TRIM(a.dsc_sede)),1,9) = 'PROVINCIA' 
                THEN TRIM(SUBSTRING(TRIM(SUBSTRING(a.dsc_sede, INSTR(a.dsc_sede,'-')+1,30)), 1, IF(INSTR(a.dsc_sede,' ')>1,INSTR(a.dsc_sede,' ')-1,30)))
            ELSE 'NO DETERMINADO' 
        END AS ciudad,  
		---------- 
		IF( INSTR(a.dsc_supervision,'FINANCIERO') > 0 , 'S', 'N' ) AS  ind_asesor_financiero ,    
        IF( a.dsc_supervision LIKE 'ESPECIALISTA%'    , 'S', 'N' ) AS  ind_asesor_especialista ,        
        SAFE_CAST(NULL AS STRING) AS ind_cliente_nuevo,    
		----------		
        a.gerente_agencia as bit_gerente_agencia, 
        a.supervisor as bit_supervisor,
        a.cod_sap_asesor as bit_cod_sap_asesor,
        a.nombre_corto_asesor as bit_nombre_corto_asesor, 
        a.dsc_estado_gestion as bit_estado_gestion, 
        a.dsc_categoria as bit_categoria, 
        a.ind_dotacion_activa as bit_dotacion_activa,
        0 AS dotacion_real,
        0 AS dotacion_activa, 
        -- Obtener las metas que hace referencia a la meta del supervisor
        b.id_meta_comercial,
        b.mnt_meta_supervisor, 
        -- Cotizacion de la produccion por asesor segun la jerarquia
        SAFE_CAST(NULL AS NUMERIC) AS mnt_cotiz_asesor_primas_anual_usd
    FROM t_bitacora_jerarquia_ffvv_asesor_xcodsap a
        LEFT JOIN t_metas_prima_neta_xsupervisor b 
            ON (b.id_subcanal = CAST(a.id_subcanal AS string) 
                AND b.periodo = PARSE_DATE('%Y%m', a.dsc_periodo)
                AND b.cod_sap_asesor = a.cod_sap_asesor)
        LEFT JOIN (
            SELECT DISTINCT trm_mes_produccion , id_bitacora_jerarquia_fuerza_ventas 
            FROM t_funnel_ventas ) fv 
			ON (fv.id_bitacora_jerarquia_fuerza_ventas = a.id_bitacora_jerarquia_fuerza_ventas 
				AND FORMAT_DATE("%Y%m",fv.trm_mes_produccion)  = a.dsc_periodo )
    WHERE 
        fv.id_bitacora_jerarquia_fuerza_ventas IS NULL
) ,
t_cliente_xperiodo AS (  -- Para obtener si es un cliente nuevo o antiguo para un determinado subcanal
    SELECT  
            fv.bit_id_subcanal ,
            fv.trm_id_contratante  ,
            min( trm_mes_produccion ) as mes_produccion_min , 
            max( trm_mes_produccion ) as mes_produccion_max 
    FROM  t_funnel_ventas fv
    GROUP BY fv.bit_id_subcanal , fv.trm_id_contratante
) 
SELECT DISTINCT 
	a.trm_mes_produccion, 
	a.trm_periodo,
	a.trm_id_tramite, 
	a.trm_origen_data, 
	a.trm_id_intermediario,
	a.nom_intermediario, 
	a.pol_id_poliza,
	a.pol_prima_emitida_bruta_anualizada_usd, 
	a.flg_presentada,
	a.mnt_cot_prima_anual_presentada_usd,
	a.mnt_prima_presentada_anualizada_usd, 
	a.mnt_prima_emitida_bruta_anualizada_usd,
	a.pol_fec_emision, 
	a.pol_tip_forma_pago, 
	a.pol_id_origen, 
	a.trm_id_contratante, 
	a.nombre_completo_contratante, 
	a.trm_fec_creacion,
	a.trm_fec_solicitud,
	a.trm_id_producto,
	a.des_producto, 
	a.trm_des_riesgo,
	a.trm_agrupacion_n1,
	a.trm_agrupacion_n2,
	a.trm_agrupacion_n3,
	a.id_bitacora_jerarquia_fuerza_ventas,
	a.bit_id_persona_asesor,
	a.bit_id_canal,
	a.bit_des_canal,
	a.bit_id_subcanal,
	a.bit_des_subcanal,
	a.bit_localidad,
	a.bit_agencia,
	a.bit_sede,
	a.ciudad, 
    ------
	a.ind_asesor_financiero,     
    a.ind_asesor_especialista,   
    IF( a.trm_mes_produccion = cxp.mes_produccion_min  ,'S' , 'N' ) AS ind_cliente_nuevo,
	-----	
	a.bit_gerente_agencia, 
	a.bit_supervisor,
	a.bit_cod_sap_asesor,
	a.bit_nombre_corto_asesor, 
	a.bit_estado_gestion, 
	a.bit_categoria, 
	a.bit_dotacion_activa,
	-- Obtener la dotacion real y activa
	CASE
		WHEN a.mnt_prima_presentada_anualizada_usd > 0 THEN 1
		WHEN COALESCE(a.mnt_prima_presentada_anualizada_usd,0) = 0 THEN 0
		ELSE 0
	END dotacion_real,
	CASE
		WHEN a.mnt_prima_emitida_bruta_anualizada_usd > 0 THEN 1
		WHEN COALESCE(a.mnt_prima_emitida_bruta_anualizada_usd,0) = 0 THEN 0
		ELSE 0
	END dotacion_activa, 
	-- Obtener las metas que hace referencia a la meta del supervisor
	b.id_meta_comercial,
	b.mnt_meta_supervisor, 
	-- Cotizacion de la produccion por asesor segun la jerarquia
	c.mnt_cotiz_asesor_primas_anual_usd, 
    current_date()	AS fec_procesamiento,
    a.trm_periodo	AS periodo
FROM 
	t_funnel_ventas a
	LEFT JOIN t_metas_prima_neta_xsupervisor b 
		ON (b.id_subcanal = CAST(a.bit_id_subcanal AS string) 
			AND b.periodo = a.trm_mes_produccion 
			AND b.cod_sap_asesor = a.bit_cod_sap_asesor)
	LEFT JOIN t_cotizaciones_prima_prod_xasesor c 
		ON (c.id_bitacora_jerarquia_fuerza_ventas = a.id_bitacora_jerarquia_fuerza_ventas)  -- en el id_bitacora_jerarquia_fuerza_ventas se encuentra el periodo 
	LEFT JOIN t_cliente_xperiodo cxp 
		ON ( cxp.trm_id_contratante  = a.trm_id_contratante and cxp.bit_id_subcanal = a.bit_id_subcanal )    	
UNION ALL -- incluir en el funnel los asesores que no tuvieron tramites en el mes y estan en la jerarquia
SELECT 
    a.trm_mes_produccion ,
    a.trm_periodo ,  
    a.trm_id_tramite, 
    a.trm_origen_data, 
    a.id_intermediario ,
    a.nom_intermediario, 
    a.pol_id_poliza,
    a.pol_prima_emitida_bruta_anualizada_usd, 
    a.flg_presentada,
    a.mnt_cot_prima_anual_presentada_usd,
    a.mnt_prima_presentada_anualizada_usd, 
    a.mnt_prima_emitida_bruta_anualizada_usd,
    a.pol_fec_emision, 
    a.pol_tip_forma_pago, 
    a.pol_id_origen, 
    a.trm_id_contratante, 
    a.nombre_completo_contratante, 
    a.trm_fec_creacion,
    a.trm_fec_solicitud,
    a.trm_id_producto,
    a.des_producto, 
    a.trm_des_riesgo,
    a.trm_agrupacion_n1,
    a.trm_agrupacion_n2,
    a.trm_agrupacion_n3,
    a.id_bitacora_jerarquia_fuerza_ventas,
    a.bit_id_persona_asesor,
    a.bit_id_canal,
    a.bit_des_canal,
    a.bit_id_subcanal,
    a.bit_des_subcanal,
    a.bit_localidad,
    a.bit_agencia,
    a.bit_sede,
    a.ciudad,  
	---
	a.ind_asesor_financiero,       
    a.ind_asesor_especialista,     
    a.ind_cliente_nuevo,
	---    
    a.bit_gerente_agencia, 
    a.bit_supervisor,
    a.bit_cod_sap_asesor,
    a.bit_nombre_corto_asesor, 
    a.bit_estado_gestion, 
    a.bit_categoria, 
    a.bit_dotacion_activa,
    a.dotacion_real,
    a.dotacion_activa, 
    a.id_meta_comercial,
    a.mnt_meta_supervisor, 
    a.mnt_cotiz_asesor_primas_anual_usd, 
    current_date()	AS fec_procesamiento,
    a.trm_periodo	AS periodo
FROM t_data_bitacora_sin_tramites a;