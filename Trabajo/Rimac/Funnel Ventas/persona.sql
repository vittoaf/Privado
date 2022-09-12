SELECT 
		a.id_persona, 
		b.tip_documento, 
		b.num_documento, 
		a.nom_completo
	FROM 
		`rs-nprd-dlk-dd-stgz-8ece.stg_modelo_persona.persona` a
		CROSS JOIN UNNEST (a.documento_identidad) b
  where 	a.id_persona = 'AX-4470185'