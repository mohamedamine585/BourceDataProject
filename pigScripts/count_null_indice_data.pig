indice = LOAD '../../input/indice/' USING PigStorage(',') AS (
    SEANCE:chararray, 
    CODE_INDICE:chararray, 
    LIB_INDICE:chararray, 
    INDICE_JOUR:float, 
    INDICE_VEILLE:float,
 
    VARIATION_VEILLE:float, 
    INDICE_PLUS_HAUT:float, 
    INDICE_PLUS_BAS:float, 
    INDICE_OUV:float
);

-- Calculate null counts for each column
null_counts = FOREACH indice GENERATE
    (SEANCE IS NULL ? 1 : 0) AS seance_null_count,
    (CODE_INDICE IS NULL ? 1 : 0) AS code_indice_null_count,
    (LIB_INDICE IS NULL ? 1 : 0) AS lib_indice_null_count,
    (INDICE_JOUR IS NULL ? 1 : 0) AS indice_jour_null_count,
    (INDICE_VEILLE IS NULL ? 1 : 0) AS indice_veille_null_count,
    (VARIATION_VEILLE IS NULL ? 1 : 0) AS variation_veille_null_count,
    (INDICE_PLUS_HAUT IS NULL ? 1 : 0) AS indice_plus_haut_null_count,
    (INDICE_PLUS_BAS IS NULL ? 1 : 0) AS indice_plus_bas_null_count,
    (INDICE_OUV IS NULL ? 1 : 0) AS indice_ouv_null_count;

-- Summarize null counts
null_counts_summary = FOREACH (GROUP null_counts ALL) GENERATE
    SUM(null_counts.seance_null_count) AS seance_null_count,
    SUM(null_counts.code_indice_null_count) AS code_indice_null_count,
    SUM(null_counts.lib_indice_null_count) AS lib_indice_null_count,
    SUM(null_counts.indice_jour_null_count) AS indice_jour_null_count,
    SUM(null_counts.indice_veille_null_count) AS indice_veille_null_count,
    SUM(null_counts.variation_veille_null_count) AS variation_veille_null_count,
    SUM(null_counts.indice_plus_haut_null_count) AS indice_plus_haut_null_count,
    SUM(null_counts.indice_plus_bas_null_count) AS indice_plus_bas_null_count,
    SUM(null_counts.indice_ouv_null_count) AS indice_ouv_null_count;
STORE null_counts_summary INTO '../../output/null_counts_indice_summary/';