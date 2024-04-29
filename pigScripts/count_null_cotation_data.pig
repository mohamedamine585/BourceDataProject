-- Load the data
cotation = LOAD '../../input/cotation/' USING PigStorage(',') AS (
    SEANCE:chararray, 
    GROUPE:chararray, 
    CODE:chararray, 
    VALEUR:chararray, 
    OUVERTURE:float, 
    CLOTURE:float, 
    PLUS_BAS:float, 
    PLUS_HAUT:float, 
    QUANTITE_NEGOCIEE:float, 
    NB_TRANSACTION:float, 
    CAPITAUX:float
);


null_counts = FOREACH cotation GENERATE
    (SEANCE IS NULL ? 1 : 0) AS seance_null_count,
    (GROUPE IS NULL ? 1 : 0) AS groupe_null_count,
    (CODE IS NULL ? 1 : 0) AS code_null_count,
    (VALEUR IS NULL ? 1 : 0) AS valeur_null_count,
    (OUVERTURE IS NULL ? 1 : 0) AS ouverture_null_count,
    (CLOTURE IS NULL ? 1 : 0) AS cloture_null_count,
    (PLUS_BAS IS NULL ? 1 : 0) AS plus_bas_null_count,
    (PLUS_HAUT IS NULL ? 1 : 0) AS plus_haut_null_count,
    (QUANTITE_NEGOCIEE IS NULL ? 1 : 0) AS quantite_negociee_null_count,
    (NB_TRANSACTION IS NULL ? 1 : 0) AS nb_transaction_null_count,
    (CAPITAUX IS NULL ? 1 : 0) AS capitaux_null_count;

-- Sum the counts
null_counts_summary = FOREACH (GROUP null_counts ALL) GENERATE
    SUM(null_counts.seance_null_count) AS seance_null_count,
    SUM(null_counts.groupe_null_count) AS groupe_null_count,
    SUM(null_counts.code_null_count) AS code_null_count,
    SUM(null_counts.valeur_null_count) AS valeur_null_count,
    SUM(null_counts.ouverture_null_count) AS ouverture_null_count,
    SUM(null_counts.cloture_null_count) AS cloture_null_count,
    SUM(null_counts.plus_bas_null_count) AS plus_bas_null_count,
    SUM(null_counts.plus_haut_null_count) AS plus_haut_null_count,
    SUM(null_counts.quantite_negociee_null_count) AS quantite_negociee_null_count,
    SUM(null_counts.nb_transaction_null_count) AS nb_transaction_null_count,
    SUM(null_counts.capitaux_null_count) AS capitaux_null_count;
row_count = FOREACH (GROUP cotation ALL) GENERATE COUNT(cotation);
STORE null_counts_summary INTO '../../output/null_counts_summary/';