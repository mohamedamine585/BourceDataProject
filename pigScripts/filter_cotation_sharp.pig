

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

filtered_cotation = FILTER cotation BY 
    NOT (SEANCE IS NULL OR 
         GROUPE IS NULL OR 
         CODE IS NULL OR 
         VALEUR IS NULL OR
         OUVERTURE IS NULL OR 
         CLOTURE IS NULL OR 
         PLUS_BAS IS NULL OR 
         PLUS_HAUT IS NULL OR 
         QUANTITE_NEGOCIEE IS NULL OR 
         NB_TRANSACTION IS NULL OR 
         CAPITAUX IS NULL);

STORE filtered_cotation INTO '../../output/clean_cotation/filtered_cotation_sharp/' USING PigStorage(',');