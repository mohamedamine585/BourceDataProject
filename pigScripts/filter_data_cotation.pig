

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
    NOT (SEANCE IS NULL AND 
         OUVERTURE IS NULL AND 
         CLOTURE IS NULL AND 
         PLUS_BAS IS NULL AND 
         PLUS_HAUT IS NULL AND 
         QUANTITE_NEGOCIEE IS NULL AND 
         NB_TRANSACTION IS NULL AND 
         CAPITAUX IS NULL);

STORE filtered_cotation INTO '../../output/filtered_cotation/';