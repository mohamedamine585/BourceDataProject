-- Load the data
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

-- Filter out rows with null values
filtered_indice = FILTER indice BY 
    NOT (SEANCE IS NULL OR 
         CODE_INDICE IS NULL OR 
         LIB_INDICE IS NULL OR 
         INDICE_JOUR IS NULL OR
         INDICE_VEILLE IS NULL OR 
         VARIATION_VEILLE IS NULL OR 
         INDICE_PLUS_HAUT IS NULL OR 
         INDICE_PLUS_BAS IS NULL OR 
         INDICE_OUV IS NULL);

-- Store the filtered data
STORE filtered_indice INTO '../../output/clean_indice/filtered_indice_sharp/' USING PigStorage(',');
