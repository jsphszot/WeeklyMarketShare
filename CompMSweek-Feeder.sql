-- WeeklyReport shows info from current week- back_weeks (python input)
DECLARE Week0 INT64 DEFAULT EXTRACT(ISOWEEK FROM CURRENT_DATE())-{back_weeks};
-- How many competitors to show per Origins (LA, +4 if NotAsia | + 9 if Asia, others), threshold
DECLARE NotAsiaComp INT64 DEFAULT 5;
DECLARE AsiaComp INT64 DEFAULT 10;

-- Common table expression, process later forks into two tables based on cookie2 (LA records and all other)
WITH cookie2 AS (
    SELECT 
    CASE 
    -- TODO set correct PAX/CAO groups
    WHEN Origen = 'FL' AND Destino = 'CL' AND TipoVuelo = 'PAX' Then 'FL - CL PAX'
    ELSE CONCAT(Origen, ' - ', Destino)
    END AS Grupo,
    Owner,
    -- due to two week lag for data from Brasil make each Week (W, WmX) column equal to current week minus 2 for GRU and VCP destinations.
    sum(IF ((RelWeek=week0-0-2 AND Destino IN ('VCP', 'GRU')) OR (Destino NOT IN ('VCP', 'GRU') AND RelWeek=week0-0), Tons, 0)) AS W,
    sum(IF ((RelWeek=week0-1-2 AND Destino IN ('VCP', 'GRU')) OR (Destino NOT IN ('VCP', 'GRU') AND RelWeek=week0-1), Tons, 0)) AS Wm1,
    sum(IF ((RelWeek=week0-2-2 AND Destino IN ('VCP', 'GRU')) OR (Destino NOT IN ('VCP', 'GRU') AND RelWeek=week0-2), Tons, 0)) AS Wm2,
    sum(IF ((RelWeek=week0-3-2 AND Destino IN ('VCP', 'GRU')) OR (Destino NOT IN ('VCP', 'GRU') AND RelWeek=week0-3), Tons, 0)) AS Wm3,
    sum(IF ((RelWeek=week0-4-2 AND Destino IN ('VCP', 'GRU')) OR (Destino NOT IN ('VCP', 'GRU') AND RelWeek=week0-4), Tons, 0)) AS Wm4,
    FROM (
        SELECT 
        Year,
        Semana,
        RelWeek,
        -- define groups according to different origin-destination concepts
        CASE 
            WHEN ZonaOrigenAWB = 'NA_USA_Florida' Then 'FL'
            WHEN ZonaOrigenAWB IN ('NA_Canada', 'NA_USA_Midwest', 'NA_USA_NorthEast', 'NA_USA_South', 'NA_USA_SouthEast') Then 'GSA NA'
            WHEN ZonaOrigenAWB = 'NA_USA_NewYork' Then 'JFK'
            WHEN ZonaOrigenAWB = 'NA_USA_West' Then 'WEST'
            WHEN ZonaOrigenAWB = 'AS_India' Then 'INDIA'
            WHEN (RegionOrigenAWB = 'Asia' AND ZonaOrigenAWB != 'AS_India') Then 'ASIA'
            WHEN PaisOrigenAWB = 'Mexico' Then 'MEX'
            When RegionOrigenAWB = 'Oceania' Then 'OCEANIA'
            WHEN PaisOrigenAWB = 'ALEMANIA' Then 'DE'
            WHEN PaisOrigenAWB = 'ESPANA' Then 'ES'
            WHEN PaisOrigenAWB = 'PAISES BAJOS' Then 'NL'
            WHEN PaisOrigenAWB = 'ITALIA' Then 'IT'
            WHEN PaisOrigenAWB = 'FRANCIA' Then 'FR'
            WHEN PaisOrigenAWB = 'BELGICA' Then 'BE'
            WHEN PaisOrigenAWB = 'REINO UNIDO' Then 'UK'
            WHEN PaisOrigenAWB IN ('DINAMARCA' , 'SUECIA', 'FINLANDIA', 'NORUEGA') Then 'SK'
            WHEN RegionOrigenAWB = 'Europe' Then 'OTROS EU'
            ELSE 'Otros' 
        END AS Origen,
        -- main destinations for SouthBound flights, Chile and Brasil (GRU, VCP, LIM)
        CASE
            WHEN PaisDestinoAWB = 'CHILE' Then 'CL'
            WHEN PostaDestinoAWB IN ('GRU', 'VCP', 'LIM') Then PostaDestinoAWB
            ELSE 'Otros'
        END AS Destino,
        TipoVuelo,
        TRIM(Owner) AS Owner,
        Tons
        FROM `ReporteWeek.McdoBASE`
    ) cookie
    -- drop unimportant groups
    WHERE Origen != 'Otros' AND Destino != 'Otros'
    GROUP BY 1,2
    ORDER BY 1,3 DESC
)

SELECT
CONCAT(CAST(Rank AS STRING), Grupo) AS excelKEY, -- (used for vlookup in excel)
Rank,
Grupo,
OwnerGrpd,
 -- Calculate MS per each week, assigns record to NULL if zero division
IEEE_DIVIDE(W, sum(W) OVER (PARTITION BY Grupo)) AS Wr,
IEEE_DIVIDE(Wm1, sum(Wm1) OVER (PARTITION BY Grupo)) AS Wm1r,
IEEE_DIVIDE(Wm2, sum(Wm2) OVER (PARTITION BY Grupo)) AS Wm2r,
IEEE_DIVIDE(Wm3, sum(Wm3) OVER (PARTITION BY Grupo)) AS Wm3r,
IEEE_DIVIDE(Wm4, sum(Wm4) OVER (PARTITION BY Grupo)) AS Wm4r,
W,
Wm1,
Wm2,
Wm3,
Wm4
FROM (

    /*
    Here we split cookie2 into two tables: one where Owner='LA', and all the other Owner in the second table.
    Remember we always want to show the LA grouping in our final table no matter how low the MarketShare
    LA is ranked as 0, All other owners are ranked according to last week's tons (W) per Grupo and tagged as 'Otros' if 
    ranked below threshold. Both tables are then appended to each other.
    */
    SELECT 
    ROW_NUMBER() OVER (PARTITION BY Grupo ORDER BY sum(W) DESC) AS Rank,
    Grupo,
    OwnerGrpd,
    sum(W) as W,
    sum(Wm1) as Wm1,
    sum(Wm2) as Wm2,
    sum(Wm3) as Wm3,
    sum(Wm4) as Wm4
    FROM (
        SELECT 
        *,
        CASE
            WHEN Grupo NOT LIKE 'ASIA%' AND ROW_NUMBER() OVER (PARTITION BY Grupo ORDER BY W DESC) >= NotAsiaComp Then 'Otros'
            WHEN ROW_NUMBER() OVER (PARTITION BY Grupo ORDER BY W DESC) >= AsiaComp Then 'Otros'
            ELSE Owner
        END AS OwnerGrpd
        FROM cookie2
        WHERE Owner != 'LA'
        ) AS cookie3
    GROUP BY 2,3

    UNION ALL

    SELECT 
    0 AS Rank,
    Grupo,
    OwnerGrpd,
    sum(W) as W,
    sum(Wm1) as Wm1,
    sum(Wm2) as Wm2,
    sum(Wm3) as Wm3,
    sum(Wm4) as Wm4
    FROM (
        SELECT 
        *,
        Owner AS OwnerGrpd
        FROM cookie2
        WHERE Owner = 'LA'
        ) AS cookie3LA
    GROUP BY 2,3

) FinalCookie
ORDER BY 3, 2
