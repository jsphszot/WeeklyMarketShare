/*
Takes Market info for Cargo tons, 
*/
DECLARE Week0 INT64 DEFAULT 2; -- current week number, for testing just wrote 2

SELECT
CONCAT(CAST(ROW_NUMBER() OVER (PARTITION BY Grupo ORDER BY sum(W) DESC) AS STRING), Grupo) AS KEY,
Grupo,
OwnerGrpd,
-- ROW_NUMBER() OVER (PARTITION BY Grupo ORDER BY sum(W) DESC) AS Rank, -- put this into KEY (used for vlookup in excel)
IEEE_DIVIDE(sum(W), sum(sum(W)) OVER (PARTITION BY Grupo)) AS Wr, -- assigns record to NULL if zero division
IEEE_DIVIDE(sum(Wm1), sum(sum(Wm1)) OVER (PARTITION BY Grupo)) AS Wm1r,
IEEE_DIVIDE(sum(Wm2), sum(sum(Wm2)) OVER (PARTITION BY Grupo)) AS Wm2r,
IEEE_DIVIDE(sum(Wm3), sum(sum(Wm3)) OVER (PARTITION BY Grupo)) AS Wm3r,
IEEE_DIVIDE(sum(Wm4), sum(sum(Wm4)) OVER (PARTITION BY Grupo)) AS Wm4r,
sum(W) as W,
sum(Wm1) as Wm1,
sum(Wm2) as Wm2,
sum(Wm3) as Wm3,
sum(Wm4) as Wm4,
FROM (
    SELECT 
    *,
    CASE 
    -- if LA is in the market, show MS no matter how low it is
    WHEN Owner = 'LA' Then Owner 
    -- only top 5 (plus LA if not in top), rest goes in others
    WHEN Grupo NOT LIKE 'ASIA%' AND ROW_NUMBER() OVER (PARTITION BY Grupo ORDER BY W DESC) > 5 Then 'Otros' 
    -- if Asian origin markets, show top 10 (plus LA if not in top), rest goes in others (diluted market amongst many competitors)
    WHEN ROW_NUMBER() OVER (PARTITION BY Grupo ORDER BY W DESC) > 10 Then 'Otros'
    ELSE Owner
    END AS OwnerGrpd
    FROM (
        SELECT 
        CASE 
        WHEN Origen = 'FL' AND Destino = 'CHILE' AND TipoVuelo = 'PAX' Then 'FL - CL PAX'
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
            -- main destinations for SouthBound flights, Chile and Brasil (GRU, VCP)
            CASE
                WHEN PaisDestinoAWB = 'CHILE' Then 'CL'
                WHEN PostaDestinoAWB IN ('GRU', 'VCP') Then PostaDestinoAWB
                ELSE 'Otros'
            END AS Destino,
            TipoVuelo,
            Owner,
            Tons
            FROM `ReporteWeek.McdoBASE`
        ) cookie
        WHERE Origen != 'Otros' AND Destino != 'Otros' -- drop all unimportant groups
        GROUP BY 1,2
        ORDER BY 1,3 DESC
    ) cookie2
) cookie3
GROUP BY 2, 3
