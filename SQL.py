
""" 
#Contrataciones por Tecnologia

SELECT t.name AS tecnologia, COUNT(*) AS contrataciones
FROM fact_application f
JOIN dim_technology t ON t.tech_sk = f.tech_sk
WHERE f.hired = 1
GROUP BY t.name
ORDER BY contrataciones DESC

#contrataciones por año

SELECT d.year AS anio, COUNT(*) AS contrataciones
FROM fact_application f
JOIN dim_date d ON d.date_sk = f.date_sk
WHERE f.hired = 1 AND d.year IS NOT NULL
GROUP BY d.year
ORDER BY d.year




#contrataciones por seniority

SELECT s.level AS seniority, COUNT(*) AS contrataciones
FROM fact_application f
JOIN dim_seniority s ON s.seniority_sk = f.seniority_sk
WHERE f.hired = 1 AND s.level IS NOT NULL
GROUP BY s.level
ORDER BY contrataciones DESC



#pais con mas y menos personas contratadas

SELECT 'Contratados'    AS tipo, d.name AS pais, t1.total AS total
FROM (
  SELECT country_sk, COUNT(*) AS total
  FROM fact_application
  WHERE hired = 1
  GROUP BY country_sk
  ORDER BY total DESC
  LIMIT 1
) t1
JOIN dim_country d ON t1.country_sk = d.country_sk

UNION ALL

SELECT 'No Contratados' AS tipo, d.name AS pais, t2.total AS total
FROM (
  SELECT country_sk, COUNT(*) AS total
  FROM fact_application
  WHERE hired = 0
  GROUP BY country_sk
  ORDER BY total DESC
  LIMIT 1
) t2
JOIN dim_country d ON t2.country_sk = d.country_sk;


#contrataciones por rango de experiencia 

SELECT
  CASE
    WHEN fa.yoe BETWEEN 0 AND 2 THEN 'Junior (0–2)'
    WHEN fa.yoe BETWEEN 3 AND 5 THEN 'SemiSenior (3–5)'
    WHEN fa.yoe >= 6              THEN 'Senior (6+)'
    ELSE 'No definido'
  END AS rango_experiencia,
  COUNT(*) AS contrataciones
FROM fact_application fa
WHERE fa.hired = 1
GROUP BY rango_experiencia
ORDER BY contrataciones DESC;

"""