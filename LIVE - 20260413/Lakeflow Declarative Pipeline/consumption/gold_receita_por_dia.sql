-- Passo: criar a view materializada de receita por dia.
-- O que observar: esta camada responde uma pergunta direta de negócio.
-- Validar: a fonte da análise é `silver_pedidos`, e não a bronze.
-- Sinal de erro: calcular métrica final a partir da camada bruta.

CREATE OR REFRESH MATERIALIZED VIEW gold_receita_por_dia
COMMENT "Camada gold com total de pedidos e receita por dia."
AS
SELECT
  CAST(updated_at AS DATE) AS data_referencia,
  COUNT(*) AS total_pedidos,
  ROUND(SUM(valor_pedido), 2) AS receita_total
FROM silver_pedidos
GROUP BY CAST(updated_at AS DATE);