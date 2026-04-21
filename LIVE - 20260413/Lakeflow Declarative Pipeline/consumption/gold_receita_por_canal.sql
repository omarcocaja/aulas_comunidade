-- Passo: criar a view materializada de receita por canal.
-- O que observar: esta análise mostra o desempenho consolidado por canal de venda.
-- Validar: o resultado está orientado a consumo analítico.
-- Sinal de erro: usar esta camada como entrada de novas transformações.

CREATE OR REFRESH MATERIALIZED VIEW gold_receita_por_canal
COMMENT "Camada gold com total de pedidos e receita por canal de venda."
AS
SELECT
  canal_venda,
  COUNT(*) AS total_pedidos,
  ROUND(SUM(valor_pedido), 2) AS receita_total
FROM silver_pedidos
GROUP BY canal_venda;