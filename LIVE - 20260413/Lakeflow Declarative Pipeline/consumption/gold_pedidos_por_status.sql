-- Passo: criar a view materializada de pedidos por status.
-- O que observar: esta análise consolida volume e receita por status do pedido.
-- Validar: a agregação usa os dados já tratados da silver.
-- Sinal de erro: repetir regras de limpeza nesta camada.

CREATE OR REFRESH MATERIALIZED VIEW gold_pedidos_por_status
COMMENT "Camada gold com total de pedidos e receita por status."
AS
SELECT
  status_pedido,
  COUNT(*) AS total_pedidos,
  ROUND(SUM(valor_pedido), 2) AS receita_total
FROM silver_pedidos
GROUP BY status_pedido;
