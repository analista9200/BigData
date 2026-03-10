from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, avg, round, sum

spark = SparkSession.builder \
    .appName("ProjetoBigData-Spark") \
    .master("local[*]") \
    .getOrCreate()

# ===================================================
# Ler Arquivo CSV
# ===================================================

caminho_dados = "dados/delivery_dados_ficticios.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ",") \
    .csv(caminho_dados)

print("Dados Carregados com Sucesso!")
print("\nEstrutura do DataFrame:")
df.printSchema()

# ===================================================
# Análise via Spark SQL
# ===================================================

df.createOrReplaceTempView("pedidos")

pedido_cidade = spark.sql("""
    SELECT
        cidade,
        COUNT(*) AS total_pedidos,
        ROUND(AVG(valor_pedido), 2) AS ticket_medio
    FROM pedidos
    GROUP BY cidade
    ORDER BY ticket_medio DESC
""")

faturamento_restaurante = spark.sql("""
    SELECT
        restaurante,
        COUNT(*) AS total_pedidos,
        ROUND(sum(valor_pedido), 2) AS faturamento,
        ROUND(avg(tempo_entrega_min), 2) AS Tempo_Medio
    FROM pedidos
    GROUP BY restaurante
    ORDER BY faturamento DESC
""")

faturamento_min_max = spark.sql("""
    SELECT
        restaurante,
        ROUND(min(valor_pedido), 2) AS Pedido_Min,
        ROUND(max(valor_pedido), 2) AS Pedido_Max,
        ROUND(max(valor_pedido) / min(valor_pedido), 2) AS ABS_Variance
    FROM pedidos
    GROUP BY restaurante
    ORDER BY Pedido_Max DESC
""")

Valorporentrega = spark.sql("""
    SELECT
        restaurante,
        ROUND(AVG(valor_pedido), 2) as mediavalorpedido,
        ROUND(AVG(tempo_entrega_min) / AVG(valor_pedido), 2) AS valor_por_tempo
    FROM pedidos
    GROUP BY restaurante
    ORDER BY valor_por_tempo DESC
""")

pedido_cidade.show()
faturamento_restaurante.show()
faturamento_min_max.show()
Valorporentrega.show()

spark.stop()