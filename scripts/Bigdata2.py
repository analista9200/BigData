from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, avg, round, sum
import matplotlib.pyplot as plt

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

# ===================================================
# Consulta 1: Tempo médio de entrega por cidade (ordenado do menor para o maior)
# ===================================================
pedido_cidade = spark.sql("""
    SELECT
        cidade,
        COUNT(*) AS total_pedidos,
        ROUND(AVG(tempo_entrega_min), 2) AS tempo_medio_min
    FROM pedidos
    GROUP BY cidade
    ORDER BY tempo_medio_min ASC
""")

pedido_cidade.show()
df_pandas = pedido_cidade.toPandas()

fig, ax1 = plt.subplots()

# eixo Y 1 (esquerda)
ax1.bar(df_pandas["cidade"], df_pandas["total_pedidos"])
ax1.set_xlabel("Cidades")
ax1.set_ylabel("Total de Pedidos")

for i, v in enumerate(df_pandas["total_pedidos"]):
    plt.text(i, v + 0.5, str(v), ha="center")

# eixo Y 2 (direita)
ax2 = ax1.twinx()
ax2.plot(df_pandas["cidade"], df_pandas["tempo_medio_min"], marker="o")
ax2.set_ylabel("Tempo Médio")

plt.title("Total de Pedidos E Tempo Médio de Entrega")

for i, v in enumerate(df_pandas["tempo_medio_min"]):
    plt.text(i, v + 0.5, str(v), ha="center")
    
plt.xticks(rotation=45)
plt.show()

# ===================================================
# Consulta 2: Cidade com melhor desempenho logístico (menor tempo médio)
# ===================================================
faturamento_restaurante = spark.sql("""
    SELECT
        cidade,
        ROUND(avg(tempo_entrega_min), 2) AS tempo_medio_min
    FROM pedidos
    GROUP BY cidade
    ORDER BY tempo_medio_min ASC
""")

faturamento_restaurante.show()
df_pandas = faturamento_restaurante.toPandas()

fig, ax1 = plt.subplots()

# eixo Y 1 (barras)
ax1.bar(df_pandas["cidade"], df_pandas["tempo_medio_min"])
ax1.set_xlabel("Cidade")
ax1.set_ylabel("Desempenho Por Cidade")


for i, v in enumerate(df_pandas["tempo_medio_min"]):
    plt.text(i, v + 0.5, str(v), ha="center")


plt.title("Melhor Desempenho por Cidade")
plt.xticks(rotation=45)
plt.show()

# ===================================================
# Consulta 3: Correlação entre quantidade de pedidos e tempo médio de entrega
# ===================================================
faturamento_min_max = spark.sql("""
    SELECT
        cidade,
        COUNT(*) AS total_pedidos,
        ROUND(AVG(tempo_entrega_min), 2) AS tempo_medio_min
    FROM pedidos
    GROUP BY cidade
    ORDER BY total_pedidos DESC
""")

faturamento_min_max.show()
df_pandas = faturamento_min_max.toPandas()

fig, ax1 = plt.subplots()

# eixo Y 1 (barras)
ax1.bar(df_pandas["cidade"], df_pandas["tempo_medio_min"])
ax1.set_xlabel("cidade")
ax1.set_ylabel("Tempo Médio de Entrega")


for i, v in enumerate(df_pandas["tempo_medio_min"]):
    plt.text(i, v + 0.5, str(v), ha="center")

# eixo Y 2
ax2 = ax1.twinx()
ax2.plot(df_pandas["cidade"], df_pandas["total_pedidos"], marker="o")
ax2.set_ylabel("Total de Pedidos")

for i, v in enumerate(df_pandas["total_pedidos"]):
    plt.text(i, v + 0.5, str(v), ha="center")

plt.title("Correlação entre quantidade de pedidos e Tempo Médio")
plt.xticks(rotation=45)
plt.show()

df_pandas = ""
spark.stop()