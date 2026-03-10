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
# Total de Pedidos e Ticket Médio
# ===================================================
pedido_cidade = spark.sql("""
    SELECT
        cidade,
        COUNT(*) AS total_pedidos,
        ROUND(AVG(valor_pedido), 2) AS ticket_medio
    FROM pedidos
    GROUP BY cidade
    ORDER BY ticket_medio DESC
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
ax2.plot(df_pandas["cidade"], df_pandas["ticket_medio"], marker="o")
ax2.set_ylabel("Ticket Médio")

plt.title("Pedidos e Ticket Médio por Cidade")

for i, v in enumerate(df_pandas["ticket_medio"]):
    plt.text(i, v + 0.5, str(v), ha="center")
    
plt.xticks(rotation=45)
plt.show()

# ===================================================
# Total de Pedidos, Faturamento e Ticket Médio
# ===================================================
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

faturamento_restaurante.show()
df_pandas = faturamento_restaurante.toPandas()

fig, ax1 = plt.subplots()

# eixo Y 1 (barras)
ax1.bar(df_pandas["restaurante"], df_pandas["total_pedidos"])
ax1.set_xlabel("Restaurante")
ax1.set_ylabel("Total de Pedidos")


for i, v in enumerate(df_pandas["total_pedidos"]):
    plt.text(i, v + 0.5, str(v), ha="center")

# eixo Y 2
ax2 = ax1.twinx()
ax2.plot(df_pandas["restaurante"], df_pandas["faturamento"], marker="o")
ax2.set_ylabel("Faturamento")

for i, v in enumerate(df_pandas["faturamento"]):
    plt.text(i, v + 0.5, str(v), ha="center")

# terceiro dado (linha diferente no mesmo eixo 2)
ax2.plot(df_pandas["restaurante"], df_pandas["Tempo_Medio"], marker="s")
ax2.set_ylabel("Ticket Médio / Faturamento")

for i, v in enumerate(df_pandas["Tempo_Medio"]):
    plt.text(i, v + 0.5, str(v), ha="center")

plt.title("Análise de Restaurantes por Faturamento e Tempo Médio")
plt.xticks(rotation=45)
plt.show()

# ===================================================
# Análise de Pedido Min/Max e Variance!
# ===================================================
faturamento_min_max = spark.sql("""
    SELECT
        restaurante,
        ROUND(min(valor_pedido), 2) AS Pedido_Min,
        ROUND(max(valor_pedido), 2) AS Pedido_Max,
        ROUND(max(valor_pedido) / min(valor_pedido), 2) AS Variance
    FROM pedidos
    GROUP BY restaurante
    ORDER BY Pedido_Max DESC
""")

faturamento_min_max.show()
df_pandas = faturamento_min_max.toPandas()

fig, ax1 = plt.subplots()

# eixo Y 1 (barras)
ax1.bar(df_pandas["restaurante"], df_pandas["Pedido_Min"])
ax1.set_xlabel("Restaurante")
ax1.set_ylabel("Mínimo Pedido")


for i, v in enumerate(df_pandas["Pedido_Min"]):
    plt.text(i, v + 0.5, str(v), ha="center")

# eixo Y 2
ax2 = ax1.twinx()
ax2.plot(df_pandas["restaurante"], df_pandas["Pedido_Max"], marker="o")
ax2.set_ylabel("Máximo Pedido")

for i, v in enumerate(df_pandas["Pedido_Max"]):
    plt.text(i, v + 0.5, str(v), ha="center")

# terceiro dado (linha diferente no mesmo eixo 2)
ax2.plot(df_pandas["restaurante"], df_pandas["Variance"], marker="s")
ax2.set_ylabel("Variação Min/Max")

for i, v in enumerate(df_pandas["Variance"]):
    plt.text(i, v + 0.5, str(v), ha="center")

plt.title("Análise de Pedido Min/Max e Variação")
plt.xticks(rotation=45)
plt.show()

# ===================================================
# Relação de Media de Pedido por restaurante e tempo de entrega
# ===================================================
Valorporentrega = spark.sql("""
    SELECT
        restaurante,
        ROUND(AVG(valor_pedido), 2) as mediavalorpedido,
        ROUND(AVG(tempo_entrega_min) / AVG(valor_pedido), 2) AS valor_por_tempo
    FROM pedidos
    GROUP BY restaurante
    ORDER BY valor_por_tempo DESC
""")

Valorporentrega.show()
df_pandas = Valorporentrega.toPandas()

fig, ax1 = plt.subplots()

# eixo Y 1 (esquerda)
ax1.bar(df_pandas["restaurante"], df_pandas["mediavalorpedido"])
ax1.set_xlabel("Restaurantes")
ax1.set_ylabel("Média de Valor de Pedido")

for i, v in enumerate(df_pandas["mediavalorpedido"]):
    plt.text(i, v + 0.5, str(v), ha="center")

# eixo Y 2 (direita)
ax2 = ax1.twinx()
ax2.plot(df_pandas["restaurante"], df_pandas["valor_por_tempo"], marker="o")
ax2.set_ylabel("Tempo por Valor")

plt.title("Media de Valor de Pedido e Relação de Tempo de Entrega")

for i, v in enumerate(df_pandas["valor_por_tempo"]):
    plt.text(i, v + 0.5, str(v), ha="center")
    
plt.xticks(rotation=45)
plt.show()


df_pandas = ""
spark.stop()